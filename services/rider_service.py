"""Rider service for operational data"""
from typing import List, Optional, Tuple
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
from models.rider import Rider
from utils.dynamodb import dynamodb_client, TABLES
from utils.geohash import encode as geohash_encode
from utils.datetime_ist import now_ist_iso
from aws_lambda_powertools import Logger

ASSIGNMENT_WINDOW_DAYS = 7
# A rider's lastSeen heartbeat arrives every 25 s (foreground) or ~15 s (OS background task).
# Keep the stale window comfortably above both intervals.
RIDER_LAST_SEEN_STALE_SECONDS = 90

logger = Logger()


def _update_order_with_rider_location(order_id: str, lat: float, lng: float, speed: float, heading: float):
    """Update order with rider's current location for real-time tracking"""
    try:
        from services.order_service import OrderService
        
        logger.info(f"Updating order {order_id} with rider location")
        OrderService.update_order(order_id, {
            'riderCurrentLat': lat,
            'riderCurrentLng': lng,
            'riderSpeed': speed,
            'riderHeading': heading,
            'riderLocationUpdatedAt': now_ist_iso()
        })
        logger.info(f"✅ Order {order_id} updated with rider location")
    except Exception as e:
        logger.error(f"Failed to update order with rider location: {str(e)}")
        # Don't throw - location update failure shouldn't block rider location update


class RiderService:
    """Service for rider operational operations"""

    @staticmethod
    def _parse_last_seen(last_seen: Optional[str]) -> Optional[datetime]:
        """Parse an ISO timestamp string into UTC."""
        if not last_seen or not isinstance(last_seen, str):
            return None

        normalized = last_seen.strip()
        if not normalized:
            return None

        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            logger.warning(f"Unable to parse rider lastSeen timestamp: {last_seen}")
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _is_rider_fresh(last_seen: Optional[str], now: Optional[datetime] = None) -> bool:
        """Return True when rider heartbeat is recent enough for assignment."""
        parsed_last_seen = RiderService._parse_last_seen(last_seen)
        if parsed_last_seen is None:
            return False

        reference_time = now or datetime.now(timezone.utc)
        if parsed_last_seen > reference_time:
            return True

        rider_age = reference_time - parsed_last_seen
        return rider_age <= timedelta(seconds=RIDER_LAST_SEEN_STALE_SECONDS)

    @staticmethod
    def _filter_assignable_riders(riders: List[Rider]) -> List[Rider]:
        """Filter riders who are active, free, located, and recently seen."""
        reference_time = datetime.now(timezone.utc)
        assignable_riders: List[Rider] = []

        for rider in riders:
            if not rider.is_active:
                continue
            if rider.working_on_order:
                continue
            if rider.lat is None or rider.lng is None:
                continue
            if not RiderService._is_rider_fresh(rider.last_seen, reference_time):
                logger.info(
                    f"Skipping stale rider {rider.rider_id}: "
                    f"lastSeen={rider.last_seen}, threshold={RIDER_LAST_SEEN_STALE_SECONDS}s"
                )
                continue

            assignable_riders.append(rider)

        return assignable_riders

    @staticmethod
    def _ensure_user_rider_exists(rider_id: str):
        """Ensure rider exists in Users table via riderId-index"""
        from services.user_service import UserService
        user = UserService.get_rider_by_rider_id(rider_id)
        if not user:
            raise Exception(f"Rider user not found for riderId: {rider_id}")
    
    @staticmethod
    def create_rider(rider: Rider) -> Rider:
        """Create a new rider operational record"""
        try:
            dynamodb_client.put_item(
                TableName=TABLES['RIDERS'],
                Item=rider.to_dynamodb_item()
            )
            return rider
        except ClientError as e:
            raise Exception(f"Failed to create rider: {str(e)}")
    
    @staticmethod
    def get_rider(rider_id: str) -> Optional[Rider]:
        """Get rider by ID"""
        try:
            response = dynamodb_client.get_item(
                TableName=TABLES['RIDERS'],
                Key={'riderId': {'S': rider_id}}
            )
            
            if 'Item' not in response:
                return None
            
            return Rider.from_dynamodb_item(response['Item'])
        except ClientError as e:
            raise Exception(f"Failed to get rider: {str(e)}")

    @staticmethod
    def get_rider_rating_and_count(rider_id: str) -> tuple:
        """
        Get rider's rating and ratedCount from Riders table.
        Returns (rating: float or None, rated_count: int).
        Reads from raw DynamoDB item to support different attribute name conventions.
        """
        try:
            response = dynamodb_client.get_item(
                TableName=TABLES['RIDERS'],
                Key={'riderId': {'S': rider_id}}
            )
            item = response.get('Item') or {}
            rating = None
            rated_count = 0
            for key in ('rating', 'Rating'):
                if key in item and 'N' in item[key]:
                    try:
                        rating = float(item[key]['N'])
                        break
                    except (TypeError, ValueError):
                        pass
            for key in ('ratedCount', 'RatedCount', 'rated_count'):
                if key in item and 'N' in item[key]:
                    try:
                        rated_count = int(float(item[key]['N']))
                        break
                    except (TypeError, ValueError):
                        pass
            return (rating, rated_count)
        except ClientError as e:
            logger.warning(f"get_rider_rating_and_count failed for {rider_id}: {e}")
            return (None, 0)
    
    @staticmethod
    def get_rider_by_mobile(phone: str) -> Optional[Rider]:
        """Get rider by phone number using GSI"""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['RIDERS'],
                IndexName='phone-index',
                KeyConditionExpression='phone = :phone',
                ExpressionAttributeValues={
                    ':phone': {'S': phone}
                }
            )
            
            if not response.get('Items'):
                return None
            
            return Rider.from_dynamodb_item(response['Items'][0])
        except ClientError as e:
            raise Exception(f"Failed to get rider by mobile: {str(e)}")
    
    @staticmethod
    def list_riders() -> List[Rider]:
        """List all riders"""
        try:
            response = dynamodb_client.scan(
                TableName=TABLES['RIDERS']
            )
            
            riders = []
            for item in response.get('Items', []):
                riders.append(Rider.from_dynamodb_item(item))
            
            return riders
        except ClientError as e:
            raise Exception(f"Failed to list riders: {str(e)}")
    
    @staticmethod
    def update_location(
        rider_id: str,
        lat: float,
        lng: float,
        speed: float = 0.0,
        heading: float = 0.0
    ) -> Rider:
        """Update rider location and movement data with geohash and GSI fields"""
        try:
            timestamp = now_ist_iso()
            
            # Calculate geohash at all precision levels
            geohash_p7 = geohash_encode(lat, lng, precision=7)
            geohash_p6 = geohash_p7[:6]
            geohash_p5 = geohash_p7[:5]
            geohash_p4 = geohash_p7[:4]
            
            # Update rider location in riders table
            dynamodb_client.update_item(
                TableName=TABLES['RIDERS'],
                Key={'riderId': {'S': rider_id}},
                UpdateExpression='SET lat = :lat, lng = :lng, speed = :speed, heading = :heading, #timestamp = :timestamp, lastSeen = :lastSeen, geohash = :geohash, GSI1PK = :gsi1pk, GSI1SK = :gsi1sk, GSI2PK = :gsi2pk, GSI2SK = :gsi2sk, GSI3PK = :gsi3pk, GSI3SK = :gsi3sk',
                ExpressionAttributeNames={
                    '#timestamp': 'timestamp'
                },
                ExpressionAttributeValues={
                    ':lat': {'N': str(lat)},
                    ':lng': {'N': str(lng)},
                    ':speed': {'N': str(speed)},
                    ':heading': {'N': str(heading)},
                    ':timestamp': {'S': timestamp},
                    ':lastSeen': {'S': timestamp},
                    ':geohash': {'S': geohash_p7},
                    ':gsi1pk': {'S': geohash_p6},
                    ':gsi1sk': {'S': f'RIDER#{rider_id}'},
                    ':gsi2pk': {'S': geohash_p5},
                    ':gsi2sk': {'S': f'RIDER#{rider_id}'},
                    ':gsi3pk': {'S': geohash_p4},
                    ':gsi3sk': {'S': f'RIDER#{rider_id}'}
                }
            )
            
            # Get updated rider to check if working on an order
            updated_rider = RiderService.get_rider(rider_id)
            
            # If rider is working on orders, update each order with rider's current location
            if updated_rider and updated_rider.working_on_order:
                logger.info(f"Rider {rider_id} is working on orders {updated_rider.working_on_order}")
                for order_id in updated_rider.working_on_order:
                    _update_order_with_rider_location(
                        order_id,
                        lat,
                        lng,
                        speed,
                        heading
                    )
            
            return updated_rider
        except ClientError as e:
            raise Exception(f"Failed to update location: {str(e)}")
    
    @staticmethod
    def set_active_status(rider_id: str, is_active: bool, lat: Optional[float] = None, lng: Optional[float] = None) -> Rider:
        """Toggle rider online/offline status with optional location"""
        try:
            RiderService._ensure_user_rider_exists(rider_id)
            timestamp = now_ist_iso()
            
            # If going online and location provided, update location and geohash
            if is_active and lat is not None and lng is not None:
                # Calculate geohash at all precision levels
                geohash_p7 = geohash_encode(lat, lng, precision=7)
                geohash_p6 = geohash_p7[:6]
                geohash_p5 = geohash_p7[:5]
                geohash_p4 = geohash_p7[:4]
                
                # When going online, also clear any stale workingOnOrder from previous sessions.
                # An uninstall/reinstall does not clear DynamoDB; stale entries block assignment.
                dynamodb_client.update_item(
                    TableName=TABLES['RIDERS'],
                    Key={'riderId': {'S': rider_id}},
                    UpdateExpression='SET isActive = :active, lastSeen = :lastSeen, lat = :lat, lng = :lng, geohash = :geohash, GSI1PK = :gsi1pk, GSI1SK = :gsi1sk, GSI2PK = :gsi2pk, GSI2SK = :gsi2sk, GSI3PK = :gsi3pk, GSI3SK = :gsi3sk REMOVE workingOnOrder',
                    ExpressionAttributeValues={
                        ':active': {'BOOL': is_active},
                        ':lastSeen': {'S': timestamp},
                        ':lat': {'N': str(lat)},
                        ':lng': {'N': str(lng)},
                        ':geohash': {'S': geohash_p7},
                        ':gsi1pk': {'S': geohash_p6},
                        ':gsi1sk': {'S': f'RIDER#{rider_id}'},
                        ':gsi2pk': {'S': geohash_p5},
                        ':gsi2sk': {'S': f'RIDER#{rider_id}'},
                        ':gsi3pk': {'S': geohash_p4},
                        ':gsi3sk': {'S': f'RIDER#{rider_id}'}
                    }
                )
                logger.info(f"[riderId={rider_id}] Went online — cleared any stale workingOnOrder")
            else:
                # No location provided — split by direction to handle workingOnOrder correctly
                if is_active:
                    # Going online without a GPS fix yet: still clear any stale order lock
                    dynamodb_client.update_item(
                        TableName=TABLES['RIDERS'],
                        Key={'riderId': {'S': rider_id}},
                        UpdateExpression='SET isActive = :active, lastSeen = :lastSeen REMOVE workingOnOrder',
                        ExpressionAttributeValues={
                            ':active': {'BOOL': is_active},
                            ':lastSeen': {'S': timestamp}
                        }
                    )
                    logger.info(f"[riderId={rider_id}] Went online (no GPS) — cleared any stale workingOnOrder")
                else:
                    dynamodb_client.update_item(
                        TableName=TABLES['RIDERS'],
                        Key={'riderId': {'S': rider_id}},
                        UpdateExpression='SET isActive = :active, lastSeen = :lastSeen',
                        ExpressionAttributeValues={
                            ':active': {'BOOL': is_active},
                            ':lastSeen': {'S': timestamp}
                        }
                    )
            
            return RiderService.get_rider(rider_id)
        except ClientError as e:
            raise Exception(f"Failed to set active status: {str(e)}")
    
    @staticmethod
    def set_working_on_order(rider_id: str, order_id: Optional[str]) -> Rider:
        """Add or clear the order(s) rider is working on"""
        try:
            RiderService._ensure_user_rider_exists(rider_id)
            logger.info(f"[orderId={order_id}] Updating rider workingOnOrder for riderId={rider_id}")
            rider = RiderService.get_rider(rider_id)
            current_orders = rider.working_on_order if rider else []

            if order_id:
                if order_id not in current_orders:
                    current_orders.append(order_id)
            else:
                current_orders = []

            if current_orders:
                dynamodb_client.update_item(
                    TableName=TABLES['RIDERS'],
                    Key={'riderId': {'S': rider_id}},
                    UpdateExpression='SET workingOnOrder = :orderIds',
                    ExpressionAttributeValues={
                        ':orderIds': {'L': [{'S': str(v)} for v in current_orders]}
                    }
                )
            else:
                dynamodb_client.update_item(
                    TableName=TABLES['RIDERS'],
                    Key={'riderId': {'S': rider_id}},
                    UpdateExpression='REMOVE workingOnOrder'
                )
            
            logger.info(f"[orderId={order_id}] Rider workingOnOrder updated for riderId={rider_id}")
            return RiderService.get_rider(rider_id)
        except ClientError as e:
            raise Exception(f"Failed to set working on order: {str(e)}")

    @staticmethod
    def add_rating(rider_id: str, new_rating: float) -> Rider:
        """Add a new rating to rider and recompute average + ratedCount."""
        try:
            rider = RiderService.get_rider(rider_id)
            if not rider:
                raise Exception("Rider not found")

            current_count = rider.rated_count or 0
            current_avg = rider.rating or 0.0
            updated_count = current_count + 1
            updated_avg = round(((current_avg * current_count) + float(new_rating)) / updated_count, 2)

            dynamodb_client.update_item(
                TableName=TABLES['RIDERS'],
                Key={'riderId': {'S': rider_id}},
                UpdateExpression='SET rating = :rating, ratedCount = :ratedCount',
                ExpressionAttributeValues={
                    ':rating': {'N': str(updated_avg)},
                    ':ratedCount': {'N': str(updated_count)}
                }
            )

            return RiderService.get_rider(rider_id)
        except ClientError as e:
            raise Exception(f"Failed to add rider rating: {str(e)}")

    @staticmethod
    def increment_assignment_count(rider_id: str) -> Optional[Rider]:
        """
        Increment the rider's 7-day assignment count. If the window has elapsed
        (or never been set), reset to 1 and start a new window.
        """
        try:
            rider = RiderService.get_rider(rider_id)
            if not rider:
                logger.warning(f"increment_assignment_count: rider not found {rider_id}")
                return None

            now = datetime.now(timezone.utc)
            window_start = rider.assignment_window_start
            reset_window = True

            if window_start:
                try:
                    start_dt = datetime.fromisoformat(window_start.replace("Z", "+00:00"))
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=timezone.utc)
                    if (now - start_dt) < timedelta(days=ASSIGNMENT_WINDOW_DAYS):
                        reset_window = False
                except (ValueError, TypeError):
                    pass

            if reset_window:
                new_count = 1
                new_window_start = now.isoformat()
            else:
                new_count = (rider.orders_assigned_last_7d or 0) + 1
                new_window_start = window_start

            dynamodb_client.update_item(
                TableName=TABLES['RIDERS'],
                Key={'riderId': {'S': rider_id}},
                UpdateExpression='SET ordersAssignedLast7d = :cnt, assignmentWindowStart = :ws',
                ExpressionAttributeValues={
                    ':cnt': {'N': str(new_count)},
                    ':ws': {'S': new_window_start}
                }
            )
            logger.info(f"[riderId={rider_id}] Assignment count updated: {new_count} (window reset={reset_window})")
            return RiderService.get_rider(rider_id)
        except ClientError as e:
            logger.error(f"increment_assignment_count failed for {rider_id}: {e}")
            return None

    @staticmethod
    def apply_rejection_penalty(rider_id: str, deduction: float = 0.1) -> Optional[Rider]:
        """
        Deduct from rider's rating for an order rejection (no-response or explicit).
        Does not change ratedCount. New rating = max(0, current_rating - deduction).
        """
        try:
            rider = RiderService.get_rider(rider_id)
            if not rider:
                logger.warning(f"apply_rejection_penalty: rider not found {rider_id}")
                return None
            current = rider.rating if rider.rating is not None else 0.0
            new_rating = round(max(0.0, current - deduction), 2)
            dynamodb_client.update_item(
                TableName=TABLES['RIDERS'],
                Key={'riderId': {'S': rider_id}},
                UpdateExpression='SET rating = :rating',
                ExpressionAttributeValues={':rating': {'N': str(new_rating)}}
            )
            logger.info(f"[riderId={rider_id}] Rejection penalty applied: rating {current} -> {new_rating}")
            return RiderService.get_rider(rider_id)
        except ClientError as e:
            logger.error(f"apply_rejection_penalty failed for {rider_id}: {e}")
            return None

    @staticmethod
    def _get_all_riders() -> List[Rider]:
        """Paginated scan of all riders (single-town deployment)."""
        riders: List[Rider] = []
        last_evaluated_key = None
        while True:
            kwargs = {'TableName': TABLES['RIDERS']}
            if last_evaluated_key:
                kwargs['ExclusiveStartKey'] = last_evaluated_key
            response = dynamodb_client.scan(**kwargs)
            for item in response.get('Items', []):
                riders.append(Rider.from_dynamodb_item(item))
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break
        return riders

    @staticmethod
    def find_available_riders_near(lat: float, lng: float, radius_km: float = 5) -> List[Tuple[Rider, float]]:
        """
        Find available online riders within radius.

        Loads all riders from the table (single-town scale), then filters by assignability and distance.

        Returns: List of (Rider, distance_km) tuples sorted by distance
        """
        try:
            from utils.distance import calculate_distance

            logger.info("Loading all riders from DB (single-town)")
            all_riders = RiderService._get_all_riders()
            logger.info(f"Found {len(all_riders)} riders in table")
            
            # Filter: active, recently seen, not working on order, and has location
            available_riders = RiderService._filter_assignable_riders(all_riders)
            
            logger.info(f"Found {len(available_riders)} available riders")
            
            # Calculate distances and filter by radius
            nearby_riders = []
            for rider in available_riders:
                distance = calculate_distance(lat, lng, rider.lat, rider.lng)
                if distance <= radius_km:
                    nearby_riders.append((rider, distance))
            
            # Sort by distance (closest first)
            nearby_riders.sort(key=lambda x: x[1])
            
            logger.info(f"Found {len(nearby_riders)} riders within {radius_km}km")
            
            return nearby_riders
        except Exception as e:
            logger.error(f"Error finding nearby riders: {str(e)}", exc_info=True)
            # Fallback to old scan method if geohash query fails
            logger.warn("Falling back to table scan")
            return RiderService._find_available_riders_scan(lat, lng, radius_km)
    
    @staticmethod
    def _find_available_riders_scan(lat: float, lng: float, radius_km: float = 5) -> List[Tuple[Rider, float]]:
        """Fallback: Find available riders using table scan (slower)"""
        try:
            from utils.distance import calculate_distance
            
            response = dynamodb_client.scan(
                TableName=TABLES['RIDERS'],
                FilterExpression='isActive = :active AND (attribute_not_exists(workingOnOrder) OR size(workingOnOrder) = :zero)',
                ExpressionAttributeValues={
                    ':active': {'BOOL': True},
                    ':zero': {'N': '0'}
                }
            )
            
            nearby_riders = []
            for item in response.get('Items', []):
                rider = Rider.from_dynamodb_item(item)
                if not RiderService._filter_assignable_riders([rider]):
                    continue
                if rider.lat is not None and rider.lng is not None:
                    distance = calculate_distance(lat, lng, rider.lat, rider.lng)
                    if distance <= radius_km:
                        nearby_riders.append((rider, distance))
            
            # Sort by distance
            nearby_riders.sort(key=lambda x: x[1])
            
            return nearby_riders
        except ClientError as e:
            raise Exception(f"Failed to find nearby riders: {str(e)}")
