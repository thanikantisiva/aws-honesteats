"""Rider service for operational data"""
from typing import List, Optional, Tuple
from botocore.exceptions import ClientError
from models.rider import Rider
from utils.dynamodb import dynamodb_client, TABLES
from utils.geohash import encode as geohash_encode, get_neighbors, get_precision_for_radius
from datetime import datetime
from aws_lambda_powertools import Logger

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
            'riderLocationUpdatedAt': datetime.utcnow().isoformat()
        })
        logger.info(f"✅ Order {order_id} updated with rider location")
    except Exception as e:
        logger.error(f"Failed to update order with rider location: {str(e)}")
        # Don't throw - location update failure shouldn't block rider location update


class RiderService:
    """Service for rider operational operations"""
    
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
            timestamp = datetime.utcnow().isoformat()
            
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
            
            # If rider is working on an order, update the order with rider's current location
            if updated_rider and updated_rider.working_on_order:
                logger.info(f"Rider {rider_id} is working on order {updated_rider.working_on_order}")
                _update_order_with_rider_location(
                    updated_rider.working_on_order,
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
            timestamp = datetime.utcnow().isoformat()
            
            # If going online and location provided, update location and geohash
            if is_active and lat is not None and lng is not None:
                # Calculate geohash at all precision levels
                geohash_p7 = geohash_encode(lat, lng, precision=7)
                geohash_p6 = geohash_p7[:6]
                geohash_p5 = geohash_p7[:5]
                geohash_p4 = geohash_p7[:4]
                
                dynamodb_client.update_item(
                    TableName=TABLES['RIDERS'],
                    Key={'riderId': {'S': rider_id}},
                    UpdateExpression='SET isActive = :active, lastSeen = :lastSeen, lat = :lat, lng = :lng, geohash = :geohash, GSI1PK = :gsi1pk, GSI1SK = :gsi1sk, GSI2PK = :gsi2pk, GSI2SK = :gsi2sk, GSI3PK = :gsi3pk, GSI3SK = :gsi3sk',
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
            else:
                # Just update active status
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
        """Set or clear the order rider is working on"""
        try:
            if order_id:
                dynamodb_client.update_item(
                    TableName=TABLES['RIDERS'],
                    Key={'riderId': {'S': rider_id}},
                    UpdateExpression='SET workingOnOrder = :orderId',
                    ExpressionAttributeValues={
                        ':orderId': {'S': order_id}
                    }
                )
            else:
                dynamodb_client.update_item(
                    TableName=TABLES['RIDERS'],
                    Key={'riderId': {'S': rider_id}},
                    UpdateExpression='REMOVE workingOnOrder'
                )
            
            return RiderService.get_rider(rider_id)
        except ClientError as e:
            raise Exception(f"Failed to set working on order: {str(e)}")
    
    @staticmethod
    def _query_riders_by_geohash(geohash: str, precision: int = 7) -> List[Rider]:
        """Query riders by geohash at specific precision with pagination"""
        try:
            riders = []
            last_evaluated_key = None
            
            # Choose index based on precision
            if precision == 7:
                # No index for P7, would need to scan - skip for now
                return []
            elif precision == 6:
                query_params = {
                    'TableName': TABLES['RIDERS'],
                    'IndexName': 'GSI1',
                    'KeyConditionExpression': 'GSI1PK = :pk',
                    'ExpressionAttributeValues': {':pk': {'S': geohash}}
                }
            elif precision == 5:
                query_params = {
                    'TableName': TABLES['RIDERS'],
                    'IndexName': 'GSI2',
                    'KeyConditionExpression': 'GSI2PK = :pk',
                    'ExpressionAttributeValues': {':pk': {'S': geohash}}
                }
            elif precision == 4:
                query_params = {
                    'TableName': TABLES['RIDERS'],
                    'IndexName': 'GSI3',
                    'KeyConditionExpression': 'GSI3PK = :pk',
                    'ExpressionAttributeValues': {':pk': {'S': geohash}}
                }
            else:
                raise ValueError(f"Unsupported precision: {precision}")
            
            # Paginate through results
            while True:
                if last_evaluated_key:
                    query_params['ExclusiveStartKey'] = last_evaluated_key
                
                response = dynamodb_client.query(**query_params)
                
                for item in response.get('Items', []):
                    riders.append(Rider.from_dynamodb_item(item))
                
                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break
            
            return riders
        except Exception as e:
            logger.error(f"Error querying geohash {geohash} at precision {precision}: {str(e)}")
            return []
    
    @staticmethod
    def find_available_riders_near(lat: float, lng: float, radius_km: float = 5) -> List[Tuple[Rider, float]]:
        """
        Find available online riders within radius using geohash spatial indexing
        
        Returns: List of (Rider, distance_km) tuples sorted by distance
        """
        try:
            from utils.distance import calculate_distance
            
            # Determine optimal geohash precision for radius
            precision = get_precision_for_radius(radius_km)
            logger.info(f"Using geohash precision {precision} for {radius_km}km radius")
            
            # Get center geohash and neighbors
            center_geohash = geohash_encode(lat, lng, precision)
            geohashes_to_query = [center_geohash] + get_neighbors(center_geohash)
            
            logger.info(f"Querying {len(geohashes_to_query)} geohash cells: {geohashes_to_query}")
            
            # Query riders in all geohash cells
            all_riders = []
            for gh in geohashes_to_query:
                riders = RiderService._query_riders_by_geohash(gh, precision)
                all_riders.extend(riders)
            
            logger.info(f"Found {len(all_riders)} riders in geohash cells")
            
            # Filter: active and not working on order
            available_riders = [
                r for r in all_riders 
                if r.is_active and not r.working_on_order and r.lat and r.lng
            ]
            
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
                FilterExpression='isActive = :active AND attribute_not_exists(workingOnOrder)',
                ExpressionAttributeValues={
                    ':active': {'BOOL': True}
                }
            )
            
            nearby_riders = []
            for item in response.get('Items', []):
                rider = Rider.from_dynamodb_item(item)
                if rider.lat and rider.lng:
                    distance = calculate_distance(lat, lng, rider.lat, rider.lng)
                    if distance <= radius_km:
                        nearby_riders.append((rider, distance))
            
            # Sort by distance
            nearby_riders.sort(key=lambda x: x[1])
            
            return nearby_riders
        except ClientError as e:
            raise Exception(f"Failed to find nearby riders: {str(e)}")
