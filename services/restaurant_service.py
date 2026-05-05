"""Restaurant service"""
from typing import List, Optional, Set
import math
import concurrent.futures
import requests
from botocore.exceptions import ClientError
from models.restaurant import Restaurant
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import python_to_dynamodb
from utils.geohash import encode as geohash_encode, get_neighbors, get_precision_for_radius
from utils.distance import calculate_distance as haversine_distance
from utils.ssm import get_secret
from aws_lambda_powertools import Logger

logger = Logger()


class RestaurantService:
    """Service for restaurant operations"""

    @staticmethod
    def _normalize_image_list(value):
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value if v is not None and str(v).strip()]
        if isinstance(value, str) and value.strip():
            return [value]
        return []
    
    @staticmethod
    def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate straight-line distance using Haversine formula.
        
        Args:
            lat1, lon1: First coordinate
            lat2, lon2: Second coordinate
            
        Returns:
            Distance in kilometers
        """
        return haversine_distance(lat1, lon1, lat2, lon2)

    @staticmethod
    def calculate_road_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate road distance using the Google Routes API (computeRoutes).
        Falls back to Haversine when the API call fails or returns no route.
        """
        try:
            api_key = get_secret('GOOGLE_MAPS_API_KEY', '')
            if not api_key:
                logger.warning("GOOGLE_MAPS_API_KEY not configured. Falling back to Haversine distance.")
                return RestaurantService.calculate_distance(lat1, lon1, lat2, lon2)

            response = requests.post(
                "https://routes.googleapis.com/directions/v2:computeRoutes",
                headers={
                    "Content-Type": "application/json",
                    "X-Goog-Api-Key": api_key,
                    # Field mask is required; request only what we need to minimise cost.
                    "X-Goog-FieldMask": "routes.distanceMeters",
                },
                json={
                    "origin": {
                        "location": {"latLng": {"latitude": lat1, "longitude": lon1}}
                    },
                    "destination": {
                        "location": {"latLng": {"latitude": lat2, "longitude": lon2}}
                    },
                    "travelMode": "DRIVE",
                    "routingPreference": "TRAFFIC_UNAWARE",
                    # Routes API has no "optimize for distance" flag; instead we request
                    # alternative routes and pick the one with the smallest distance.
                    "computeAlternativeRoutes": True,
                },
                timeout=10,
            )
            data = response.json() if response.content else {}

            if response.status_code == 200:
                routes = data.get("routes") or []
                distances = [r["distanceMeters"] for r in routes if r.get("distanceMeters") is not None]
                if distances:
                    distances_km = [round(m / 1000.0, 3) for m in distances]
                    meters = min(distances)
                    chosen_km = round(meters / 1000.0, 3)
                    logger.info(
                        f"Google Routes API returned {len(distances)} route(s) "
                        f"from ({lat1},{lon1}) to ({lat2},{lon2}): {distances_km} km. "
                        f"Picked shortest: {chosen_km} km."
                    )
                    return chosen_km

            error_status = (data.get("error") or {}).get("status")
            error_message = (data.get("error") or {}).get("message")
            logger.warning(
                f"Google Routes API failed. statusCode={response.status_code}, "
                f"errorStatus={error_status}, errorMessage={error_message}. "
                "Falling back to Haversine distance."
            )
        except Exception as e:
            logger.warning(f"Google Routes API error: {str(e)}. Falling back to Haversine distance.")

        return RestaurantService.calculate_distance(lat1, lon1, lat2, lon2)
    
    @staticmethod
    def _query_restaurants_by_geohash(geohash: str, precision: int = 7) -> List[Restaurant]:
        """Query restaurants by geohash at specific precision with pagination"""
        try:
            restaurants = []
            last_evaluated_key = None
            
            # Determine which index and key to use based on precision
            query_params = {
                'TableName': TABLES['RESTAURANTS']
            }
            
            if precision == 7:
                # Use main table (PK)
                query_params['KeyConditionExpression'] = 'PK = :pk'
                query_params['ExpressionAttributeValues'] = {':pk': {'S': geohash}}
            elif precision == 6:
                # Use GSI1 (precision 6)
                query_params['IndexName'] = 'GSI1'
                query_params['KeyConditionExpression'] = 'GSI1PK = :gsi1pk'
                query_params['ExpressionAttributeValues'] = {':gsi1pk': {'S': geohash}}
            elif precision == 5:
                # Use GSI2 (precision 5)
                query_params['IndexName'] = 'GSI2'
                query_params['KeyConditionExpression'] = 'GSI2PK = :gsi2pk'
                query_params['ExpressionAttributeValues'] = {':gsi2pk': {'S': geohash}}
            elif precision == 4:
                # Use GSI3 (precision 4)
                query_params['IndexName'] = 'GSI3'
                query_params['KeyConditionExpression'] = 'GSI3PK = :gsi3pk'
                query_params['ExpressionAttributeValues'] = {':gsi3pk': {'S': geohash}}
            else:
                logger.error(f"Unsupported precision: {precision}")
                return []
            
            # Paginate through all results
            while True:
                if last_evaluated_key:
                    query_params['ExclusiveStartKey'] = last_evaluated_key
                
                response = dynamodb_client.query(**query_params)
                
                for item in response.get('Items', []):
                    restaurants.append(Restaurant.from_dynamodb_item(item))
                
                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break  # No more pages
            
            return restaurants
        except ClientError as e:
            logger.error(f"Error querying geohash {geohash} at precision {precision}: {str(e)}")
            return []
    
    @staticmethod
    def find_nearby_restaurants(
        latitude: float,
        longitude: float,
        min_results: int = 50,
        max_distance_km: float = 5.0
    ) -> List[Restaurant]:
        """
        Find nearby restaurants using geohash with intelligent precision reduction
        
        Args:
            latitude: User's latitude
            longitude: User's longitude
            min_results: Minimum number of restaurants to return
            max_distance_km: Maximum distance in kilometers
            
        Returns:
            List of nearby restaurants within max_distance, sorted by distance
        """
        logger.info(f"🔍 Starting nearby restaurant search")
        logger.info(f"   User location: {latitude:.6f}, {longitude:.6f}")
        logger.info(f"   Min results: {min_results}, Max distance: {max_distance_km}km")
        
        all_restaurants = []
        seen_ids: Set[str] = set()
        
        # Try different precisions until we have enough restaurants
        for precision in range(7, 3, -1):  # Try 7, 6, 5, 4
            if len(all_restaurants) >= min_results:
                logger.info(f"✅ Target reached: {len(all_restaurants)} restaurants (>= {min_results})")
                break
            
            geohash = geohash_encode(latitude, longitude, precision)
            neighbors = get_neighbors(geohash)
            geohashes_to_query = [geohash] + neighbors
            
            logger.info(f"📊 Precision {precision} search:")
            logger.info(f"   Center geohash: {geohash}")
            logger.info(f"   Neighbors: {neighbors}")
            logger.info(f"   Total geohashes to query: {len(geohashes_to_query)}")
            
            query_start_time = __import__('time').time()
            restaurants_found_in_iteration = 0
            
            # Query geohashes in parallel for speed
            with concurrent.futures.ThreadPoolExecutor(max_workers=9) as executor:
                future_to_geohash = {
                    executor.submit(RestaurantService._query_restaurants_by_geohash, gh, precision): gh 
                    for gh in geohashes_to_query
                }
                
                for future in concurrent.futures.as_completed(future_to_geohash):
                    geohash_queried = future_to_geohash[future]
                    try:
                        restaurants = future.result()
                        logger.info(f"   Geohash {geohash_queried}: {len(restaurants)} restaurants found")
                        
                        for restaurant in restaurants:
                            if restaurant.restaurant_id not in seen_ids:
                                # Calculate distance
                                distance = RestaurantService.calculate_distance(
                                    latitude, longitude,
                                    restaurant.latitude, restaurant.longitude
                                )
                                
                                logger.info(f"      {restaurant.name}: {distance:.2f}km away")
                                
                                # Only include if within max distance
                                if distance <= max_distance_km:
                                    restaurant.distance = distance
                                    all_restaurants.append(restaurant)
                                    seen_ids.add(restaurant.restaurant_id)
                                    restaurants_found_in_iteration += 1
                                    logger.info(f"         ✅ Added (within {max_distance_km}km)")
                                else:
                                    logger.info(f"         ❌ Skipped (beyond {max_distance_km}km)")
                    except Exception as e:
                        logger.error(f"   ❌ Error querying geohash {geohash_queried}: {str(e)}")
            
            query_duration = __import__('time').time() - query_start_time
            logger.info(f"   Precision {precision} complete: {restaurants_found_in_iteration} new restaurants in {query_duration:.2f}s")
            logger.info(f"   Total so far: {len(all_restaurants)} restaurants")
        
        # Sort by distance
        logger.info(f"🔄 Sorting {len(all_restaurants)} restaurants by distance...")
        all_restaurants.sort(key=lambda r: r.distance if hasattr(r, 'distance') else float('inf'))
        
        if all_restaurants:
            logger.info(f"   Nearest: {all_restaurants[0].name} ({all_restaurants[0].distance:.2f}km)")
            if len(all_restaurants) > 1:
                logger.info(f"   Farthest: {all_restaurants[-1].name} ({all_restaurants[-1].distance:.2f}km)")
        
        final_count = min(len(all_restaurants), min_results)
        logger.info(f"✅ Returning {final_count} restaurants (capped at {min_results})")
        
        return all_restaurants[:min_results]  # Cap at min_results
    
    @staticmethod
    def get_restaurant(restaurant_id: str, location_id: Optional[str] = None) -> Optional[Restaurant]:
        """Get restaurant by ID. If location_id is provided, uses PK/SK. Otherwise uses GSI1."""
        try:
            if location_id:
                # Use PK/SK lookup
                pk = f"LOCATION#{location_id}"
                sk = f"RESTAURANT#{restaurant_id}"
                response = dynamodb_client.get_item(
                    TableName=TABLES['RESTAURANTS'],
                    Key={
                        'PK': {'S': pk},
                        'SK': {'S': sk}
                    }
                )
                
                if 'Item' not in response:
                    return None
                
                return Restaurant.from_dynamodb_item(response['Item'])
            else:
                # Use GSI1 to find by restaurant_id
                # Note: This requires knowing the owner_id or scanning, so we'll query by location
                # For now, we'll need location_id. If not provided, we can't efficiently find it.
                # This is a limitation - in production, you might want a restaurant_id-index GSI
                raise Exception("location_id is required to get restaurant by ID")
        except ClientError as e:
            raise Exception(f"Failed to get restaurant: {str(e)}")
    
    @staticmethod
    def get_restaurants_by_location(location_id: str) -> List[Restaurant]:
        """Get all restaurants in a location (city)"""
        try:
            pk = f"LOCATION#{location_id}"
            response = dynamodb_client.query(
                TableName=TABLES['RESTAURANTS'],
                KeyConditionExpression='PK = :pk',
                ExpressionAttributeValues={
                    ':pk': {'S': pk}
                }
            )
            
            restaurants = []
            for item in response.get('Items', []):
                restaurants.append(Restaurant.from_dynamodb_item(item))
            
            return restaurants
        except ClientError as e:
            raise Exception(f"Failed to get restaurants by location: {str(e)}")
    
    @staticmethod
    def get_restaurants_by_owner(owner_id: str) -> List[Restaurant]:
        """Get all restaurants for an owner using GSI1"""
        try:
            gsi1pk = f"OWNER#{owner_id}"
            response = dynamodb_client.query(
                TableName=TABLES['RESTAURANTS'],
                IndexName='GSI1',
                KeyConditionExpression='GSI1PK = :gsi1pk',
                ExpressionAttributeValues={
                    ':gsi1pk': {'S': gsi1pk}
                }
            )
            
            restaurants = []
            for item in response.get('Items', []):
                restaurants.append(Restaurant.from_dynamodb_item(item))
            
            return restaurants
        except ClientError as e:
            raise Exception(f"Failed to get restaurants by owner: {str(e)}")
    
    @staticmethod
    def create_restaurant(restaurant: Restaurant) -> Restaurant:
        """Create a new restaurant"""
        try:
            dynamodb_client.put_item(
                TableName=TABLES['RESTAURANTS'],
                Item=restaurant.to_dynamodb_item()
            )
            return restaurant
        except ClientError as e:
            raise Exception(f"Failed to create restaurant: {str(e)}")
    
    @staticmethod
    def list_restaurants() -> List[Restaurant]:
        """List all restaurants (scan - use sparingly)"""
        try:
            response = dynamodb_client.scan(
                TableName=TABLES['RESTAURANTS']
            )
            
            restaurants = []
            for item in response.get('Items', []):
                restaurants.append(Restaurant.from_dynamodb_item(item))
            
            return restaurants
        except ClientError as e:
            raise Exception(f"Failed to list restaurants: {str(e)}")
    
    @staticmethod
    def get_restaurant_by_id(restaurant_id: str) -> Optional[Restaurant]:
        """Get restaurant by restaurant ID using GSI"""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['RESTAURANTS'],
                IndexName='restaurantId-index',
                KeyConditionExpression='restaurantId = :restaurant_id',
                ExpressionAttributeValues={
                    ':restaurant_id': {'S': restaurant_id}
                }
            )
            
            items = response.get('Items', [])
            if not items:
                return None
            
            return Restaurant.from_dynamodb_item(items[0])
        except ClientError as e:
            raise Exception(f"Failed to get restaurant by ID: {str(e)}")
    
    @staticmethod
    def update_restaurant(restaurant_id: str, updates: dict) -> Restaurant:
        """Update restaurant information"""
        try:
            # First, get the existing restaurant
            existing_restaurant = RestaurantService.get_restaurant_by_id(restaurant_id)
            if not existing_restaurant:
                raise Exception("Restaurant not found")
            
            logger.info(f"Current restaurant state: {existing_restaurant.name} at {existing_restaurant.latitude}, {existing_restaurant.longitude}, geohash: {existing_restaurant.geohash}")
            
            # Check if lat/lng is being updated
            lat_changed = 'latitude' in updates and updates['latitude'] != existing_restaurant.latitude
            lng_changed = 'longitude' in updates and updates['longitude'] != existing_restaurant.longitude
            location_changed = lat_changed or lng_changed
            
            if location_changed:
                # Location changed - need to delete old entry and create new one (PK changed)
                new_lat = updates.get('latitude', existing_restaurant.latitude)
                new_lng = updates.get('longitude', existing_restaurant.longitude)
                
                logger.info(f"⚠️ Location changed! Old: {existing_restaurant.latitude}, {existing_restaurant.longitude} -> New: {new_lat}, {new_lng}")
                logger.info(f"   Deleting old entry with geohash: {existing_restaurant.geohash}")
                
                # Delete old entry
                dynamodb_client.delete_item(
                    TableName=TABLES['RESTAURANTS'],
                    Key={
                        'PK': {'S': existing_restaurant.geohash},
                        'SK': {'S': f"RESTAURANT#{restaurant_id}"}
                    }
                )
                
                # Create new restaurant object with updated data
                updated_restaurant = Restaurant(
                    location_id=existing_restaurant.location_id,
                    restaurant_id=restaurant_id,
                    name=updates.get('name', existing_restaurant.name),
                    latitude=float(new_lat),
                    longitude=float(new_lng),
                    is_open=updates.get('isOpen', existing_restaurant.is_open),
                    cuisine=updates.get('cuisine', existing_restaurant.cuisine),
                    rating=updates.get('rating', existing_restaurant.rating),
                    rated_count=existing_restaurant.rated_count,
                    owner_id=updates.get('ownerId', existing_restaurant.owner_id),
                    restaurant_image=updates.get('restaurantImage', existing_restaurant.restaurant_image),
                    closes_at=updates.get('closesAt', existing_restaurant.closes_at),
                    opens_at=updates.get('opensAt', existing_restaurant.opens_at),
                    avg_preparation_time=updates.get('avgPreparationTime', existing_restaurant.avg_preparation_time),
                    fcm_token=updates.get('fcmToken', existing_restaurant.fcm_token),
                    fcm_token_updated_at=updates.get('fcmTokenUpdatedAt', existing_restaurant.fcm_token_updated_at),
                    position=updates.get('position', existing_restaurant.position),
                    top_offer_banner=updates.get('topOfferBanner', existing_restaurant.top_offer_banner)
                )
                
                logger.info(f"   Creating new entry with geohash: {updated_restaurant.geohash}")
                logger.info(f"   New geohashes - P7: {updated_restaurant.geohash}, P6: {updated_restaurant.geohash_6}, P5: {updated_restaurant.geohash_5}, P4: {updated_restaurant.geohash_4}")
                
                # Write new entry
                dynamodb_client.put_item(
                    TableName=TABLES['RESTAURANTS'],
                    Item=updated_restaurant.to_dynamodb_item()
                )
                
                return updated_restaurant
            
            # No location change - regular update
            pk = existing_restaurant.geohash
            sk = f"RESTAURANT#{restaurant_id}"
            
            update_expressions = []
            expression_attribute_names = {}
            expression_attribute_values = {}
            
            if 'latitude' in updates:
                update_expressions.append('#latitude = :latitude')
                expression_attribute_names['#latitude'] = 'latitude'
                expression_attribute_values[':latitude'] = {'N': str(updates['latitude'])}
            
            if 'longitude' in updates:
                update_expressions.append('#longitude = :longitude')
                expression_attribute_names['#longitude'] = 'longitude'
                expression_attribute_values[':longitude'] = {'N': str(updates['longitude'])}
                
            # Compare and build update expressions only for changed fields
            if 'name' in updates and updates['name'] != existing_restaurant.name:
                update_expressions.append('#name = :name')
                expression_attribute_names['#name'] = 'name'
                expression_attribute_values[':name'] = {'S': updates['name']}
            
            if 'isOpen' in updates and updates['isOpen'] != existing_restaurant.is_open:
                update_expressions.append('#isOpen = :isOpen')
                expression_attribute_names['#isOpen'] = 'isOpen'
                expression_attribute_values[':isOpen'] = {'BOOL': updates['isOpen']}
            
            if 'restaurantImage' in updates:
                normalized_new_images = RestaurantService._normalize_image_list(updates['restaurantImage'])
                normalized_existing_images = RestaurantService._normalize_image_list(existing_restaurant.restaurant_image)
            else:
                normalized_new_images = None
                normalized_existing_images = None

            if normalized_new_images is not None and normalized_new_images != normalized_existing_images:
                update_expressions.append('#restaurant_image = :restaurant_image')
                expression_attribute_names['#restaurant_image'] = 'restaurant_image'
                expression_attribute_values[':restaurant_image'] = {
                    'L': [{'S': img} for img in normalized_new_images]
                }
            
            if 'cuisine' in updates:
                cuisine_list = updates['cuisine']
                if isinstance(cuisine_list, list):
                    update_expressions.append('#cuisine = :cuisine')
                    expression_attribute_names['#cuisine'] = 'cuisine'
                    expression_attribute_values[':cuisine'] = {'L': [{'S': str(c)} for c in cuisine_list]}
            
            if 'rating' in updates and updates.get('rating') != existing_restaurant.rating:
                update_expressions.append('#rating = :rating')
                expression_attribute_names['#rating'] = 'rating'
                expression_attribute_values[':rating'] = {'N': str(updates['rating'])}
            
            if 'ownerId' in updates and updates['ownerId'] != existing_restaurant.owner_id:
                update_expressions.append('#ownerId = :ownerId')
                expression_attribute_names['#ownerId'] = 'ownerId'
                expression_attribute_values[':ownerId'] = {'S': updates['ownerId']}

            if 'closesAt' in updates and updates['closesAt'] != existing_restaurant.closes_at:
                update_expressions.append('#closesAt = :closesAt')
                expression_attribute_names['#closesAt'] = 'closesAt'
                expression_attribute_values[':closesAt'] = {'S': updates['closesAt']}

            if 'opensAt' in updates and updates['opensAt'] != existing_restaurant.opens_at:
                update_expressions.append('#opensAt = :opensAt')
                expression_attribute_names['#opensAt'] = 'opensAt'
                expression_attribute_values[':opensAt'] = {'S': updates['opensAt']}

            if 'avgPreparationTime' in updates:
                expression_attribute_names['#avgPreparationTime'] = 'avgPreparationTime'
                if updates['avgPreparationTime'] is not None and updates['avgPreparationTime'] != existing_restaurant.avg_preparation_time:
                    update_expressions.append('#avgPreparationTime = :avgPreparationTime')
                    expression_attribute_values[':avgPreparationTime'] = {'N': str(int(updates['avgPreparationTime']))}

            if 'position' in updates:
                expression_attribute_names['#position'] = 'position'
                if updates['position'] is None:
                    pass
                elif updates['position'] != existing_restaurant.position:
                    update_expressions.append('#position = :position')
                    expression_attribute_values[':position'] = {'N': str(int(updates['position']))}

            if 'topOfferBanner' in updates:
                expression_attribute_names['#topOfferBanner'] = 'topOfferBanner'
                if updates['topOfferBanner'] is None:
                    pass
                elif updates['topOfferBanner'] != existing_restaurant.top_offer_banner:
                    update_expressions.append('#topOfferBanner = :topOfferBanner')
                    expression_attribute_values[':topOfferBanner'] = {'S': str(updates['topOfferBanner'])}

            if 'fcmToken' in updates:
                expression_attribute_names['#fcmToken'] = 'fcmToken'
                if updates['fcmToken'] is not None:
                    update_expressions.append('#fcmToken = :fcmToken')
                    expression_attribute_values[':fcmToken'] = {'S': str(updates['fcmToken'])}

            if 'fcmTokenUpdatedAt' in updates:
                expression_attribute_names['#fcmTokenUpdatedAt'] = 'fcmTokenUpdatedAt'
                if updates['fcmTokenUpdatedAt'] is not None:
                    update_expressions.append('#fcmTokenUpdatedAt = :fcmTokenUpdatedAt')
                    expression_attribute_values[':fcmTokenUpdatedAt'] = {'S': str(updates['fcmTokenUpdatedAt'])}

            if 'shiftTimings' in updates:
                update_expressions.append('#shiftTimings = :shiftTimings')
                expression_attribute_names['#shiftTimings'] = 'shiftTimings'
                expression_attribute_values[':shiftTimings'] = python_to_dynamodb(updates['shiftTimings'] or [])

            if 'timezone' in updates:
                tz_val = str(updates['timezone'] or 'Asia/Kolkata').strip()
                update_expressions.append('#timezone = :timezone')
                expression_attribute_names['#timezone'] = 'timezone'
                expression_attribute_values[':timezone'] = {'S': tz_val}

            set_expressions = [expr for expr in update_expressions if expr]
            remove_expressions = []
            if 'fcmToken' in updates and updates['fcmToken'] is None:
                remove_expressions.append('#fcmToken')
            if 'fcmTokenUpdatedAt' in updates and updates['fcmTokenUpdatedAt'] is None:
                remove_expressions.append('#fcmTokenUpdatedAt')
            if 'fcmTokens' in updates and not updates.get('fcmTokens'):
                remove_expressions.append('#fcmTokens')
            if 'position' in updates and updates['position'] is None:
                remove_expressions.append('#position')
            if 'topOfferBanner' in updates and updates['topOfferBanner'] is None:
                remove_expressions.append('#topOfferBanner')
            if 'avgPreparationTime' in updates and updates['avgPreparationTime'] is None:
                remove_expressions.append('#avgPreparationTime')

            if not set_expressions and not remove_expressions:
                logger.info("No changes detected, returning existing restaurant")
                return existing_restaurant
            
            logger.info(f"Updating fields: {list(updates.keys())}")
            
            update_kwargs = {
                'TableName': TABLES['RESTAURANTS'],
                'Key': {
                    'PK': {'S': pk},
                    'SK': {'S': sk}
                },
                'UpdateExpression': " ".join(
                    part for part in [
                        f"SET {', '.join(set_expressions)}" if set_expressions else "",
                        f"REMOVE {', '.join(remove_expressions)}" if remove_expressions else ""
                    ] if part
                ),
                'ExpressionAttributeNames': expression_attribute_names
            }

            if expression_attribute_names:
                update_kwargs['ExpressionAttributeNames'] = expression_attribute_names
            elif 'ExpressionAttributeNames' in update_kwargs:
                del update_kwargs['ExpressionAttributeNames']

            if expression_attribute_values:
                update_kwargs['ExpressionAttributeValues'] = expression_attribute_values

            dynamodb_client.update_item(
                **update_kwargs
            )
            
            logger.info(f"✅ Restaurant updated successfully")
            
            # Return updated restaurant
            return RestaurantService.get_restaurant_by_id(restaurant_id)
        except ClientError as e:
            raise Exception(f"Failed to update restaurant: {str(e)}")

    @staticmethod
    def add_rating(restaurant_id: str, new_rating: float) -> Restaurant:
        """Add a new rating to restaurant and recompute average + ratedCount."""
        try:
            restaurant = RestaurantService.get_restaurant_by_id(restaurant_id)
            if not restaurant:
                raise Exception("Restaurant not found")

            current_count = restaurant.rated_count or 0
            current_avg = restaurant.rating or 0.0
            updated_count = current_count + 1
            updated_avg = round(((current_avg * current_count) + float(new_rating)) / updated_count, 2)

            dynamodb_client.update_item(
                TableName=TABLES['RESTAURANTS'],
                Key={
                    'PK': {'S': restaurant.geohash},
                    'SK': {'S': f"RESTAURANT#{restaurant_id}"}
                },
                UpdateExpression='SET rating = :rating, ratedCount = :ratedCount',
                ExpressionAttributeValues={
                    ':rating': {'N': str(updated_avg)},
                    ':ratedCount': {'N': str(updated_count)}
                }
            )

            return RestaurantService.get_restaurant_by_id(restaurant_id)
        except ClientError as e:
            raise Exception(f"Failed to add restaurant rating: {str(e)}")

    @staticmethod
    def add_fcm_token(restaurant_id: str, fcm_token: str, updated_at: str) -> Restaurant:
        """Atomically add an FCM token to restaurant's token set."""
        restaurant = RestaurantService.get_restaurant_by_id(restaurant_id)
        if not restaurant:
            raise Exception("Restaurant not found")

        dynamodb_client.update_item(
            TableName=TABLES['RESTAURANTS'],
            Key={
                'PK': {'S': restaurant.geohash},
                'SK': {'S': f"RESTAURANT#{restaurant_id}"}
            },
            UpdateExpression='ADD #fcmTokens :tokenSet SET #fcmToken = :fcmToken, #fcmTokenUpdatedAt = :updatedAt',
            ExpressionAttributeNames={
                '#fcmTokens': 'fcmTokens',
                '#fcmToken': 'fcmToken',
                '#fcmTokenUpdatedAt': 'fcmTokenUpdatedAt'
            },
            ExpressionAttributeValues={
                ':tokenSet': {'SS': [fcm_token]},
                ':fcmToken': {'S': fcm_token},
                ':updatedAt': {'S': updated_at}
            }
        )
        return RestaurantService.get_restaurant_by_id(restaurant_id)

    @staticmethod
    def remove_fcm_token(restaurant_id: str, fcm_token: Optional[str], updated_at: Optional[str]) -> Restaurant:
        """Remove one device token; optionally clear all when token not provided."""
        from utils.datetime_ist import now_ist_iso
        updated_at = updated_at or now_ist_iso()
        restaurant = RestaurantService.get_restaurant_by_id(restaurant_id)
        if not restaurant:
            raise Exception("Restaurant not found")

        if fcm_token:
            dynamodb_client.update_item(
                TableName=TABLES['RESTAURANTS'],
                Key={
                    'PK': {'S': restaurant.geohash},
                    'SK': {'S': f"RESTAURANT#{restaurant_id}"}
                },
                UpdateExpression='DELETE #fcmTokens :tokenSet SET #fcmTokenUpdatedAt = :updatedAt',
                ExpressionAttributeNames={
                    '#fcmTokens': 'fcmTokens',
                    '#fcmTokenUpdatedAt': 'fcmTokenUpdatedAt'
                },
                ExpressionAttributeValues={
                    ':tokenSet': {'SS': [fcm_token]},
                    ':updatedAt': {'S': updated_at}
                }
            )
            latest = RestaurantService.get_restaurant_by_id(restaurant_id)
            tokens = latest.fcm_tokens or []
            RestaurantService.update_restaurant(restaurant_id, {'fcmToken': tokens[-1] if tokens else None})
            return RestaurantService.get_restaurant_by_id(restaurant_id)

        RestaurantService.update_restaurant(restaurant_id, {
            'fcmToken': None,
            'fcmTokens': [],
            'fcmTokenUpdatedAt': updated_at
        })
        return RestaurantService.get_restaurant_by_id(restaurant_id)
