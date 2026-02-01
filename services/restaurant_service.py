"""Restaurant service"""
from typing import List, Optional, Set
import math
import concurrent.futures
from botocore.exceptions import ClientError
from models.restaurant import Restaurant
from utils.dynamodb import dynamodb_client, TABLES
from utils.geohash import encode as geohash_encode, get_neighbors, get_precision_for_radius
from aws_lambda_powertools import Logger

logger = Logger()


class RestaurantService:
    """Service for restaurant operations"""
    
    @staticmethod
    def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate distance between two coordinates using Haversine formula
        
        Args:
            lat1, lon1: First coordinate
            lat2, lon2: Second coordinate
            
        Returns:
            Distance in kilometers
        """
        R = 6371  # Earth's radius in km
        
        dLat = math.radians(lat2 - lat1)
        dLon = math.radians(lon2 - lon1)
        
        a = (math.sin(dLat / 2) * math.sin(dLat / 2) +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dLon / 2) * math.sin(dLon / 2))
        
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c
        
        return distance
    
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
                    owner_id=updates.get('ownerId', existing_restaurant.owner_id),
                    restaurant_image=updates.get('restaurantImage', existing_restaurant.restaurant_image)
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
            
            if 'name' in updates:
                update_expressions.append('#name = :name')
                expression_attribute_names['#name'] = 'name'
                expression_attribute_values[':name'] = {'S': updates['name']}
            
            if 'latitude' in updates:
                update_expressions.append('#latitude = :latitude')
                expression_attribute_names['#latitude'] = 'latitude'
                expression_attribute_values[':latitude'] = {'N': str(updates['latitude'])}
            
            if 'longitude' in updates:
                update_expressions.append('#longitude = :longitude')
                expression_attribute_names['#longitude'] = 'longitude'
                expression_attribute_values[':longitude'] = {'N': str(updates['longitude'])}
            
            if 'isOpen' in updates:
                update_expressions.append('#isOpen = :isOpen')
                expression_attribute_names['#isOpen'] = 'isOpen'
                expression_attribute_values[':isOpen'] = {'BOOL': updates['isOpen']}
            
            if 'restaurantImage' in updates:
                update_expressions.append('#restaurant_image = :restaurant_image')
                expression_attribute_names['#restaurant_image'] = 'restaurant_image'
                expression_attribute_values[':restaurant_image'] = {'S': updates['restaurantImage']}
            
            if 'cuisine' in updates:
                update_expressions.append('#cuisine = :cuisine')
                expression_attribute_names['#cuisine'] = 'cuisine'
                cuisine_list = updates['cuisine']
                if isinstance(cuisine_list, list):
                    expression_attribute_values[':cuisine'] = {'L': [{'S': str(c)} for c in cuisine_list]}
                else:
                    expression_attribute_values[':cuisine'] = {'L': []}
            
            if 'rating' in updates:
                update_expressions.append('#rating = :rating')
                expression_attribute_names['#rating'] = 'rating'
                expression_attribute_values[':rating'] = {'N': str(updates['rating'])}
            
            if 'ownerId' in updates:
                update_expressions.append('#GSI1PK = :gsi1pk, #GSI1SK = :gsi1sk')
                expression_attribute_names['#GSI1PK'] = 'GSI1PK'
                expression_attribute_names['#GSI1SK'] = 'GSI1SK'
                owner_id = updates['ownerId']
                expression_attribute_values[':gsi1pk'] = {'S': f"OWNER#{owner_id}"}
                expression_attribute_values[':gsi1sk'] = {'S': f"RESTAURANT#{restaurant_id}"}
            
            # Compare and build update expressions only for changed fields
            if 'name' in updates and updates['name'] != existing_restaurant.name:
                update_expressions.append('#name = :name')
                expression_attribute_names['#name'] = 'name'
                expression_attribute_values[':name'] = {'S': updates['name']}
            
            if 'isOpen' in updates and updates['isOpen'] != existing_restaurant.is_open:
                update_expressions.append('#isOpen = :isOpen')
                expression_attribute_names['#isOpen'] = 'isOpen'
                expression_attribute_values[':isOpen'] = {'BOOL': updates['isOpen']}
            
            if 'restaurantImage' in updates and updates['restaurantImage'] != existing_restaurant.restaurant_image:
                update_expressions.append('#restaurant_image = :restaurant_image')
                expression_attribute_names['#restaurant_image'] = 'restaurant_image'
                expression_attribute_values[':restaurant_image'] = {'S': updates['restaurantImage']}
            
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
            
            if not update_expressions:
                logger.info("No changes detected, returning existing restaurant")
                return existing_restaurant
            
            logger.info(f"Updating fields: {list(updates.keys())}")
            
            dynamodb_client.update_item(
                TableName=TABLES['RESTAURANTS'],
                Key={
                    'PK': {'S': pk},
                    'SK': {'S': sk}
                },
                UpdateExpression=f"SET {', '.join(update_expressions)}",
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            
            logger.info(f"✅ Restaurant updated successfully")
            
            # Return updated restaurant
            return RestaurantService.get_restaurant_by_id(restaurant_id)
        except ClientError as e:
            raise Exception(f"Failed to update restaurant: {str(e)}")
