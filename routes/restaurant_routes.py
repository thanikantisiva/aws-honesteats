"""Restaurant routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.restaurant_service import RestaurantService
from models.restaurant import Restaurant
from utils.dynamodb import generate_id
from utils.geohash import encode as geohash_encode


logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_restaurant_routes(app):
    """Register restaurant routes"""
    
    @app.get("/api/v1/restaurants")
    @tracer.capture_method
    def list_restaurants():
        """List all restaurants, filtered by proximity using geohash"""
        try:
            query_params = app.current_event.query_string_parameters or {}
            user_lat = query_params.get('latitude')
            user_lng = query_params.get('longitude')
            geohash_param = query_params.get('geohash')
            location_id = query_params.get('locationId')
            
            logger.info(f"Listing restaurants - lat: {user_lat}, lng: {user_lng}, geohash: {geohash_param}")
            
            # Require user coordinates for geohash-based spatial query
            if not user_lat or not user_lng:
                return {"error": "latitude and longitude are required"}, 400
            
            # Use geohash-based spatial query
            restaurants = RestaurantService.find_nearby_restaurants(
                float(user_lat),
                float(user_lng),
                min_results=50,
                max_distance_km=5.0
            )
            
            restaurant_list = []
            for r in restaurants:
                restaurant_dict = r.to_dict()
                distance_km = round(r.distance, 2) if hasattr(r, 'distance') else None
                restaurant_dict['distance'] = distance_km
                
                # Calculate delivery time: (distance / 35 km/hr) + 15 mins prep time
                if distance_km:
                    travel_time_hours = distance_km / 35.0  # Speed: 35 km/hr
                    travel_time_mins = travel_time_hours * 60
                    total_time_mins = int(travel_time_mins + 15)  # Add 15 mins for food prep
                    restaurant_dict['deliveryTimeMinutes'] = total_time_mins
                    
                    # Calculate delivery fee: ₹12 per km, minimum ₹60
                    delivery_fee = max(60, int(distance_km * 12))
                    restaurant_dict['deliveryFee'] = delivery_fee
                    
                    logger.info(f"   {r.name}: {distance_km}km → {total_time_mins}mins, ₹{delivery_fee} delivery")
                else:
                    restaurant_dict['deliveryTimeMinutes'] = None
                    restaurant_dict['deliveryFee'] = 60  # Default minimum
                
                restaurant_list.append(restaurant_dict)
            
            metrics.add_metric(name="RestaurantsListed", unit="Count", value=1)
            
            return {
                "restaurants": restaurant_list,
                "total": len(restaurant_list)
            }, 200
        except Exception as e:
            logger.error("Error listing restaurants", exc_info=True)
            return {"error": "Failed to list restaurants", "message": str(e)}, 500
    
    @app.get("/api/v1/restaurants/<restaurant_id>")
    @tracer.capture_method
    def get_restaurant(restaurant_id: str):
        """Get restaurant by ID"""
        try:
            logger.info(f"Getting restaurant: {restaurant_id}")
            # Try to get from query params or scan (for now, we'll need location_id)
            # For MVP, we can list all and filter, but ideally location_id should be provided
            restaurants = RestaurantService.list_restaurants()
            restaurant = next((r for r in restaurants if r.restaurant_id == restaurant_id), None)
            
            if not restaurant:
                return {"error": "Restaurant not found"}, 404
            
            metrics.add_metric(name="RestaurantRetrieved", unit="Count", value=1)
            return restaurant.to_dict(), 200
        except Exception as e:
            logger.error("Error getting restaurant", exc_info=True)
            return {"error": "Failed to get restaurant", "message": str(e)}, 500
    
    @app.post("/api/v1/restaurants")
    @tracer.capture_method
    def create_restaurant():
        """Create a new restaurant"""
        try:
            body = app.current_event.json_body
            location_id = body.get('locationId')
            restaurant_id = generate_id('RES')
            name = body.get('name')
            latitude = body.get('latitude')
            longitude = body.get('longitude')
            
            if not all([location_id, name, latitude is not None, longitude is not None]):
                return {"error": "locationId, name, latitude, and longitude are required"}, 400
            
            logger.info(f"Creating restaurant: {name}, ID: {restaurant_id}, Location: {latitude}, {longitude}")
            
            restaurant = Restaurant(
                location_id=location_id,
                restaurant_id=restaurant_id,
                name=name,
                latitude=float(latitude),
                longitude=float(longitude),
                is_open=body.get('isOpen', True),
                cuisine=body.get('cuisine', []),
                rating=body.get('rating'),
                owner_id=body.get('ownerId'),
                restaurant_image=body.get('restaurantImage')
                # geohash auto-generated in __init__
            )
            
            logger.info(f"Generated geohashes - P7: {restaurant.geohash}, P6: {restaurant.geohash_6}, P5: {restaurant.geohash_5}, P4: {restaurant.geohash_4}")
            
            created_restaurant = RestaurantService.create_restaurant(restaurant)
            metrics.add_metric(name="RestaurantCreated", unit="Count", value=1)
            
            return created_restaurant.to_dict(), 201
        except Exception as e:
            logger.error("Error creating restaurant", exc_info=True)
            return {"error": "Failed to create restaurant", "message": str(e)}, 500
    
    @app.put("/api/v1/restaurants/<restaurant_id>")
    @tracer.capture_method
    def update_restaurant(restaurant_id: str):
        """Update restaurant information"""
        try:
            body = app.current_event.json_body
            
            
            updates = {}
            
            if 'name' in body:
                updates['name'] = body['name']
            if 'latitude' in body:
                updates['latitude'] = float(body['latitude'])
            if 'longitude' in body:
                updates['longitude'] = float(body['longitude'])
            if 'isOpen' in body:
                updates['isOpen'] = body['isOpen']
            if 'restaurantImage' in body:
                updates['restaurantImage'] = body['restaurantImage']
            if 'cuisine' in body:
                updates['cuisine'] = body['cuisine'] if isinstance(body['cuisine'], list) else []
            if 'rating' in body:
                updates['rating'] = float(body['rating'])
            if 'ownerId' in body:
                updates['ownerId'] = body['ownerId']
            
            if not updates:
                return {"error": "No fields to update"}, 400
            
            logger.info(f"Updating restaurant: {restaurant_id} with updates: {list(updates.keys())}")
            
            updated_restaurant = RestaurantService.update_restaurant(restaurant_id, updates)
            metrics.add_metric(name="RestaurantUpdated", unit="Count", value=1)
            
            return updated_restaurant.to_dict(), 200
        except Exception as e:
            logger.error("Error updating restaurant", exc_info=True)
            return {"error": "Failed to update restaurant", "message": str(e)}, 500

