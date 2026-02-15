"""Location routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.location_service import LocationService
from utils.geohash import encode as geohash_encode
from services.restaurant_service import RestaurantService

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_location_routes(app):
    """Register location routes"""
    
    @app.post("/api/v1/location/reverse-geocode")
    @tracer.capture_method
    def reverse_geocode():
        """Reverse geocode coordinates to address"""
        try:
            body = app.current_event.json_body
            latitude = body.get('latitude')
            longitude = body.get('longitude')
            
            if latitude is None or longitude is None:
                return {"error": "latitude and longitude are required"}, 400
            
            logger.info(f"Reverse geocoding request: lat={latitude}, lon={longitude}")
            
            result = LocationService.reverse_geocode(
                float(latitude),
                float(longitude)
            )
            
            metrics.add_metric(name="ReverseGeocodeSuccess", unit="Count", value=1)
            return result, 200
            
        except Exception as e:
            logger.error("Error in reverse geocode", exc_info=True)
            metrics.add_metric(name="ReverseGeocodeFailed", unit="Count", value=1)
            return {"error": "Failed to reverse geocode", "message": str(e)}, 500
    
    @app.post("/api/v1/location/geohash")
    @tracer.capture_method
    def generate_geohash():
        """Generate geohash for given coordinates"""
        try:
            body = app.current_event.json_body
            latitude = body.get('latitude')
            longitude = body.get('longitude')
            precision = body.get('precision', 7)  # Default precision 7 (~153m)
            
            if latitude is None or longitude is None:
                return {"error": "latitude and longitude are required"}, 400
            
            logger.info(f"Geohash request: lat={latitude}, lon={longitude}, precision={precision}")
            
            geohash = geohash_encode(float(latitude), float(longitude), int(precision))
            
            metrics.add_metric(name="GeohashGenerated", unit="Count", value=1)
            return {
                "latitude": float(latitude),
                "longitude": float(longitude),
                "geohash": geohash,
                "precision": int(precision)
            }, 200
            
        except Exception as e:
            logger.error("Error generating geohash", exc_info=True)
            metrics.add_metric(name="GeohashFailed", unit="Count", value=1)
            return {"error": "Failed to generate geohash", "message": str(e)}, 500

    @app.post("/api/v1/location/distance")
    @tracer.capture_method
    def calculate_distance():
        """Calculate distance between two coordinates using Google Directions API"""
        try:
            body = app.current_event.json_body
            from_lat = body.get('fromLat')
            from_lng = body.get('fromLng')
            to_lat = body.get('toLat')
            to_lng = body.get('toLng')

            if from_lat is None or from_lng is None or to_lat is None or to_lng is None:
                return {"error": "fromLat, fromLng, toLat, toLng are required"}, 400

            distance_km = RestaurantService.calculate_distance(
                float(from_lat), float(from_lng), float(to_lat), float(to_lng)
            )

            return {
                "fromLat": float(from_lat),
                "fromLng": float(from_lng),
                "toLat": float(to_lat),
                "toLng": float(to_lng),
                "distanceKm": distance_km
            }, 200
        except Exception as e:
            logger.error("Error calculating distance", exc_info=True)
            return {"error": "Failed to calculate distance", "message": str(e)}, 500
