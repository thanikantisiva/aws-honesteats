"""Rider routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.rider_service import RiderService
from models.rider import Rider
from utils.dynamodb import generate_id

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_rider_routes(app):
    """Register rider routes"""
    
    @app.get("/api/v1/riders")
    @tracer.capture_method
    def list_riders():
        """List all riders"""
        try:
            logger.info("Listing riders")
            riders = RiderService.list_riders()
            metrics.add_metric(name="RidersListed", unit="Count", value=1)
            
            return {
                "riders": [r.to_dict() for r in riders],
                "total": len(riders)
            }, 200
        except Exception as e:
            logger.error("Error listing riders", exc_info=True)
            return {"error": "Failed to list riders", "message": str(e)}, 500
    
    @app.get("/api/v1/riders/<rider_id>")
    @tracer.capture_method
    def get_rider(rider_id: str):
        """Get rider by ID"""
        try:
            logger.info(f"Getting rider: {rider_id}")
            rider = RiderService.get_rider(rider_id)
            
            if not rider:
                return {"error": "Rider not found"}, 404
            
            metrics.add_metric(name="RiderRetrieved", unit="Count", value=1)
            return rider.to_dict(), 200
        except Exception as e:
            logger.error("Error getting rider", exc_info=True)
            return {"error": "Failed to get rider", "message": str(e)}, 500
    
    @app.post("/api/v1/riders")
    @tracer.capture_method
    def create_rider():
        """Create a new rider (admin endpoint - direct operational record creation)"""
        try:
            body = app.current_event.json_body
            phone = body.get('phone')
            
            if not phone:
                return {"error": "Phone number is required"}, 400
            
            logger.info(f"Creating rider operational record: {phone}")
            
            rider = Rider(
                rider_id=generate_id('RDR'),
                phone=phone,
                is_active=body.get('isActive', False),
                lat=body.get('lat'),
                lng=body.get('lng'),
                speed=body.get('speed', 0.0),
                heading=body.get('heading', 0.0),
                working_on_order=None
            )
            
            created_rider = RiderService.create_rider(rider)
            metrics.add_metric(name="RiderCreated", unit="Count", value=1)
            
            return created_rider.to_dict(), 201
        except Exception as e:
            logger.error("Error creating rider", exc_info=True)
            return {"error": "Failed to create rider", "message": str(e)}, 500
    
    @app.put("/api/v1/riders/<rider_id>/location")
    @tracer.capture_method
    def update_rider_location(rider_id: str):
        """Update rider location"""
        try:
            body = app.current_event.json_body
            lat = body.get('lat')
            lng = body.get('lng')
            speed = body.get('speed', 0.0)
            heading = body.get('heading', 0.0)
            
            if lat is None or lng is None:
                return {"error": "lat and lng are required"}, 400
            
            logger.info(f"Updating rider location: {rider_id}")
            
            updated_rider = RiderService.update_location(
                rider_id,
                float(lat),
                float(lng),
                float(speed),
                float(heading)
            )
            metrics.add_metric(name="RiderLocationUpdated", unit="Count", value=1)
            
            return updated_rider.to_dict(), 200
        except Exception as e:
            logger.error("Error updating rider location", exc_info=True)
            return {"error": "Failed to update rider location", "message": str(e)}, 500
    
    @app.put("/api/v1/riders/<rider_id>/status")
    @tracer.capture_method
    def update_rider_status(rider_id: str):
        """Toggle rider online/offline status with optional location"""
        try:
            body = app.current_event.json_body
            is_active = body.get('isActive')
            lat = body.get('lat')
            lng = body.get('lng')
            
            if is_active is None:
                return {"error": "isActive is required"}, 400
            
            logger.info(f"Updating rider status: {rider_id}, isActive: {is_active}, lat: {lat}, lng: {lng}")
            
            # Update status with location if going online
            updated_rider = RiderService.set_active_status(
                rider_id, 
                bool(is_active),
                lat=float(lat) if lat is not None else None,
                lng=float(lng) if lng is not None else None
            )
            metrics.add_metric(name="RiderStatusUpdated", unit="Count", value=1)
            
            response = {
                "riderId": updated_rider.rider_id,
                "isActive": updated_rider.is_active,
                "lastSeen": updated_rider.last_seen,
                "message": f"Rider is now {'online' if is_active else 'offline'}"
            }
            
            # Include geohash if available
            if updated_rider.geohash:
                response["geohash"] = updated_rider.geohash
                logger.info(f"Rider geohash updated: {updated_rider.geohash}")
            
            return response, 200
        except Exception as e:
            logger.error("Error updating rider status", exc_info=True)
            return {"error": "Failed to update rider status", "message": str(e)}, 500

