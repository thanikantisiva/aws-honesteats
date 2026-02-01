"""Address routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.address_service import AddressService
from models.address import Address
from utils.dynamodb import generate_id

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_address_routes(app):
    """Register address routes"""
    
    @app.get("/api/v1/users/<phone>/addresses")
    @tracer.capture_method
    def list_addresses(phone: str):
        """List all addresses for a customer"""
        try:
            logger.info(f"Listing addresses for user: {phone}")
            addresses = AddressService.list_addresses(phone)
            metrics.add_metric(name="AddressesListed", unit="Count", value=1)
            
            return {
                "phone": phone,
                "addresses": [a.to_dict() for a in addresses],
                "total": len(addresses)
            }, 200
        except Exception as e:
            logger.error("Error listing addresses", exc_info=True)
            return {"error": "Failed to list addresses", "message": str(e)}, 500
    
    @app.get("/api/v1/users/<phone>/addresses/<address_id>")
    @tracer.capture_method
    def get_address(phone: str, address_id: str):
        """Get address by ID"""
        try:
            logger.info(f"Getting address: {address_id} for user: {phone}")
            address = AddressService.get_address(phone, address_id)
            
            if not address:
                return {"error": "Address not found"}, 404
            
            metrics.add_metric(name="AddressRetrieved", unit="Count", value=1)
            return address.to_dict(), 200
        except Exception as e:
            logger.error("Error getting address", exc_info=True)
            return {"error": "Failed to get address", "message": str(e)}, 500
    
    @app.post("/api/v1/users/<phone>/addresses")
    @tracer.capture_method
    def create_address(phone: str):
        """Create a new address"""
        try:
            body = app.current_event.json_body
            label = body.get('label')
            address_text = body.get('address')
            lat = body.get('lat', 0.0)
            lng = body.get('lng', 0.0)
            geocoded_address = body.get('geocodedAddress')
            formatted_address = body.get('formattedAddress')
            place_id = body.get('placeId')
            components = body.get('components')
            
            if not all([label, address_text, lat is not None, lng is not None]):
                return {"error": "label, address, lat, and lng are required"}, 400
            
            logger.info(f"Creating address for user: {phone}, data received: geocoded={geocoded_address}, formatted={formatted_address}, place_id={place_id}, components={components}")
            
            address = Address(
                phone=phone,
                address_id=generate_id('ADD'),
                label=label,
                address=address_text,
                lat=float(lat),
                lng=float(lng),
                geocoded_address=geocoded_address,
                formatted_address=formatted_address,
                place_id=place_id,
                components=components
            )
            
            logger.info(f"Address object created, to_dict: {address.to_dict()}")
            
            created_address = AddressService.create_address(address)
            metrics.add_metric(name="AddressCreated", unit="Count", value=1)
            
            return created_address.to_dict(), 201
        except Exception as e:
            logger.error("Error creating address", exc_info=True)
            return {"error": "Failed to create address", "message": str(e)}, 500
    
    @app.put("/api/v1/users/<phone>/addresses/<address_id>")
    @tracer.capture_method
    def update_address(phone: str, address_id: str):
        """Update address"""
        try:
            body = app.current_event.json_body
            updates = {}
            
            if 'label' in body:
                updates['label'] = body['label']
            if 'address' in body:
                updates['address'] = body['address']
            if 'lat' in body:
                updates['lat'] = float(body['lat'])
            if 'lng' in body:
                updates['lng'] = float(body['lng'])
            
            if 'geocodedAddress' in body:
                updates['geocodedAddress'] = body['geocodedAddress']
            
            if 'formattedAddress' in body:
                updates['formattedAddress'] = body['formattedAddress']
            
            if 'placeId' in body:
                updates['placeId'] = body['placeId']
            
            if 'components' in body:
                updates['components'] = body['components']
            
            if not updates:
                return {"error": "No fields to update"}, 400
            
            logger.info(f"Updating address: {address_id} for user: {phone}")
            
            updated_address = AddressService.update_address(phone, address_id, updates)
            metrics.add_metric(name="AddressUpdated", unit="Count", value=1)
            
            return updated_address.to_dict(), 200
        except Exception as e:
            logger.error("Error updating address", exc_info=True)
            return {"error": "Failed to update address", "message": str(e)}, 500
    
    @app.delete("/api/v1/users/<phone>/addresses/<address_id>")
    @tracer.capture_method
    def delete_address(phone: str, address_id: str):
        """Delete an address"""
        try:
            logger.info(f"Deleting address: {address_id} for user: {phone}")
            AddressService.delete_address(phone, address_id)
            metrics.add_metric(name="AddressDeleted", unit="Count", value=1)
            
            return {"message": "Address deleted successfully"}, 200
        except Exception as e:
            logger.error("Error deleting address", exc_info=True)
            return {"error": "Failed to delete address", "message": str(e)}, 500

