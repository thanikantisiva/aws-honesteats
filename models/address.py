"""Address model"""
from typing import Optional
from utils.geohash import encode as geohash_encode


class Address:
    """Customer address model"""
    
    def __init__(
        self,
        phone: str,
        address_id: str,
        label: str,
        address: str,
        lat: float,
        lng: float,
        geocoded_address: Optional[str] = None,
        formatted_address: Optional[str] = None,
        place_id: Optional[str] = None,
        components: Optional[dict] = None,
        geohash: Optional[str] = None
    ):
        self.phone = phone
        self.address_id = address_id
        self.label = label
        self.address = address  # User-entered address details
        self.lat = lat
        self.lng = lng
        self.geocoded_address = geocoded_address  # Short address from Google Maps
        self.formatted_address = formatted_address  # Full detailed address from Google Maps
        self.place_id = place_id  # Google Maps place ID
        self.components = components or {}  # Address components (road, city, state, etc.)
        self.geohash = geohash_encode(lat, lng, 7)  # Auto-generate if not provided
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        result = {
            "phone": self.phone,
            "addressId": self.address_id,
            "label": self.label,
            "address": self.address,
            "lat": self.lat,
            "lng": self.lng,
            "geohash": self.geohash
        }
        if self.geocoded_address:
            result["geocodedAddress"] = self.geocoded_address
        if self.formatted_address:
            result["formattedAddress"] = self.formatted_address
        if self.place_id:
            result["placeId"] = self.place_id
        if self.components:
            result["components"] = self.components
        return result
    
    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "Address":
        """Create Address from DynamoDB item"""
        # Parse components from JSON string if present
        components = None
        if "components" in item:
            import json
            try:
                components = json.loads(item["components"].get("S", "{}"))
            except:
                components = {}
        
        return cls(
            phone=item.get("phone", {}).get("S", ""),
            address_id=item.get("addressId", {}).get("S", ""),
            label=item.get("label", {}).get("S", ""),
            address=item.get("address", {}).get("S", ""),
            lat=float(item.get("lat", {}).get("N", "0")),
            lng=float(item.get("lng", {}).get("N", "0")),
            geocoded_address=item.get("geocodedAddress", {}).get("S") if "geocodedAddress" in item else None,
            formatted_address=item.get("formattedAddress", {}).get("S") if "formattedAddress" in item else None,
            place_id=item.get("placeId", {}).get("S") if "placeId" in item else None,
            components=components,
            geohash=item.get("geohash", {}).get("S") if "geohash" in item else None
        )
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format"""
        import json
        
        item = {
            "phone": {"S": self.phone},
            "addressId": {"S": self.address_id},
            "label": {"S": self.label},
            "address": {"S": self.address},
            "lat": {"N": str(self.lat)},
            "lng": {"N": str(self.lng)},
            "geohash": {"S": self.geohash}
        }
        if self.geocoded_address:
            item["geocodedAddress"] = {"S": self.geocoded_address}
        if self.formatted_address:
            item["formattedAddress"] = {"S": self.formatted_address}
        if self.place_id:
            item["placeId"] = {"S": self.place_id}
        if self.components:
            item["components"] = {"S": json.dumps(self.components)}
        return item

