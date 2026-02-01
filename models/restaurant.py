"""Restaurant model"""
from typing import Optional, List
from datetime import datetime
from utils.geohash import encode as geohash_encode


class Restaurant:
    """Restaurant model"""
    
    def __init__(
        self,
        location_id: str,
        restaurant_id: str,
        name: str,
        latitude: float,
        longitude: float,
        is_open: bool = True,
        cuisine: Optional[List[str]] = None,
        rating: Optional[float] = None,
        owner_id: Optional[str] = None,
        restaurant_image: Optional[str] = None,
        geohash: Optional[str] = None,
        created_at: Optional[str] = None
    ):
        self.location_id = location_id
        self.restaurant_id = restaurant_id
        self.name = name
        self.latitude = latitude
        self.longitude = longitude
        self.is_open = is_open
        self.cuisine = cuisine or []
        self.rating = rating
        self.owner_id = owner_id
        self.restaurant_image = restaurant_image
        self.geohash = geohash or geohash_encode(latitude, longitude, 7)  # Auto-generate if not provided
        self.geohash_6 = self.geohash[:6]
        self.geohash_5 = self.geohash[:5]
        self.geohash_4 = self.geohash[:4]
        self.created_at = created_at or datetime.utcnow().isoformat()
    
    @property
    def pk(self) -> str:
        """Get partition key - uses geohash precision 7 for spatial indexing"""
        return self.geohash
    
    @property
    def sk(self) -> str:
        """Get sort key"""
        return f"RESTAURANT#{self.restaurant_id}"
    
    @property
    def gsi1pk(self) -> str:
        """Get GSI1 partition key - geohash precision 6"""
        return self.geohash_6
    
    @property
    def gsi1sk(self) -> str:
        """Get GSI1 sort key"""
        return f"RESTAURANT#{self.restaurant_id}"
    
    @property
    def gsi2pk(self) -> str:
        """Get GSI2 partition key - geohash precision 5"""
        return self.geohash_5
    
    @property
    def gsi2sk(self) -> str:
        """Get GSI2 sort key"""
        return f"RESTAURANT#{self.restaurant_id}"
    
    @property
    def gsi3pk(self) -> str:
        """Get GSI3 partition key - geohash precision 4"""
        return self.geohash_4
    
    @property
    def gsi3sk(self) -> str:
        """Get GSI3 sort key"""
        return f"RESTAURANT#{self.restaurant_id}"
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        result = {
            "restaurantId": self.restaurant_id,
            "name": self.name,
            "locationId": self.location_id,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "isOpen": self.is_open,
            "cuisine": self.cuisine,
            "geohash": self.geohash
        }
        if self.rating is not None:
            result["rating"] = self.rating
        if self.owner_id:
            result["ownerId"] = self.owner_id
        if self.restaurant_image:
            result["restaurantImage"] = self.restaurant_image
        if self.created_at:
            result["createdAt"] = self.created_at
        return result
    
    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "Restaurant":
        """Create Restaurant from DynamoDB item"""
        # Extract geohash and restaurantId from PK and SK
        pk = item.get("PK", {}).get("S", "")
        sk = item.get("SK", {}).get("S", "")
        
        geohash = item.get("geohash", {}).get("S", pk)  # Use stored geohash field
        restaurant_id = sk.replace("RESTAURANT#", "") if sk.startswith("RESTAURANT#") else ""
        location_id = item.get("locationId", {}).get("S", "")
        
        # Handle cuisine as list of strings
        cuisine_list = []
        if "cuisine" in item:
            cuisine_attr = item["cuisine"]
            if "L" in cuisine_attr:  # DynamoDB List type
                cuisine_list = [c.get("S", "") for c in cuisine_attr["L"]]
            elif "SS" in cuisine_attr:  # DynamoDB String Set type
                cuisine_list = list(cuisine_attr["SS"])
        
        # Extract ownerId from GSI1PK if present
        owner_id = None
        if "GSI1PK" in item:
            gsi1pk = item.get("GSI1PK", {}).get("S", "")
            if gsi1pk.startswith("OWNER#"):
                owner_id = gsi1pk.replace("OWNER#", "")
        
        rating = None
        if "rating" in item:
            rating_attr = item["rating"]
            if "N" in rating_attr:
                rating = float(rating_attr["N"])
        
        return cls(
            location_id=location_id,
            restaurant_id=restaurant_id,
            name=item.get("name", {}).get("S", ""),
            latitude=float(item.get("latitude", {}).get("N", "0")),
            longitude=float(item.get("longitude", {}).get("N", "0")),
            is_open=item.get("isOpen", {}).get("BOOL", True) if "isOpen" in item else True,
            cuisine=cuisine_list,
            rating=rating,
            owner_id=owner_id,
            restaurant_image=item.get("restaurant_image", {}).get("S") if "restaurant_image" in item else None,
            geohash=geohash,
            created_at=item.get("createdAt", {}).get("S") if "createdAt" in item else None
        )
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format"""
        item = {
            "PK": {"S": self.pk},
            "SK": {"S": self.sk},
            "restaurantId": {"S": self.restaurant_id},
            "name": {"S": self.name},
            "locationId": {"S": self.location_id},
            "latitude": {"N": str(self.latitude)},
            "longitude": {"N": str(self.longitude)},
            "isOpen": {"BOOL": self.is_open},
            "cuisine": {"L": [{"S": c} for c in self.cuisine]} if self.cuisine else {"L": []},
            "geohash": {"S": self.geohash},
            "GSI1PK": {"S": self.gsi1pk},
            "GSI1SK": {"S": self.gsi1sk},
            "GSI2PK": {"S": self.gsi2pk},
            "GSI2SK": {"S": self.gsi2sk},
            "GSI3PK": {"S": self.gsi3pk},
            "GSI3SK": {"S": self.gsi3sk}
        }
        
        if self.rating is not None:
            item["rating"] = {"N": str(self.rating)}
        
        if self.restaurant_image:
            item["restaurant_image"] = {"S": self.restaurant_image}
        
        if self.created_at:
            item["createdAt"] = {"S": self.created_at}
        
        return item
