"""Restaurant model"""
from typing import Optional, List, Union
from utils.geohash import encode as geohash_encode
from utils.datetime_ist import now_ist_iso
from utils.dynamodb_helpers import python_to_dynamodb, dynamodb_to_python


class Restaurant:
    """Restaurant model"""

    @staticmethod
    def _normalize_image_list(value: Optional[Union[str, List[str]]]) -> List[str]:
        """Normalize image field to a list for backward compatibility."""
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value if v is not None and str(v).strip()]
        if isinstance(value, str) and value.strip():
            return [value]
        return []
    
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
        rated_count: int = 0,
        owner_id: Optional[str] = None,
        restaurant_image: Optional[Union[str, List[str]]] = None,
        geohash: Optional[str] = None,
        created_at: Optional[str] = None,
        closes_at: Optional[str] = None,
        opens_at: Optional[str] = None,
        avg_preparation_time: Optional[int] = None,
        fcm_token: Optional[str] = None,
        fcm_token_updated_at: Optional[str] = None,
        position: Optional[int] = None,
        top_offer_banner: Optional[str] = None,
        shift_timings: Optional[List[dict]] = None,
        timezone: Optional[str] = None
    ):
        self.location_id = location_id
        self.restaurant_id = restaurant_id
        self.name = name
        self.latitude = latitude
        self.longitude = longitude
        self.is_open = is_open
        self.cuisine = cuisine or []
        self.rating = rating
        self.rated_count = rated_count or 0
        self.owner_id = owner_id
        self.restaurant_image = self._normalize_image_list(restaurant_image)
        self.geohash = geohash or geohash_encode(latitude, longitude, 7)  # Auto-generate if not provided
        self.geohash_6 = self.geohash[:6]
        self.geohash_5 = self.geohash[:5]
        self.geohash_4 = self.geohash[:4]
        self.created_at = created_at or now_ist_iso()
        self.closes_at = closes_at
        self.opens_at = opens_at
        self.avg_preparation_time = avg_preparation_time
        self.fcm_token = fcm_token
        self.fcm_token_updated_at = fcm_token_updated_at
        self.position = position
        self.top_offer_banner = top_offer_banner
        self.shift_timings = shift_timings or []
        self.timezone = timezone or "Asia/Kolkata"
    
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
        result["ratedCount"] = self.rated_count
        if self.owner_id:
            result["ownerId"] = self.owner_id
        if self.restaurant_image:
            result["restaurantImage"] = self.restaurant_image
        if self.created_at:
            result["createdAt"] = self.created_at
        if self.closes_at:
            result["closesAt"] = self.closes_at
        if self.opens_at:
            result["opensAt"] = self.opens_at
        if self.avg_preparation_time is not None:
            result["avgPreparationTime"] = self.avg_preparation_time
        if self.position is not None:
            result["position"] = self.position
        if self.top_offer_banner:
            result["topOfferBanner"] = self.top_offer_banner
        if self.shift_timings:
            result["shiftTimings"] = self.shift_timings
        result["timezone"] = self.timezone
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

        rated_count = 0
        if "ratedCount" in item and "N" in item["ratedCount"]:
            rated_count = int(float(item["ratedCount"]["N"]))
        
        restaurant_image = None
        if "restaurant_image" in item:
            restaurant_image_attr = item.get("restaurant_image", {})
            if "L" in restaurant_image_attr:
                restaurant_image = [img.get("S", "") for img in restaurant_image_attr["L"] if img.get("S")]
            elif "S" in restaurant_image_attr:
                restaurant_image = restaurant_image_attr.get("S")

        position = None
        if "position" in item and "N" in item["position"]:
            position = int(float(item["position"]["N"]))

        top_offer_banner = item.get("topOfferBanner", {}).get("S") if "topOfferBanner" in item else None
        avg_preparation_time = None
        if "avgPreparationTime" in item and "N" in item["avgPreparationTime"]:
            avg_preparation_time = int(float(item["avgPreparationTime"]["N"]))

        return cls(
            location_id=location_id,
            restaurant_id=restaurant_id,
            name=item.get("name", {}).get("S", ""),
            latitude=float(item.get("latitude", {}).get("N", "0")),
            longitude=float(item.get("longitude", {}).get("N", "0")),
            is_open=item.get("isOpen", {}).get("BOOL", True) if "isOpen" in item else True,
            cuisine=cuisine_list,
            rating=rating,
            rated_count=rated_count,
            owner_id=owner_id,
            restaurant_image=restaurant_image,
            geohash=geohash,
            created_at=item.get("createdAt", {}).get("S") if "createdAt" in item else None,
            closes_at=item.get("closesAt", {}).get("S") if "closesAt" in item else None,
            opens_at=item.get("opensAt", {}).get("S") if "opensAt" in item else None,
            avg_preparation_time=avg_preparation_time,
            fcm_token=item.get("fcmToken", {}).get("S") if "fcmToken" in item else None,
            fcm_token_updated_at=item.get("fcmTokenUpdatedAt", {}).get("S") if "fcmTokenUpdatedAt" in item else None,
            position=position,
            top_offer_banner=top_offer_banner,
            shift_timings=dynamodb_to_python(item["shiftTimings"]) if "shiftTimings" in item else [],
            timezone=item.get("timezone", {}).get("S", "Asia/Kolkata") if "timezone" in item else "Asia/Kolkata"
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

        item["ratedCount"] = {"N": str(self.rated_count)}
        
        if self.restaurant_image:
            item["restaurant_image"] = {"L": [{"S": img} for img in self.restaurant_image]}
        
        if self.created_at:
            item["createdAt"] = {"S": self.created_at}
        if self.closes_at:
            item["closesAt"] = {"S": self.closes_at}
        if self.opens_at:
            item["opensAt"] = {"S": self.opens_at}
        if self.avg_preparation_time is not None:
            item["avgPreparationTime"] = {"N": str(int(self.avg_preparation_time))}
        if self.fcm_token:
            item["fcmToken"] = {"S": self.fcm_token}
        if self.fcm_token_updated_at:
            item["fcmTokenUpdatedAt"] = {"S": self.fcm_token_updated_at}
        if self.position is not None:
            item["position"] = {"N": str(self.position)}
        if self.top_offer_banner:
            item["topOfferBanner"] = {"S": self.top_offer_banner}
        if self.shift_timings:
            item["shiftTimings"] = python_to_dynamodb(self.shift_timings)
        item["timezone"] = {"S": self.timezone}

        return item
