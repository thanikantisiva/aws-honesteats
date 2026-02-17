"""Rider operational model for real-time tracking"""
from typing import Optional, List
from datetime import datetime
from utils.geohash import encode as geohash_encode


class Rider:
    """Rider operational model - lightweight for frequent location updates"""
    
    def __init__(
        self,
        rider_id: str,
        phone: str,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        speed: Optional[float] = 0.0,
        heading: Optional[float] = 0.0,
        timestamp: Optional[str] = None,
        is_active: bool = False,
        working_on_order: Optional[List[str]] = None,
        last_seen: Optional[str] = None,
        geohash: Optional[str] = None
    ):
        self.rider_id = rider_id
        self.phone = phone
        self.lat = lat
        self.lng = lng
        self.speed = speed  # km/h
        self.heading = heading  # degrees (0-360)
        self.timestamp = timestamp or datetime.utcnow().isoformat()
        self.is_active = is_active
        self.working_on_order = working_on_order or []
        self.last_seen = last_seen or datetime.utcnow().isoformat()
        
        # Auto-generate geohash if lat/lng provided
        if geohash:
            self.geohash = geohash
        elif lat is not None and lng is not None:
            self.geohash = geohash_encode(lat, lng, 7)
        else:
            self.geohash = None
            
        # Multi-precision geohash for different query ranges
        if self.geohash:
            self.geohash_6 = self.geohash[:6]  # ~610m cells
            self.geohash_5 = self.geohash[:5]  # ~2.4km cells
            self.geohash_4 = self.geohash[:4]  # ~20km cells
        else:
            self.geohash_6 = None
            self.geohash_5 = None
            self.geohash_4 = None
    
    @property
    def gsi1pk(self) -> Optional[str]:
        """Get GSI1 partition key - geohash precision 6"""
        return self.geohash_6
    
    @property
    def gsi1sk(self) -> str:
        """Get GSI1 sort key"""
        return f"RIDER#{self.rider_id}"
    
    @property
    def gsi2pk(self) -> Optional[str]:
        """Get GSI2 partition key - geohash precision 5"""
        return self.geohash_5
    
    @property
    def gsi2sk(self) -> str:
        """Get GSI2 sort key"""
        return f"RIDER#{self.rider_id}"
    
    @property
    def gsi3pk(self) -> Optional[str]:
        """Get GSI3 partition key - geohash precision 4"""
        return self.geohash_4
    
    @property
    def gsi3sk(self) -> str:
        """Get GSI3 sort key"""
        return f"RIDER#{self.rider_id}"
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            "riderId": self.rider_id,
            "phone": self.phone,
            "lat": self.lat,
            "lng": self.lng,
            "speed": self.speed,
            "heading": self.heading,
            "timestamp": self.timestamp,
            "isActive": self.is_active,
            "workingOnOrder": self.working_on_order,
            "lastSeen": self.last_seen,
            "geohash": self.geohash
        }
    
    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "Rider":
        """Create Rider from DynamoDB item"""
        return cls(
            rider_id=item.get("riderId", {}).get("S", ""),
            phone=item.get("phone", {}).get("S", ""),
            lat=float(item.get("lat", {}).get("N")) if "lat" in item else None,
            lng=float(item.get("lng", {}).get("N")) if "lng" in item else None,
            speed=float(item.get("speed", {}).get("N", "0")) if "speed" in item else 0.0,
            heading=float(item.get("heading", {}).get("N", "0")) if "heading" in item else 0.0,
            timestamp=item.get("timestamp", {}).get("S", ""),
            is_active=item.get("isActive", {}).get("BOOL", False),
            working_on_order=(
                [v.get("S", "") for v in item.get("workingOnOrder", {}).get("L", [])]
                if "workingOnOrder" in item
                else []
            ),
            last_seen=item.get("lastSeen", {}).get("S", ""),
            geohash=item.get("geohash", {}).get("S") if "geohash" in item else None
        )
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format"""
        item = {
            "riderId": {"S": self.rider_id},
            "phone": {"S": self.phone},
            "timestamp": {"S": self.timestamp},
            "isActive": {"BOOL": self.is_active},
            "lastSeen": {"S": self.last_seen}
        }
        
        if self.lat is not None:
            item["lat"] = {"N": str(self.lat)}
        if self.lng is not None:
            item["lng"] = {"N": str(self.lng)}
        if self.speed is not None:
            item["speed"] = {"N": str(self.speed)}
        if self.heading is not None:
            item["heading"] = {"N": str(self.heading)}
        if self.working_on_order:
            item["workingOnOrder"] = {"L": [{"S": str(v)} for v in self.working_on_order]}
        
        # Add geohash fields for spatial indexing
        if self.geohash:
            item["geohash"] = {"S": self.geohash}
            # Add GSI fields for multi-precision geohash queries
            item["GSI1PK"] = {"S": self.gsi1pk}  # Precision 6
            item["GSI1SK"] = {"S": self.gsi1sk}
            item["GSI2PK"] = {"S": self.gsi2pk}  # Precision 5
            item["GSI2SK"] = {"S": self.gsi2sk}
            item["GSI3PK"] = {"S": self.gsi3pk}  # Precision 4
            item["GSI3SK"] = {"S": self.gsi3sk}
            
        return item
