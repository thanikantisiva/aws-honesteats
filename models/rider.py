"""Rider operational model for real-time tracking"""
from typing import Optional, List
from utils.geohash import encode as geohash_encode
from utils.datetime_ist import now_ist_iso


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
        rating: Optional[float] = None,
        rated_count: int = 0,
        working_on_order: Optional[List[str]] = None,
        last_seen: Optional[str] = None,
        geohash: Optional[str] = None,
        orders_assigned_last_7d: int = 0,
        assignment_window_start: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
    ):
        self.rider_id = rider_id
        self.phone = phone
        self.lat = lat
        self.lng = lng
        self.speed = speed  # km/h
        self.heading = heading  # degrees (0-360)
        self.timestamp = timestamp or now_ist_iso()
        self.is_active = is_active
        self.rating = rating
        self.rated_count = rated_count or 0
        self.working_on_order = working_on_order or []
        self.last_seen = last_seen or now_ist_iso()
        self.orders_assigned_last_7d = orders_assigned_last_7d or 0
        self.assignment_window_start = assignment_window_start
        self.first_name = (first_name or "").strip() or None
        self.last_name = (last_name or "").strip() or None
        
        # Auto-generate geohash if lat/lng provided
        if geohash:
            self.geohash = geohash
        elif lat is not None and lng is not None:
            self.geohash = geohash_encode(lat, lng, 7)
        else:
            self.geohash = None
            
        # Multi-precision geohash. Only the 2-char prefix is indexed (GSI3) for the
        # rider-assignment query. The longer prefixes are derived attributes kept on
        # the Rider object for logging / future use, not written to DynamoDB.
        if self.geohash:
            self.geohash_4 = self.geohash[:4]  # ~20km cells (kept for legacy callers)
            self.geohash_2 = self.geohash[:2]  # ~1250km cells (GSI3 partition)
        else:
            self.geohash_4 = None
            self.geohash_2 = None
    
    @property
    def gsi3pk(self) -> Optional[str]:
        """Get GSI3 partition key - geohash precision 2.

        Acts as a single-partition index of all riders within a deployment region,
        so the rider-assignment path can use Query (not Scan) to fan out candidates.
        """
        return self.geohash_2

    @property
    def gsi3sk(self) -> str:
        """Get GSI3 sort key"""
        return f"RIDER#{self.rider_id}"
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        result = {
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
            "geohash": self.geohash,
            "ratedCount": self.rated_count
        }
        if self.rating is not None:
            result["rating"] = self.rating
        if self.orders_assigned_last_7d:
            result["ordersAssignedLast7d"] = self.orders_assigned_last_7d
        if self.assignment_window_start:
            result["assignmentWindowStart"] = self.assignment_window_start
        if self.first_name:
            result["firstName"] = self.first_name
        if self.last_name:
            result["lastName"] = self.last_name
        return result
    
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
            rating=float(item.get("rating", {}).get("N")) if "rating" in item else None,
            rated_count=int(float(item.get("ratedCount", {}).get("N", "0"))) if "ratedCount" in item else 0,
            working_on_order=(
                [v.get("S", "") for v in item.get("workingOnOrder", {}).get("L", [])]
                if "workingOnOrder" in item
                else []
            ),
            last_seen=item.get("lastSeen", {}).get("S", ""),
            geohash=item.get("geohash", {}).get("S") if "geohash" in item else None,
            orders_assigned_last_7d=int(float(item.get("ordersAssignedLast7d", {}).get("N", "0"))) if "ordersAssignedLast7d" in item else 0,
            assignment_window_start=item.get("assignmentWindowStart", {}).get("S") if "assignmentWindowStart" in item else None,
            first_name=item.get("firstName", {}).get("S") if "firstName" in item else None,
            last_name=item.get("lastName", {}).get("S") if "lastName" in item else None,
        )
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format"""
        item = {
            "riderId": {"S": self.rider_id},
            "phone": {"S": self.phone},
            "timestamp": {"S": self.timestamp},
            "isActive": {"BOOL": self.is_active},
            "lastSeen": {"S": self.last_seen},
            "ratedCount": {"N": str(self.rated_count)}
        }
        
        if self.lat is not None:
            item["lat"] = {"N": str(self.lat)}
        if self.lng is not None:
            item["lng"] = {"N": str(self.lng)}
        if self.speed is not None:
            item["speed"] = {"N": str(self.speed)}
        if self.heading is not None:
            item["heading"] = {"N": str(self.heading)}
        if self.rating is not None:
            item["rating"] = {"N": str(self.rating)}
        if self.working_on_order:
            item["workingOnOrder"] = {"L": [{"S": str(v)} for v in self.working_on_order]}
        if self.orders_assigned_last_7d:
            item["ordersAssignedLast7d"] = {"N": str(self.orders_assigned_last_7d)}
        if self.assignment_window_start:
            item["assignmentWindowStart"] = {"S": self.assignment_window_start}
        if self.first_name:
            item["firstName"] = {"S": self.first_name}
        if self.last_name:
            item["lastName"] = {"S": self.last_name}
        
        # Add geohash fields for spatial indexing (only GSI3 is indexed)
        if self.geohash:
            item["geohash"] = {"S": self.geohash}
            item["GSI3PK"] = {"S": self.gsi3pk}  # Precision 2
            item["GSI3SK"] = {"S": self.gsi3sk}

        return item
