"""Rider earnings model"""
from typing import Optional
from datetime import datetime


class RiderEarnings:
    """Rider earnings tracking model"""
    
    def __init__(
        self,
        rider_id: str,
        date: str,  # YYYY-MM-DD format
        total_deliveries: int = 0,
        total_earnings: float = 0.0,
        delivery_fees: float = 0.0,
        tips: float = 0.0,
        incentives: float = 0.0,
        online_time_minutes: int = 0,
        created_at: Optional[str] = None
    ):
        self.rider_id = rider_id
        self.date = date
        self.total_deliveries = total_deliveries
        self.total_earnings = total_earnings
        self.delivery_fees = delivery_fees
        self.tips = tips
        self.incentives = incentives
        self.online_time_minutes = online_time_minutes
        self.created_at = created_at or datetime.utcnow().isoformat()
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            "riderId": self.rider_id,
            "date": self.date,
            "totalDeliveries": self.total_deliveries,
            "totalEarnings": self.total_earnings,
            "deliveryFees": self.delivery_fees,
            "tips": self.tips,
            "incentives": self.incentives,
            "onlineTimeMinutes": self.online_time_minutes,
            "createdAt": self.created_at
        }
    
    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "RiderEarnings":
        """Create RiderEarnings from DynamoDB item"""
        return cls(
            rider_id=item.get("riderId", {}).get("S", ""),
            date=item.get("date", {}).get("S", ""),
            total_deliveries=int(item.get("totalDeliveries", {}).get("N", "0")),
            total_earnings=float(item.get("totalEarnings", {}).get("N", "0")),
            delivery_fees=float(item.get("deliveryFees", {}).get("N", "0")),
            tips=float(item.get("tips", {}).get("N", "0")),
            incentives=float(item.get("incentives", {}).get("N", "0")),
            online_time_minutes=int(item.get("onlineTimeMinutes", {}).get("N", "0")),
            created_at=item.get("createdAt", {}).get("S", "")
        )
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format"""
        return {
            "riderId": {"S": self.rider_id},
            "date": {"S": self.date},
            "totalDeliveries": {"N": str(self.total_deliveries)},
            "totalEarnings": {"N": str(self.total_earnings)},
            "deliveryFees": {"N": str(self.delivery_fees)},
            "tips": {"N": str(self.tips)},
            "incentives": {"N": str(self.incentives)},
            "onlineTimeMinutes": {"N": str(self.online_time_minutes)},
            "createdAt": {"S": self.created_at}
        }
