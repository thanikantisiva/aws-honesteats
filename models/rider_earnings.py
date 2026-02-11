"""Rider earnings model"""
from typing import Optional
from datetime import datetime


class RiderEarnings:
    """Rider earnings tracking model"""
    
    def __init__(
        self,
        rider_id: str,
        date: str,  # YYYY-MM-DD#ORDER_ID format
        total_deliveries: int = 0,
        total_earnings: float = 0.0,
        delivery_fees: float = 0.0,
        tips: float = 0.0,
        incentives: float = 0.0,
        online_time_minutes: int = 0,
        order_id: Optional[str] = None,
        settlement_id: Optional[str] = None,
        settled: bool = False,
        settled_at: Optional[str] = None,
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
        self.order_id = order_id
        self.settlement_id = settlement_id
        self.settled = settled
        self.settled_at = settled_at
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
            "orderId": self.order_id,
            "settlementId": self.settlement_id,
            "settled": self.settled,
            "settledAt": self.settled_at,
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
            order_id=item.get("orderId", {}).get("S") if "orderId" in item else None,
            settlement_id=item.get("settlementId", {}).get("S") if "settlementId" in item else None,
            settled=item.get("settled", {}).get("BOOL", False) if "settled" in item else False,
            settled_at=item.get("settledAt", {}).get("S") if "settledAt" in item else None,
            created_at=item.get("createdAt", {}).get("S", "")
        )
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format"""
        item = {
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
        if self.order_id:
            item["orderId"] = {"S": self.order_id}
        if self.settlement_id:
            item["settlementId"] = {"S": self.settlement_id}
        item["settled"] = {"BOOL": self.settled}
        if self.settled_at:
            item["settledAt"] = {"S": self.settled_at}
        return item
