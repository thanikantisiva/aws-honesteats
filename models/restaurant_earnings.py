"""Restaurant earnings model"""
from typing import Optional
from datetime import datetime


class RestaurantEarnings:
    """Restaurant earnings tracking model"""

    def __init__(
        self,
        restaurant_id: str,
        date: str,  # YYYY-MM-DD#ORDER_ID format
        total_orders: int = 0,
        total_earnings: float = 0.0,
        settled: bool = False,
        settled_at: Optional[str] = None,
        settlement_id: Optional[str] = None,
        order_id: Optional[str] = None,
        created_at: Optional[str] = None
    ):
        self.restaurant_id = restaurant_id
        self.date = date
        self.total_orders = total_orders
        self.total_earnings = total_earnings
        self.settled = settled
        self.settled_at = settled_at
        self.settlement_id = settlement_id
        self.order_id = order_id
        self.created_at = created_at or datetime.utcnow().isoformat()

    def to_dict(self) -> dict:
        return {
            "restaurantId": self.restaurant_id,
            "date": self.date,
            "totalOrders": self.total_orders,
            "totalEarnings": self.total_earnings,
            "orderId": self.order_id,
            "settled": self.settled,
            "settledAt": self.settled_at,
            "settlementId": self.settlement_id,
            "createdAt": self.created_at
        }

    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "RestaurantEarnings":
        return cls(
            restaurant_id=item.get("restaurantId", {}).get("S", ""),
            date=item.get("date", {}).get("S", ""),
            total_orders=int(item.get("totalOrders", {}).get("N", "0")),
            total_earnings=float(item.get("totalEarnings", {}).get("N", "0")),
            order_id=item.get("orderId", {}).get("S") if "orderId" in item else None,
            settled=item.get("settled", {}).get("BOOL", False) if "settled" in item else False,
            settled_at=item.get("settledAt", {}).get("S") if "settledAt" in item else None,
            settlement_id=item.get("settlementId", {}).get("S") if "settlementId" in item else None,
            created_at=item.get("createdAt", {}).get("S", "")
        )

    def to_dynamodb_item(self) -> dict:
        item = {
            "restaurantId": {"S": self.restaurant_id},
            "date": {"S": self.date},
            "totalOrders": {"N": str(self.total_orders)},
            "totalEarnings": {"N": str(self.total_earnings)},
            "createdAt": {"S": self.created_at},
            "settled": {"BOOL": self.settled}
        }
        if self.order_id:
            item["orderId"] = {"S": self.order_id}
        if self.settled_at:
            item["settledAt"] = {"S": self.settled_at}
        if self.settlement_id:
            item["settlementId"] = {"S": self.settlement_id}
        return item
