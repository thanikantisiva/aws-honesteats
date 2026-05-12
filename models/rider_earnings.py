"""Rider earnings model"""
from typing import Optional
from utils.datetime_ist import now_ist_iso


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
        delivery_duration_minutes: int = 0,
        order_id: Optional[str] = None,
        settlement_id: Optional[str] = None,
        settled: bool = False,
        settled_at: Optional[str] = None,
        created_at: Optional[str] = None,
        entry_type: Optional[str] = None,
        bonus_type: Optional[str] = None,
        milestone_stops: Optional[int] = None,
        campaign_start_date: Optional[str] = None,
        campaign_end_date: Optional[str] = None,
        bonus_label: Optional[str] = None,
    ):
        self.rider_id = rider_id
        self.date = date
        self.total_deliveries = total_deliveries
        self.total_earnings = total_earnings
        self.delivery_fees = delivery_fees
        self.tips = tips
        self.incentives = incentives
        self.online_time_minutes = online_time_minutes
        self.delivery_duration_minutes = delivery_duration_minutes
        self.order_id = order_id
        self.settlement_id = settlement_id
        self.settled = settled
        self.settled_at = settled_at
        self.created_at = created_at or now_ist_iso()
        self.entry_type = entry_type
        self.bonus_type = bonus_type
        self.milestone_stops = milestone_stops
        self.campaign_start_date = campaign_start_date
        self.campaign_end_date = campaign_end_date
        self.bonus_label = bonus_label
    
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
            "deliveryDurationMinutes": self.delivery_duration_minutes,
            "orderId": self.order_id,
            "settlementId": self.settlement_id,
            "settled": self.settled,
            "settledAt": self.settled_at,
            "createdAt": self.created_at,
            "entryType": self.entry_type,
            "bonusType": self.bonus_type,
            "milestoneStops": self.milestone_stops,
            "campaignStartDate": self.campaign_start_date,
            "campaignEndDate": self.campaign_end_date,
            "bonusLabel": self.bonus_label,
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
            delivery_duration_minutes=int(item.get("deliveryDurationMinutes", {}).get("N", "0")),
            order_id=item.get("orderId", {}).get("S") if "orderId" in item else None,
            settlement_id=item.get("settlementId", {}).get("S") if "settlementId" in item else None,
            settled=item.get("settled", {}).get("BOOL", False) if "settled" in item else False,
            settled_at=item.get("settledAt", {}).get("S") if "settledAt" in item else None,
            created_at=item.get("createdAt", {}).get("S", ""),
            entry_type=item.get("entryType", {}).get("S") if "entryType" in item else None,
            bonus_type=item.get("bonusType", {}).get("S") if "bonusType" in item else None,
            milestone_stops=int(item.get("milestoneStops", {}).get("N", "0")) if "milestoneStops" in item else None,
            campaign_start_date=item.get("campaignStartDate", {}).get("S") if "campaignStartDate" in item else None,
            campaign_end_date=item.get("campaignEndDate", {}).get("S") if "campaignEndDate" in item else None,
            bonus_label=item.get("bonusLabel", {}).get("S") if "bonusLabel" in item else None,
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
            "deliveryDurationMinutes": {"N": str(self.delivery_duration_minutes)},
            "createdAt": {"S": self.created_at}
        }
        if self.order_id:
            item["orderId"] = {"S": self.order_id}
        if self.settlement_id:
            item["settlementId"] = {"S": self.settlement_id}
        if self.entry_type:
            item["entryType"] = {"S": self.entry_type}
        if self.bonus_type:
            item["bonusType"] = {"S": self.bonus_type}
        if self.milestone_stops is not None:
            item["milestoneStops"] = {"N": str(self.milestone_stops)}
        if self.campaign_start_date:
            item["campaignStartDate"] = {"S": self.campaign_start_date}
        if self.campaign_end_date:
            item["campaignEndDate"] = {"S": self.campaign_end_date}
        if self.bonus_label:
            item["bonusLabel"] = {"S": self.bonus_label}
        item["settled"] = {"BOOL": self.settled}
        if self.settled_at:
            item["settledAt"] = {"S": self.settled_at}
        return item
