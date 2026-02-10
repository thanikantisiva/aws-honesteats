"""Order model"""
import time
from typing import List, Optional, Dict, Any
from datetime import datetime


class Order:
    """Order model"""
    
    #restaurant statuses
    STATUS_INITIATED = "INITIATED"  # Order created, payment pending
    STATUS_PENDING = "PENDING"  # Payment completed, awaiting restaurant confirmation # old flow
    STATUS_CONFIRMED = "CONFIRMED"  # Payment successful, confirmed by platform
    STATUS_ACCEPTED = "ACCEPTED"  # Accepted by restaurant

    #Order assign status
    STATUS_PREPARING = "PREPARING"   
    READY_FOR_PICKUP = "READY_FOR_PICKUP"
    STATUS_AWAITING_RIDER_ASSIGNMENT = "AWAITING_RIDER_ASSIGNMENT"
    OFFERED_TO_RIDER = "OFFERED_TO_RIDER"

    #Rider statuses
    RIDER_ASSIGNED = "RIDER_ASSIGNED"
    PICKED_UP = "PICKED_UP"
    STATUS_OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    STATUS_DELIVERED = "DELIVERED"
    STATUS_CANCELLED = "CANCELLED"
    
    def __init__(
        self,
        order_id: str,
        customer_phone: str,
        restaurant_id: str,
        items: List[Dict[str, Any]],
        food_total: float,
        delivery_fee: float,
        platform_fee: float,
        grand_total: float,
        status: str = STATUS_PENDING,
        rider_id: Optional[str] = None,
        restaurant_name: Optional[str] = None,
        restaurant_image: Optional[str] = None,
        delivery_address: Optional[str] = None,
        formatted_address: Optional[str] = None,
        address_id: Optional[str] = None,
        payment_id: Optional[str] = None,
        payment_method: Optional[str] = None,
        revenue: Optional[Dict[str, Any]] = None,
        # Rider-specific fields
        pickup_address: Optional[str] = None,
        pickup_lat: Optional[float] = None,
        pickup_lng: Optional[float] = None,
        delivery_lat: Optional[float] = None,
        delivery_lng: Optional[float] = None,
        delivery_otp: Optional[str] = None,
        pickup_otp: Optional[str] = None,
        rider_assigned_at: Optional[str] = None,
        rider_pickup_at: Optional[str] = None,
        rider_delivered_at: Optional[str] = None,
        # Real-time rider tracking during delivery
        rider_current_lat: Optional[float] = None,
        rider_current_lng: Optional[float] = None,
        rider_speed: Optional[float] = None,
        rider_heading: Optional[float] = None,
        rider_location_updated_at: Optional[str] = None,
        # Rider assignment tracking
        rider_assignment_attempts: int = 0,
        last_assignment_attempt_at: Optional[str] = None,
        # Rider offer tracking
        offered_at: Optional[str] = None,
        rejected_by_riders: Optional[List[str]] = None,
        created_at: Optional[int] = None
    ):
        self.order_id = order_id
        self.customer_phone = customer_phone
        self.restaurant_id = restaurant_id
        self.items = items
        self.food_total = food_total
        self.delivery_fee = delivery_fee
        self.platform_fee = platform_fee
        self.grand_total = grand_total
        self.status = status
        self.rider_id = rider_id
        self.restaurant_name = restaurant_name
        self.restaurant_image = restaurant_image
        self.delivery_address = delivery_address
        self.formatted_address = formatted_address
        self.address_id = address_id
        self.payment_id = payment_id
        self.payment_method = payment_method
        self.revenue = revenue
        # Rider fields
        self.pickup_address = pickup_address
        self.pickup_lat = pickup_lat
        self.pickup_lng = pickup_lng
        self.delivery_lat = delivery_lat
        self.delivery_lng = delivery_lng
        self.delivery_otp = delivery_otp
        self.pickup_otp = pickup_otp
        self.rider_assigned_at = rider_assigned_at
        self.rider_pickup_at = rider_pickup_at
        self.rider_delivered_at = rider_delivered_at
        # Real-time rider tracking
        self.rider_current_lat = rider_current_lat
        self.rider_current_lng = rider_current_lng
        self.rider_speed = rider_speed
        self.rider_heading = rider_heading
        self.rider_location_updated_at = rider_location_updated_at
        # Rider assignment tracking
        self.rider_assignment_attempts = rider_assignment_attempts
        self.last_assignment_attempt_at = last_assignment_attempt_at
        self.offered_at = offered_at
        self.rejected_by_riders = rejected_by_riders or []
        # Use timestamp in milliseconds for sorting
        self.created_at = created_at or int(time.time() * 1000)
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        result = {
            "orderId": self.order_id,
            "customerPhone": self.customer_phone,
            "restaurantId": self.restaurant_id,
            "items": self.items,
            "foodTotal": self.food_total,
            "deliveryFee": self.delivery_fee,
            "platformFee": self.platform_fee,
            "grandTotal": self.grand_total,
            "status": self.status,
            "riderId": self.rider_id,
            "createdAt": datetime.fromtimestamp(self.created_at / 1000).isoformat() if isinstance(self.created_at, int) else self.created_at
        }
        if self.restaurant_name:
            result["restaurantName"] = self.restaurant_name
        if self.restaurant_image:
            result["restaurantImage"] = self.restaurant_image
        if self.delivery_address:
            result["deliveryAddress"] = self.delivery_address
        if self.formatted_address:
            result["formattedAddress"] = self.formatted_address
        if self.address_id:
            result["addressId"] = self.address_id
        if self.payment_id:
            result["paymentId"] = self.payment_id
        if self.payment_method:
            result["paymentMethod"] = self.payment_method
        if self.revenue:
            result["revenue"] = self.revenue
        # Rider fields
        if self.pickup_address:
            result["pickupAddress"] = self.pickup_address
        if self.pickup_lat is not None:
            result["pickupLat"] = self.pickup_lat
        if self.pickup_lng is not None:
            result["pickupLng"] = self.pickup_lng
        if self.delivery_lat is not None:
            result["deliveryLat"] = self.delivery_lat
        if self.delivery_lng is not None:
            result["deliveryLng"] = self.delivery_lng
        if self.delivery_otp:
            result["deliveryOtp"] = self.delivery_otp
        if self.pickup_otp:
            result["pickupOtp"] = self.pickup_otp
        if self.rider_assigned_at:
            result["riderAssignedAt"] = self.rider_assigned_at
        if self.rider_pickup_at:
            result["riderPickupAt"] = self.rider_pickup_at
        if self.rider_delivered_at:
            result["riderDeliveredAt"] = self.rider_delivered_at
        # Real-time rider tracking
        if self.rider_current_lat is not None:
            result["riderCurrentLat"] = self.rider_current_lat
        if self.rider_current_lng is not None:
            result["riderCurrentLng"] = self.rider_current_lng
        if self.rider_speed is not None:
            result["riderSpeed"] = self.rider_speed
        if self.rider_heading is not None:
            result["riderHeading"] = self.rider_heading
        if self.rider_location_updated_at:
            result["riderLocationUpdatedAt"] = self.rider_location_updated_at
        # Rider assignment tracking
        if self.rider_assignment_attempts:
            result["riderAssignmentAttempts"] = self.rider_assignment_attempts
        if self.last_assignment_attempt_at:
            result["lastAssignmentAttemptAt"] = self.last_assignment_attempt_at
        if self.offered_at:
            result["offeredAt"] = self.offered_at
        if self.rejected_by_riders:
            result["rejectedByRiders"] = self.rejected_by_riders
        return result
    
    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "Order":
        """Create Order from DynamoDB item"""
        import json
        from utils.dynamodb_helpers import dynamodb_to_python
        
        # Handle items as List or String (for backward compatibility)
        if "items" in item:
            if "L" in item["items"]:
                items = dynamodb_to_python(item["items"])
            else:
                items = json.loads(item.get("items", {}).get("S", "[]"))
        else:
            items = []
        
        # Handle createdAt as number (timestamp in milliseconds)
        created_at = None
        if "createdAt" in item:
            if "N" in item["createdAt"]:
                created_at = int(item["createdAt"]["N"])
            elif "S" in item["createdAt"]:
                # Fallback: try to parse ISO string
                try:
                    dt = datetime.fromisoformat(item["createdAt"]["S"].replace("Z", "+00:00"))
                    created_at = int(dt.timestamp() * 1000)
                except:
                    created_at = int(time.time() * 1000)
        
        return cls(
            order_id=item.get("orderId", {}).get("S", ""),
            customer_phone=item.get("customerPhone", {}).get("S", ""),
            restaurant_id=item.get("restaurantId", {}).get("S", ""),
            items=items,
            food_total=float(item.get("foodTotal", {}).get("N", "0")),
            delivery_fee=float(item.get("deliveryFee", {}).get("N", "0")),
            platform_fee=float(item.get("platformFee", {}).get("N", "0")),
            grand_total=float(item.get("grandTotal", {}).get("N", "0")),
            status=item.get("status", {}).get("S", cls.STATUS_PENDING),
            rider_id=item.get("riderId", {}).get("S") if "riderId" in item else None,
            restaurant_name=item.get("restaurantName", {}).get("S") if "restaurantName" in item else None,
            restaurant_image=item.get("restaurantImage", {}).get("S") if "restaurantImage" in item else None,
            delivery_address=item.get("deliveryAddress", {}).get("S") if "deliveryAddress" in item else None,
            formatted_address=item.get("formattedAddress", {}).get("S") if "formattedAddress" in item else None,
            address_id=item.get("addressId", {}).get("S") if "addressId" in item else None,
            payment_id=item.get("paymentId", {}).get("S") if "paymentId" in item else None,
            payment_method=item.get("paymentMethod", {}).get("S") if "paymentMethod" in item else None,
            revenue=dynamodb_to_python(item["revenue"]) if "revenue" in item and "M" in item["revenue"] else (json.loads(item.get("revenue", {}).get("S")) if "revenue" in item else None),
            # Rider fields
            pickup_address=item.get("pickupAddress", {}).get("S") if "pickupAddress" in item else None,
            pickup_lat=float(item.get("pickupLat", {}).get("N")) if "pickupLat" in item else None,
            pickup_lng=float(item.get("pickupLng", {}).get("N")) if "pickupLng" in item else None,
            delivery_lat=float(item.get("deliveryLat", {}).get("N")) if "deliveryLat" in item else None,
            delivery_lng=float(item.get("deliveryLng", {}).get("N")) if "deliveryLng" in item else None,
            delivery_otp=item.get("deliveryOtp", {}).get("S") if "deliveryOtp" in item else None,
            pickup_otp=item.get("pickupOtp", {}).get("S") if "pickupOtp" in item else None,
            rider_assigned_at=item.get("riderAssignedAt", {}).get("S") if "riderAssignedAt" in item else None,
            rider_pickup_at=item.get("riderPickupAt", {}).get("S") if "riderPickupAt" in item else None,
            rider_delivered_at=item.get("riderDeliveredAt", {}).get("S") if "riderDeliveredAt" in item else None,
            # Real-time rider tracking
            rider_current_lat=float(item.get("riderCurrentLat", {}).get("N")) if "riderCurrentLat" in item else None,
            rider_current_lng=float(item.get("riderCurrentLng", {}).get("N")) if "riderCurrentLng" in item else None,
            rider_speed=float(item.get("riderSpeed", {}).get("N")) if "riderSpeed" in item else None,
            rider_heading=float(item.get("riderHeading", {}).get("N")) if "riderHeading" in item else None,
            rider_location_updated_at=item.get("riderLocationUpdatedAt", {}).get("S") if "riderLocationUpdatedAt" in item else None,
            # Rider assignment tracking
            rider_assignment_attempts=int(item.get("riderAssignmentAttempts", {}).get("N", "0")),
            last_assignment_attempt_at=item.get("lastAssignmentAttemptAt", {}).get("S") if "lastAssignmentAttemptAt" in item else None,
            # Rider offer tracking
            offered_at=item.get("offeredAt", {}).get("S") if "offeredAt" in item else None,
            rejected_by_riders=dynamodb_to_python(item["rejectedByRiders"]) if "rejectedByRiders" in item else [],
            created_at=created_at
        )
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format"""
        import json
        from utils.dynamodb_helpers import python_to_dynamodb
        
        # Ensure createdAt is an integer (timestamp in milliseconds)
        created_at = self.created_at
        if isinstance(created_at, str):
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                created_at = int(dt.timestamp() * 1000)
            except:
                created_at = int(time.time() * 1000)
        
        item = {
            "orderId": {"S": self.order_id},
            "customerPhone": {"S": self.customer_phone},
            "restaurantId": {"S": self.restaurant_id},
            "items": python_to_dynamodb(self.items),  # Store as List of Maps
            "foodTotal": {"N": str(self.food_total)},
            "deliveryFee": {"N": str(self.delivery_fee)},
            "platformFee": {"N": str(self.platform_fee)},
            "grandTotal": {"N": str(self.grand_total)},
            "status": {"S": self.status},
            "createdAt": {"N": str(created_at)},  # Store as number for GSI sorting
            # Composite sort keys for efficient status filtering
            "customerStatusCreatedAt": {"S": f"{self.status}#{created_at}"},
            "restaurantStatusCreatedAt": {"S": f"{self.status}#{created_at}"}
        }
        if self.rider_id:
            item["riderId"] = {"S": self.rider_id}
            # Add rider composite key for efficient status filtering
            item["riderStatusCreatedAt"] = {"S": f"{self.status}#{created_at}"}
        if self.restaurant_name:
            item["restaurantName"] = {"S": self.restaurant_name}
        if self.restaurant_image:
            item["restaurantImage"] = {"S": self.restaurant_image}
        if self.delivery_address:
            item["deliveryAddress"] = {"S": self.delivery_address}
        if self.formatted_address:
            item["formattedAddress"] = {"S": self.formatted_address}
        if self.address_id:
            item["addressId"] = {"S": self.address_id}
        if self.payment_id:
            item["paymentId"] = {"S": self.payment_id}
        if self.payment_method:
            item["paymentMethod"] = {"S": self.payment_method}
        if self.revenue:
            item["revenue"] = python_to_dynamodb(self.revenue)  # Store as Map
        # Rider fields
        if self.pickup_address:
            item["pickupAddress"] = {"S": self.pickup_address}
        if self.pickup_lat is not None:
            item["pickupLat"] = {"N": str(self.pickup_lat)}
        if self.pickup_lng is not None:
            item["pickupLng"] = {"N": str(self.pickup_lng)}
        if self.delivery_lat is not None:
            item["deliveryLat"] = {"N": str(self.delivery_lat)}
        if self.delivery_lng is not None:
            item["deliveryLng"] = {"N": str(self.delivery_lng)}
        if self.delivery_otp:
            item["deliveryOtp"] = {"S": self.delivery_otp}
        if self.pickup_otp:
            item["pickupOtp"] = {"S": self.pickup_otp}
        if self.rider_assigned_at:
            item["riderAssignedAt"] = {"S": self.rider_assigned_at}
        if self.rider_pickup_at:
            item["riderPickupAt"] = {"S": self.rider_pickup_at}
        if self.rider_delivered_at:
            item["riderDeliveredAt"] = {"S": self.rider_delivered_at}
        # Real-time rider tracking
        if self.rider_current_lat is not None:
            item["riderCurrentLat"] = {"N": str(self.rider_current_lat)}
        if self.rider_current_lng is not None:
            item["riderCurrentLng"] = {"N": str(self.rider_current_lng)}
        if self.rider_speed is not None:
            item["riderSpeed"] = {"N": str(self.rider_speed)}
        if self.rider_heading is not None:
            item["riderHeading"] = {"N": str(self.rider_heading)}
        if self.rider_location_updated_at:
            item["riderLocationUpdatedAt"] = {"S": self.rider_location_updated_at}
        # Rider offer tracking
        if self.offered_at:
            item["offeredAt"] = {"S": self.offered_at}
        if self.rejected_by_riders:
            item["rejectedByRiders"] = python_to_dynamodb(self.rejected_by_riders)
        return item
