"""Order model"""
import time
from typing import List, Optional, Dict, Any, Union
from utils.datetime_ist import now_ist_iso, epoch_ms_to_ist_iso
from utils.dynamodb_helpers import dynamodb_to_python, python_to_dynamodb


class Order:
    """Order model"""

    @staticmethod
    def _platform_fee_from_calculated_response(calculated_fee_response: Optional[Dict[str, Any]]) -> Optional[float]:
        if not isinstance(calculated_fee_response, dict):
            return None
        try:
            value = calculated_fee_response.get("platformFee")
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None
    
    #restaurant statuses
    STATUS_INITIATED = "INITIATED"  # Order created, payment pending
    STATUS_PENDING = "PENDING"  # Payment completed, awaiting restaurant confirmation # old flow
    STATUS_CONFIRMED = "CONFIRMED"  # Payment successful, confirmed by platform
    STATUS_ACCEPTED = "ACCEPTED"  # Accepted by restaurant

    # Order assignment statuses
    STATUS_PREPARING = "PREPARING"   
    READY_FOR_PICKUP = "READY_FOR_PICKUP"
    STATUS_AWAITING_RIDER_ASSIGNMENT = "AWAITING_RIDER_ASSIGNMENT"
    OFFERED_TO_RIDER = "OFFERED_TO_RIDER"

    # Rider statuses
    RIDER_ASSIGNED = "RIDER_ASSIGNED"
    PICKED_UP = "PICKED_UP"
    STATUS_OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    STATUS_DELIVERED = "DELIVERED"
    STATUS_CANCELLED = "CANCELLED"
    STATUS_FAILED_INVENTORY = "FAILED_INVENTORY"  # theater orders only

    # Order types
    ORDER_TYPE_DELIVERY = "DELIVERY"
    ORDER_TYPE_PICKUP = "PICKUP"  # in-venue (theater) — no rider, no delivery address
    
    @classmethod
    def get_all_statuses(cls):
        """Get all valid order statuses"""
        return [
            cls.STATUS_INITIATED,
            cls.STATUS_PENDING,
            cls.STATUS_CONFIRMED,
            cls.STATUS_ACCEPTED,
            cls.STATUS_PREPARING,
            cls.READY_FOR_PICKUP,
            cls.STATUS_AWAITING_RIDER_ASSIGNMENT,
            cls.OFFERED_TO_RIDER,
            cls.RIDER_ASSIGNED,
            cls.PICKED_UP,
            cls.STATUS_OUT_FOR_DELIVERY,
            cls.STATUS_DELIVERED,
            cls.STATUS_CANCELLED
        ]
    
    def __init__(
        self,
        order_id: str,
        customer_phone: str,
        receiver_phone: Optional[str],
        restaurant_id: str,
        items: List[Dict[str, Any]],
        food_total: float,
        delivery_fee: float,
        platform_fee: float,
        grand_total: float,
        status: str = STATUS_PENDING,
        rider_id: Optional[str] = None,
        rider_name: Optional[str] = None,
        restaurant_name: Optional[str] = None,
        restaurant_image: Optional[str] = None,
        delivery_address: Optional[str] = None,
        formatted_address: Optional[str] = None,
        address_id: Optional[str] = None,
        payment_id: Optional[str] = None,
        payment_method: Optional[str] = None,
        payment_channel: Optional[str] = None,
        rating: Optional[float] = None,
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
        created_at: Optional[Union[int, str]] = None,
        calculated_fee_response: Optional[Dict[str, Any]] = None,
        preparation_time: Optional[int] = None,
        # Theater / pickup mode
        order_type: str = ORDER_TYPE_DELIVERY,
        pickup_token: Optional[str] = None,
        inventory_reverted: bool = False,
        # Ops-initiated item adjustments (see services/order_adjustment_service.py)
        original_grand_total: Optional[float] = None,
        prepaid_amount: Optional[float] = None,
        amount_due_at_delivery: Optional[float] = None,
        adjustments: Optional[List[Dict[str, Any]]] = None,
        was_adjusted: bool = False,
        internal_status: Optional[str] = None,
        # YumCoins redemption (platform-funded discount on food)
        coins_spent: int = 0,
        coin_discount: float = 0.0,
    ):
        self.order_id = order_id
        self.customer_phone = customer_phone
        self.receiver_phone = receiver_phone or customer_phone
        self.restaurant_id = restaurant_id
        self.items = items
        self.food_total = food_total
        self.delivery_fee = delivery_fee
        self._platform_fee = float(platform_fee or 0)
        self.grand_total = grand_total
        self.status = status
        self.rider_id = rider_id
        self.rider_name = rider_name
        self.restaurant_name = restaurant_name
        self.restaurant_image = restaurant_image
        self.delivery_address = delivery_address
        self.formatted_address = formatted_address
        self.address_id = address_id
        self.payment_id = payment_id
        self.payment_method = payment_method
        self.payment_channel = payment_channel
        self.rating = rating
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
        self.calculated_fee_response = calculated_fee_response
        self.preparation_time = preparation_time
        # Theater / pickup
        self.order_type = order_type if order_type in (self.ORDER_TYPE_DELIVERY, self.ORDER_TYPE_PICKUP) else self.ORDER_TYPE_DELIVERY
        self.pickup_token = pickup_token
        self.inventory_reverted = bool(inventory_reverted)
        # Ops adjustments
        self.original_grand_total = float(original_grand_total) if original_grand_total is not None else None
        self.prepaid_amount = float(prepaid_amount) if prepaid_amount is not None else None
        self.amount_due_at_delivery = float(amount_due_at_delivery) if amount_due_at_delivery is not None else None
        self.adjustments = adjustments or []
        self.was_adjusted = bool(was_adjusted)
        self.internal_status = internal_status
        # YumCoins redemption
        self.coins_spent = int(coins_spent or 0)
        self.coin_discount = float(coin_discount or 0.0)
        # Store as IST ISO string; support int (legacy) for backward compatibility
        self.created_at = created_at if created_at is not None else now_ist_iso()

    @property
    def platform_fee(self) -> float:
        calculated_platform_fee = self._platform_fee_from_calculated_response(self.calculated_fee_response)
        if calculated_platform_fee is not None:
            return calculated_platform_fee
        return self._platform_fee
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        result = {
            "orderId": self.order_id,
            "customerPhone": self.customer_phone,
            "receiverPhone": self.receiver_phone,
            "restaurantId": self.restaurant_id,
            "items": self.items,
            "foodTotal": self.food_total,
            "deliveryFee": self.delivery_fee,
            "platformFee": self.platform_fee,
            "grandTotal": self.grand_total,
            "status": self.status,
            "riderId": self.rider_id,
            "rating": self.rating,
            "createdAt": epoch_ms_to_ist_iso(self.created_at) if isinstance(self.created_at, int) else self.created_at
        }
        if self.rider_name:
            result["riderName"] = self.rider_name
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
        if self.payment_channel:
            result["paymentChannel"] = self.payment_channel
        if self.revenue:
            result["revenue"] = self.revenue
        if self.calculated_fee_response:
            result["calculatedFeeResponse"] = self.calculated_fee_response
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
        if self.preparation_time is not None:
            result["preparationTime"] = self.preparation_time
        # Theater / pickup
        result["orderType"] = self.order_type
        if self.pickup_token:
            result["pickupToken"] = self.pickup_token
        if self.inventory_reverted:
            result["inventoryReverted"] = True
        # Ops adjustments — always include amountDueAtDelivery so client apps
        # (especially the rider app) can drive collection UI from a single
        # canonical field regardless of whether an adjustment ever happened.
        if self.original_grand_total is not None:
            result["originalGrandTotal"] = self.original_grand_total
        if self.prepaid_amount is not None:
            result["prepaidAmount"] = self.prepaid_amount
        # Default the field even when unset so rider/customer apps don't have
        # to special-case missing values. COD orders => grandTotal; prepaid => 0.
        if self.amount_due_at_delivery is not None:
            result["amountDueAtDelivery"] = self.amount_due_at_delivery
        else:
            result["amountDueAtDelivery"] = self._infer_amount_due_at_delivery()
        if self.adjustments:
            result["adjustments"] = self.adjustments
        if self.was_adjusted:
            result["wasAdjusted"] = True
        if self.internal_status:
            result["internalStatus"] = self.internal_status
        if self.coins_spent:
            result["coinsSpent"] = self.coins_spent
        if self.coin_discount:
            result["coinDiscount"] = self.coin_discount
        return result

    def _infer_amount_due_at_delivery(self) -> float:
        """Fallback used when amountDueAtDelivery hasn't been explicitly stamped
        on the order (legacy rows or pre-adjustment state). COD orders owe the
        full grandTotal at delivery; prepaid orders owe nothing."""
        pm = (self.payment_method or "").upper()
        pc = (self.payment_channel or "").upper()
        if pm == "COD" or pc in ("COD_AT_DELIVERY", "UPI_QR_AT_RIDER"):
            return float(self.grand_total or 0)
        return 0.0
    
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
        
        # Handle createdAt: S (IST ISO string) or N (legacy epoch ms)
        created_at = None
        if "createdAt" in item:
            if "S" in item["createdAt"]:
                created_at = item["createdAt"]["S"]
            elif "N" in item["createdAt"]:
                created_at = int(item["createdAt"]["N"])
        
        return cls(
            order_id=item.get("orderId", {}).get("S", ""),
            customer_phone=item.get("customerPhone", {}).get("S", ""),
            receiver_phone=item.get("receiverPhone", {}).get("S", item.get("customerPhone", {}).get("S", "")),
            restaurant_id=item.get("restaurantId", {}).get("S", ""),
            items=items,
            food_total=float(item.get("foodTotal", {}).get("N", "0")),
            delivery_fee=float(item.get("deliveryFee", {}).get("N", "0")),
            platform_fee=float(item.get("platformFee", {}).get("N", "0")),
            grand_total=float(item.get("grandTotal", {}).get("N", "0")),
            status=item.get("status", {}).get("S", cls.STATUS_PENDING),
            rider_id=item.get("riderId", {}).get("S") if "riderId" in item else None,
            rider_name=item.get("riderName", {}).get("S") if "riderName" in item else None,
            restaurant_name=item.get("restaurantName", {}).get("S") if "restaurantName" in item else None,
            restaurant_image=item.get("restaurantImage", {}).get("S") if "restaurantImage" in item else None,
            delivery_address=item.get("deliveryAddress", {}).get("S") if "deliveryAddress" in item else None,
            formatted_address=item.get("formattedAddress", {}).get("S") if "formattedAddress" in item else None,
            address_id=item.get("addressId", {}).get("S") if "addressId" in item else None,
            payment_id=item.get("paymentId", {}).get("S") if "paymentId" in item else None,
            payment_method=item.get("paymentMethod", {}).get("S") if "paymentMethod" in item else None,
            payment_channel=item.get("paymentChannel", {}).get("S") if "paymentChannel" in item else None,
            rating=float(item.get("rating", {}).get("N")) if "rating" in item else None,
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
            created_at=created_at,
            calculated_fee_response=dynamodb_to_python(item["calculatedFeeResponse"]) if "calculatedFeeResponse" in item and "M" in item["calculatedFeeResponse"] else None,
            preparation_time=int(float(item["preparationTime"]["N"])) if "preparationTime" in item else None,
            # Theater / pickup
            order_type=item.get("orderType", {}).get("S", cls.ORDER_TYPE_DELIVERY) if "orderType" in item else cls.ORDER_TYPE_DELIVERY,
            pickup_token=item.get("pickupToken", {}).get("S") if "pickupToken" in item else None,
            inventory_reverted=bool(item.get("inventoryReverted", {}).get("BOOL", False)) if "inventoryReverted" in item else False,
            # Ops adjustments
            original_grand_total=float(item["originalGrandTotal"]["N"]) if "originalGrandTotal" in item and "N" in item["originalGrandTotal"] else None,
            prepaid_amount=float(item["prepaidAmount"]["N"]) if "prepaidAmount" in item and "N" in item["prepaidAmount"] else None,
            amount_due_at_delivery=float(item["amountDueAtDelivery"]["N"]) if "amountDueAtDelivery" in item and "N" in item["amountDueAtDelivery"] else None,
            adjustments=dynamodb_to_python(item["adjustments"]) if "adjustments" in item and "L" in item["adjustments"] else [],
            was_adjusted=bool(item.get("wasAdjusted", {}).get("BOOL", False)) if "wasAdjusted" in item else False,
            internal_status=item.get("internalStatus", {}).get("S") if "internalStatus" in item else None,
            coins_spent=int(item["coinsSpent"]["N"]) if "coinsSpent" in item else 0,
            coin_discount=float(item["coinDiscount"]["N"]) if "coinDiscount" in item else 0.0,
        )
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format"""
        import json
        from utils.dynamodb_helpers import python_to_dynamodb
        
        # Normalize createdAt to IST ISO string for storage
        created_at = self.created_at
        if isinstance(created_at, int):
            created_at = epoch_ms_to_ist_iso(created_at)
        elif not isinstance(created_at, str):
            created_at = now_ist_iso()
        
        item = {
            "orderId": {"S": self.order_id},
            "customerPhone": {"S": self.customer_phone},
            "receiverPhone": {"S": self.receiver_phone or self.customer_phone},
            "restaurantId": {"S": self.restaurant_id},
            "items": python_to_dynamodb(self.items),  # Store as List of Maps
            "foodTotal": {"N": str(self.food_total)},
            "deliveryFee": {"N": str(self.delivery_fee)},
            "grandTotal": {"N": str(self.grand_total)},
            "status": {"S": self.status},
            "createdAt": {"S": created_at},  # IST ISO string
            # Composite sort keys for efficient status filtering (ISO sorts correctly)
            "customerStatusCreatedAt": {"S": f"{self.status}#{created_at}"},
            "restaurantStatusCreatedAt": {"S": f"{self.status}#{created_at}"}
        }
        if self.rider_id:
            item["riderId"] = {"S": self.rider_id}
            item["riderStatusCreatedAt"] = {"S": f"{self.status}#{created_at}"}
        if self.rider_name:
            item["riderName"] = {"S": self.rider_name}
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
        if self.payment_channel:
            item["paymentChannel"] = {"S": self.payment_channel}
        if self.rating is not None:
            item["rating"] = {"N": str(self.rating)}
        if self.revenue:
            item["revenue"] = python_to_dynamodb(self.revenue)
        if self.calculated_fee_response:
            item["calculatedFeeResponse"] = python_to_dynamodb(self.calculated_fee_response)
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
        if self.preparation_time is not None:
            item["preparationTime"] = {"N": str(self.preparation_time)}
        # Theater / pickup — only persist non-default values to keep existing rows untouched
        if self.order_type and self.order_type != self.ORDER_TYPE_DELIVERY:
            item["orderType"] = {"S": self.order_type}
        if self.pickup_token:
            item["pickupToken"] = {"S": self.pickup_token}
        if self.inventory_reverted:
            item["inventoryReverted"] = {"BOOL": True}
        # Ops adjustments — persist only when explicitly set so old rows don't churn.
        if self.original_grand_total is not None:
            item["originalGrandTotal"] = {"N": str(self.original_grand_total)}
        if self.prepaid_amount is not None:
            item["prepaidAmount"] = {"N": str(self.prepaid_amount)}
        if self.amount_due_at_delivery is not None:
            item["amountDueAtDelivery"] = {"N": str(self.amount_due_at_delivery)}
        if self.adjustments:
            item["adjustments"] = python_to_dynamodb(self.adjustments)
        if self.was_adjusted:
            item["wasAdjusted"] = {"BOOL": True}
        if self.internal_status:
            item["internalStatus"] = {"S": self.internal_status}
        if self.coins_spent:
            item["coinsSpent"] = {"N": str(self.coins_spent)}
        if self.coin_discount:
            item["coinDiscount"] = {"N": str(self.coin_discount)}
        return item
