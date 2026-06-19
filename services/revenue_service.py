"""Shared revenue computation.

Extracted from `revenue_calculator.py` so that the stream-triggered Lambda
and the ops item-adjustment endpoint compute revenue from the exact same
code path. Any future change to the revenue model belongs here.
"""
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, Tuple, List
from aws_lambda_powertools import Logger
from config.pricing import calculate_customer_price_from_hike
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python

logger = Logger(service="revenue-service")


def _fetch_restaurant_commission_config(restaurant_id: str) -> dict:
    """Fetch commission config for a restaurant from CONFIG#RESTAURANT#{restaurant_id}."""
    try:
        response = dynamodb_client.get_item(
            TableName=TABLES["CONFIG"],
            Key={
                "partitionkey": {"S": f"CONFIG#RESTAURANT#{restaurant_id}"},
                "sortKey": {"S": "CONFIG"},
            },
        )
        item = response.get("Item")
        if not item:
            return {}
        config = dynamodb_to_python(item.get("config", {"NULL": True}))
        return config if isinstance(config, dict) else {}
    except Exception as e:
        logger.warning(f"Failed to fetch restaurant commission config for {restaurant_id}: {e}")
        return {}


def _fetch_global_default_commission() -> float:
    """Fetch defaultCommission from CONFIG#GLOBAL / CONFIG. Returns 0.0 if not found."""
    try:
        response = dynamodb_client.get_item(
            TableName=TABLES["CONFIG"],
            Key={
                "partitionkey": {"S": "CONFIG#GLOBAL"},
                "sortKey": {"S": "CONFIG"},
            },
        )
        item = response.get("Item")
        if not item:
            return 0.0
        config = dynamodb_to_python(item.get("config", {"NULL": True}))
        if not isinstance(config, dict):
            return 0.0
        return float(config.get("restaurantCommissionPercentage", 0) or 0)
    except Exception as e:
        logger.warning(f"Failed to fetch global default commission: {e}")
        return 0.0


def _resolve_commission_pct(item_id: str, restaurant_config: dict, global_default: float) -> float:
    """Resolve commission percentage with 3-tier priority: item → restaurant → global."""
    item_key = f"ITM-{item_id}_commissionPercentage"
    for field in (item_key, "restaurantCommissionPercentage"):
        value = restaurant_config.get(field)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
    return global_default


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return default


def _normalize_coupon_issuer(value) -> Optional[str]:
    normalized = str(value or "").strip().upper()
    return normalized or None


def _get_gross_item_price(item: dict, customer_price: float, stored_discount_amount: float) -> float:
    restaurant_price = _safe_float(item.get("restaurantPrice"))
    hike_percentage = _safe_float(item.get("hikePercentage"))

    if restaurant_price > 0:
        reconstructed = calculate_customer_price_from_hike(restaurant_price, hike_percentage)
        return round(max(reconstructed, customer_price), 2)

    if stored_discount_amount > 0:
        return round(customer_price + stored_discount_amount, 2)

    return round(customer_price, 2)


def compute_revenue(order) -> Tuple[dict, list]:
    """Build the revenue dict + enriched items from an Order object.

    Pure function — no writes. Mirrors the prior `_compute_revenue` body that
    used to live in `revenue_calculator.py`.

    Returns (revenue_dict, enriched_items).
    """
    # Local import avoids a module-level import cycle with coupon_service.
    from services.coupon_service import CouponService

    total_customer_paid = 0.0
    gross_food_value = 0.0
    total_food_commission = 0.0
    total_item_coupon_discount = 0.0
    platform_item_coupon_discount = 0.0
    restaurant_item_coupon_discount = 0.0
    total_items_restaurant_price = 0.0
    restaurant_revenue: dict = {}
    platform_revenue: dict = {}
    enriched_items: list = []

    restaurant_config = _fetch_restaurant_commission_config(order.restaurant_id)
    global_default_commission = _fetch_global_default_commission()

    logger.info(
        f"Commission config for restaurant {order.restaurant_id}: "
        f"restaurantCommissionPercentage={restaurant_config.get('restaurantCommissionPercentage')}, "
        f"globalDefault={global_default_commission}"
    )

    total_add_on_value = 0.0

    for item in (order.items or []):
        item_copy = dict(item)
        item_id = item.get("itemId") or item.get("item_id") or ""
        try:
            quantity = int(item.get("quantity", 1) or 1)
        except (TypeError, ValueError):
            quantity = 1
        customer_price = _safe_float(item.get("price"))
        add_on_total = _safe_float(item.get("addOnTotal"))
        restaurant_price = _safe_float(item.get("restaurantPrice"))
        # Pre-discount (gross) customer price reconstructed from restaurantPrice + hike;
        # pass 0 stored discount so the gross does NOT come from the stored itemDiscountAmount.
        gross_price = _get_gross_item_price(item, customer_price, 0.0)
        # Re-derive the per-item coupon discount live by re-fetching the line's
        # item-offer coupon (instead of trusting the stored itemDiscountAmount).
        disc = CouponService.get_item_coupon_discount(
            item.get("itemOfferCouponCode"),
            gross_price,
            order.restaurant_id,
            item_id,
        )
        item_discount_amount = round(max(_safe_float(disc.get("discountAmount")), 0.0), 2)
        # Use the freshly-fetched coupon's issuer; fall back to the stored value
        # only when no live coupon was applied (keeps commission-base stable).
        fresh_issuer = disc.get("issuedBy")
        coupon_issued_by = _normalize_coupon_issuer(
            fresh_issuer if fresh_issuer is not None else item.get("couponIssuedBy")
        )
        item_restaurant_owed = restaurant_price if restaurant_price > 0 else gross_price
        total_items_restaurant_price += item_restaurant_owed * quantity

        commission_pct = _resolve_commission_pct(item_id, restaurant_config, global_default_commission)
       
        #taking gross price always as commission base, because commission is calculated on gross price, not on customer price
        commission_base = gross_price
        item_commission_per_unit = round(commission_base * commission_pct / 100.0, 4)
        item_commission = item_commission_per_unit * quantity

        item_copy["itemCommissionPercentage"] = round(commission_pct, 4)
        item_copy["itemCommissionAmount"] = round(item_commission, 4)
        item_copy["grossPrice"] = gross_price
        item_copy["itemDiscountAmount"] = item_discount_amount
        item_copy["couponIssuedBy"] = coupon_issued_by

        enriched_items.append(item_copy)

        total_food_commission += item_commission
        total_customer_paid += (customer_price + add_on_total) * quantity
        gross_food_value += (gross_price + add_on_total) * quantity
        total_add_on_value += add_on_total * quantity
        total_item_coupon_discount += item_discount_amount * quantity

        if coupon_issued_by == "YUMDUDE":
            platform_item_coupon_discount += item_discount_amount * quantity
        elif coupon_issued_by == "RESTAURANT":
            restaurant_item_coupon_discount += item_discount_amount * quantity

    fee_resp = order.calculated_fee_response
    fee_dict = fee_resp if isinstance(fee_resp, dict) else {}
    food_commission = round(total_food_commission, 2)
    platform_revenue["foodCommission"] = food_commission

    distance_km = _safe_float(fee_dict.get("distance"))
    long_distance_bonus = 15.0 if distance_km > 6 else 0.0

    platform_fee = 0.0
    if isinstance(fee_resp, dict):
        try:
            platform_fee = float(fee_resp.get("platformFee", 0) or 0)
        except (TypeError, ValueError):
            platform_fee = 0.0
    elif order.platform_fee:
        platform_fee = float(order.platform_fee)

    platform_revenue["platformFee"] = platform_fee

    total_platform_revenue = round(food_commission + platform_fee, 2)
    restaurant_settlement = round(gross_food_value - food_commission, 2)
    restaurant_revenue["revenue"] = restaurant_settlement

    restaurant_owed_total = round(total_items_restaurant_price + total_add_on_value, 2)
    if restaurant_settlement > restaurant_owed_total:
        excess_from_restaurant = round(restaurant_settlement - restaurant_owed_total, 2)
        restaurant_settlement = restaurant_owed_total
        restaurant_revenue["revenue"] = restaurant_settlement
        platform_revenue["excessFromRestaurantRevenue"] = excess_from_restaurant
        total_platform_revenue = round(total_platform_revenue + excess_from_restaurant, 2)

    coupon_code = None
    coupon_applied = False
    coupon_discount = 0.0
    issued_by = None
    is_once_per_user = False
    is_once_per_day = False

    if isinstance(fee_resp, dict):
        coupon_applied = bool(fee_resp.get("couponApplied"))
        coupon_discount = float(fee_resp.get("breakdown", {}).get("couponDiscount", 0))
        coupon_code = fee_resp.get("couponCode", None)

    if coupon_applied and coupon_code:
        try:
            pk = f"COUPON#{coupon_code}"
            response = dynamodb_client.query(
                TableName=TABLES["CONFIG"],
                KeyConditionExpression="partitionkey = :pk",
                ExpressionAttributeValues={":pk": {"S": pk}},
                Limit=1,
            )
            coupon_item = (response.get("Items") or [None])[0]
            if coupon_item:
                issued_by = _normalize_coupon_issuer(coupon_item.get("issuedBy", {}).get("S"))
                is_once_per_user = coupon_item.get("isOncePerUser", {}).get("BOOL", False)
                is_once_per_day = coupon_item.get("isOncePerDay", {}).get("BOOL", False)
        except Exception as e:
            logger.warning(f"Failed to look up coupon {coupon_code}: {e}")

    delivery_fee_discount_raw = fee_dict.get("breakdown", {}).get("deliveryFeeDiscount", 0)
    if delivery_fee_discount_raw and delivery_fee_discount_raw > 0:
        platform_revenue["deliveryFeeDiscount"] = round(delivery_fee_discount_raw, 2)

    rider_settlement = _safe_float(fee_dict.get("riderSettlementAmount"))
    customer_delivery_fee = _safe_float(fee_dict.get("deliveryFee"))
    rider_delivery_subsidy = round(max(rider_settlement - customer_delivery_fee, 0.0), 2)
    if rider_delivery_subsidy > 0:
        total_platform_revenue = round(total_platform_revenue - rider_delivery_subsidy, 2)
        platform_revenue["riderDeliverySubsidy"] = rider_delivery_subsidy

    if coupon_applied and coupon_discount > 0:
        if issued_by == "YUMDUDE":
            total_platform_revenue = round(total_platform_revenue - coupon_discount, 2)
            platform_revenue["couponDiscount"] = coupon_discount
        elif issued_by == "RESTAURANT":
            restaurant_settlement = round(restaurant_settlement - coupon_discount, 2)
            restaurant_revenue["couponDiscount"] = coupon_discount

    # YumCoins redemption is always platform-funded: the customer paid less for
    # food, but the restaurant still settles on the gross food value, so the
    # platform absorbs the coin discount (same shape as a YUMDUDE coupon).
    coin_discount_amt = _safe_float(getattr(order, "coin_discount", 0))
    if coin_discount_amt > 0:
        total_platform_revenue = round(total_platform_revenue - coin_discount_amt, 2)
        platform_revenue["coinDiscount"] = round(coin_discount_amt, 2)

    if platform_item_coupon_discount > 0:
        platform_revenue["itemCouponDiscount"] = round(platform_item_coupon_discount, 2)
    if restaurant_item_coupon_discount > 0:
        restaurant_revenue["itemCouponDiscount"] = round(restaurant_item_coupon_discount, 2)

    restaurant_revenue["finalPayout"] = round(
        restaurant_revenue.get("revenue", 0)
        - restaurant_revenue.get("couponDiscount", 0)
        - restaurant_revenue.get("itemCouponDiscount", 0),
        2,
    )
    platform_revenue["finalPayout"] = round(
        platform_revenue.get("foodCommission", 0)
        + platform_revenue.get("platformFee", 0)
        + platform_revenue.get("excessFromRestaurantRevenue", 0)
        - platform_revenue.get("riderDeliverySubsidy", 0)
        - platform_revenue.get("couponDiscount", 0)
        - platform_revenue.get("itemCouponDiscount", 0)
        - platform_revenue.get("coinDiscount", 0)
        - long_distance_bonus,
        2,
    )

    rider_revenue = {
        "finalPayout": round(rider_settlement + long_distance_bonus, 2),
        "riderSettlementAmount": round(rider_settlement, 2),
    }
    if long_distance_bonus > 0:
        rider_revenue["longDistanceBonus"] = long_distance_bonus

    gst_data = fee_dict.get("gst", {})
    gst_on_food = round(float(gst_data.get("gstOnFood", 0) or 0), 2)
    gst_on_delivery = round(float(gst_data.get("gstOnDeliveryFee", 0) or 0), 2)
    gst_on_platform = round(float(gst_data.get("gstOnPlatformFee", 0) or 0), 2)
    total_gst = round(gst_on_food + gst_on_delivery + gst_on_platform, 2)

    revenue = {
        "totalCustomerPaid": round(total_customer_paid, 2),
        "customerPaidFoodValue": round(total_customer_paid, 2),
        "grossFoodValue": round(gross_food_value, 2),
        "totalAddOnValue": round(total_add_on_value, 2),
        "itemCouponDiscountTotal": round(total_item_coupon_discount, 2),
        "itemCouponDiscountByPlatform": round(platform_item_coupon_discount, 2),
        "itemCouponDiscountByRestaurant": round(restaurant_item_coupon_discount, 2),
        "totalDiscount": round(fee_dict.get("breakdown", {}).get("totalDiscount", 0), 2),
        "couponCode": coupon_code,
        "couponApplied": coupon_applied,
        "couponIssuedBy": issued_by,
        "couponIsOncePerUser": is_once_per_user,
        "couponIsOncePerDay": is_once_per_day,
        "restaurantRevenue": restaurant_revenue,
        "platformRevenue": platform_revenue,
        "riderRevenue": rider_revenue,
        "govtRevenue": {
            "gstOnFood": gst_on_food,
            "gstOnDeliveryFee": gst_on_delivery,
            "gstOnPlatformFee": gst_on_platform,
            "finalPayout": total_gst,
        },
    }
    return revenue, enriched_items
