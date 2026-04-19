"""
Lambda triggered by DynamoDB Orders stream when an order becomes CONFIRMED.
Computes the revenue breakdown (food commission, platform revenue, restaurant settlement)
and writes it to both the Order and Payment records.
"""
import json
from datetime import datetime, timezone, timedelta
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from config.pricing import calculate_customer_price_from_hike
from services.order_service import OrderService
from services.payment_service import PaymentService
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python

logger = Logger(service="revenue-calculator")


def _fetch_restaurant_commission_config(restaurant_id: str) -> dict:
    """
    Fetch commission config for a restaurant from CONFIG#RESTAURANT#{restaurant_id}.
    Returns the parsed 'config' attribute dict, or {} if not found.
    """
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
    """
    Fetch defaultCommission from CONFIG#GLOBAL / CONFIG.
    Returns 0.0 if not found.
    """
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
    """
    Resolve commission percentage for an item using 3-tier priority:
    1. Item-level:       ITM-{item_id}_commissionPercentage  in restaurant config
    2. Restaurant-level: restaurantCommissionPercentage       in restaurant config
    3. Global default:   defaultCommission                    from CONFIG#GLOBAL
    """
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


def _normalize_coupon_issuer(value) -> str | None:
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


def _compute_revenue(order) -> tuple[dict, list]:
    """
    Build the revenue dict from an Order object and a list of order line items
    annotated with commission fields.

    Food commission is percentage-driven using a 3-tier config lookup:
    item-level → restaurant-level → global default.

    Each item in the returned list includes:
      - itemCommissionPercentage: resolved commission % for that line
      - itemCommissionAmount: total platform commission for the line (all units)
    """
    total_customer_paid = 0.0
    gross_food_value = 0.0
    total_food_commission = 0.0
    total_item_coupon_discount = 0.0
    platform_item_coupon_discount = 0.0
    restaurant_item_coupon_discount = 0.0
    restaurantRevenue = {}
    platformRevenue = {}
    enriched_items: list = []

    restaurant_config = _fetch_restaurant_commission_config(order.restaurant_id)
    global_default_commission = _fetch_global_default_commission()

    logger.info(
        f"Commission config for restaurant {order.restaurant_id}: "
        f"restaurantCommissionPercentage={restaurant_config.get('restaurantCommissionPercentage')}, "
        f"globalDefault={global_default_commission}"
    )

    for item in (order.items or []):
        item_copy = dict(item)
        item_id = item.get("itemId") or item.get("item_id") or ""
        try:
            quantity = int(item.get("quantity", 1) or 1)
        except (TypeError, ValueError):
            quantity = 1
        customer_price = _safe_float(item.get("price"))
        stored_discount_amount = max(_safe_float(item.get("itemDiscountAmount")), 0.0)
        gross_price = _get_gross_item_price(item, customer_price, stored_discount_amount)
        item_discount_amount = round(max(gross_price - customer_price, 0.0), 2)
        coupon_issued_by = _normalize_coupon_issuer(item.get("couponIssuedBy"))

        commission_pct = _resolve_commission_pct(item_id, restaurant_config, global_default_commission)
        # Per-unit commission (4 dp), then line total
        item_commission_per_unit = round(customer_price * commission_pct / 100.0, 4)
        item_commission = item_commission_per_unit * quantity

        item_copy["itemCommissionPercentage"] = round(commission_pct, 4)
        item_copy["itemCommissionAmount"] = round(item_commission, 4)
        item_copy["grossPrice"] = gross_price
        item_copy["itemDiscountAmount"] = item_discount_amount
        item_copy["couponIssuedBy"] = coupon_issued_by

        enriched_items.append(item_copy)

        total_food_commission += item_commission
        total_customer_paid += customer_price * quantity
        gross_food_value += gross_price * quantity
        total_item_coupon_discount += item_discount_amount * quantity

        if coupon_issued_by == "YUMDUDE":
            platform_item_coupon_discount += item_discount_amount * quantity
        elif coupon_issued_by == "RESTAURANT":
            restaurant_item_coupon_discount += item_discount_amount * quantity

        logger.info(
            f"Item {item_id}: grossPrice={gross_price}, customerPrice={customer_price}, qty={quantity}, "
            f"itemDiscountAmount={item_discount_amount}, couponIssuedBy={coupon_issued_by}, "
            f"itemCommissionPercentage={item_copy['itemCommissionPercentage']}, "
            f"itemCommissionAmount={item_copy['itemCommissionAmount']}"
        )

    fee_resp = order.calculated_fee_response
    fee_dict = fee_resp if isinstance(fee_resp, dict) else {}
    food_commission = round(total_food_commission, 2)
    platformRevenue["foodCommission"] = food_commission
    
    platform_fee = 0.0
    if isinstance(fee_resp, dict):
        try:
            platform_fee = float(fee_resp.get("platformFee", 0) or 0)
        except (TypeError, ValueError):
            platform_fee = 0.0
    elif order.platform_fee:
        platform_fee = float(order.platform_fee)

    platformRevenue["platformFee"] = platform_fee

    total_platform_revenue = round(food_commission + platform_fee, 2)
    restaurant_settlement = round(gross_food_value - food_commission, 2)
    restaurantRevenue["revenue"] = restaurant_settlement

    
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


    if fee_dict.get("breakdown", {}).get("deliveryFeeDiscount", 0) > 0:
        dfd = fee_dict.get("breakdown", {}).get("deliveryFeeDiscount", 0)
        total_platform_revenue = round(total_platform_revenue - dfd, 2)
        platformRevenue["deliveryFeeDiscount"] = dfd

    if coupon_applied and coupon_discount > 0:
        if issued_by == "YUMDUDE":
            total_platform_revenue = round(total_platform_revenue - coupon_discount, 2)
            platformRevenue["couponDiscount"] = coupon_discount
        elif issued_by == "RESTAURANT":
            restaurant_settlement = round(restaurant_settlement - coupon_discount, 2)
            restaurantRevenue["couponDiscount"] = coupon_discount

    if platform_item_coupon_discount > 0:
        platformRevenue["itemCouponDiscount"] = round(platform_item_coupon_discount, 2)
    if restaurant_item_coupon_discount > 0:
        restaurantRevenue["itemCouponDiscount"] = round(restaurant_item_coupon_discount, 2)

    restaurantRevenue["finalPayout"] = round(
        restaurantRevenue.get("revenue", 0)
        - restaurantRevenue.get("couponDiscount", 0)
        - restaurantRevenue.get("itemCouponDiscount", 0),
        2,
    )
    platformRevenue["finalPayout"] = round(
        platformRevenue.get("foodCommission", 0)
        + platformRevenue.get("platformFee", 0)
        - platformRevenue.get("deliveryFeeDiscount", 0)
        - platformRevenue.get("couponDiscount", 0)
        - platformRevenue.get("itemCouponDiscount", 0),
        2,
    )

    gst_data = fee_dict.get("gst", {})
    gst_on_food = round(float(gst_data.get("gstOnFood", 0) or 0), 2)
    gst_on_delivery = round(float(gst_data.get("gstOnDeliveryFee", 0) or 0), 2)
    gst_on_platform = round(float(gst_data.get("gstOnPlatformFee", 0) or 0), 2)
    total_gst = round(gst_on_food + gst_on_delivery + gst_on_platform, 2)

    revenue = {
        "totalCustomerPaid": round(total_customer_paid, 2),
        "customerPaidFoodValue": round(total_customer_paid, 2),
        "grossFoodValue": round(gross_food_value, 2),
        "itemCouponDiscountTotal": round(total_item_coupon_discount, 2),
        "itemCouponDiscountByPlatform": round(platform_item_coupon_discount, 2),
        "itemCouponDiscountByRestaurant": round(restaurant_item_coupon_discount, 2),
        "totalDiscount": round(fee_dict.get("breakdown", {}).get("totalDiscount", 0), 2),
        "couponCode": coupon_code,
        "couponApplied": coupon_applied,
        "couponIssuedBy": issued_by,
        "couponIsOncePerUser": is_once_per_user,
        "couponIsOncePerDay": is_once_per_day,
        "restaurantRevenue": restaurantRevenue,
        "platformRevenue": platformRevenue,
        "riderRevenue": {
            "finalPayout": fee_dict.get("riderSettlementAmount", 0)
        },
        "govtRevenue": {
            "gstOnFood": gst_on_food,
            "gstOnDeliveryFee": gst_on_delivery,
            "gstOnPlatformFee": gst_on_platform,
            "finalPayout": total_gst,
        },
    }
    return revenue, enriched_items


def _extract_coupon_from_order(order) -> str | None:
    """Try to find coupon code stored alongside the order (from calculatedFeeResponse or items metadata)."""
    if order.calculated_fee_response and isinstance(order.calculated_fee_response, dict):
        code = order.calculated_fee_response.get("couponCode")
        if code:
            return code
    return None


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    logger.info(f"Processing {len(event.get('Records', []))} stream records")

    processed = 0
    errors = 0

    for record in event.get("Records", []):
        try:
            if record["eventName"] != "MODIFY":
                continue

            new_image = record["dynamodb"].get("NewImage", {})
            old_image = record["dynamodb"].get("OldImage", {})

            new_status = new_image.get("status", {}).get("S", "")
            old_status = old_image.get("status", {}).get("S", "")

            if new_status != "CONFIRMED" or old_status == new_status:
                continue

            order_id = new_image.get("orderId", {}).get("S", "")
            if not order_id:
                continue

            logger.info(f"[orderId={order_id}] Computing revenue for CONFIRMED order")

            order = OrderService.get_order(order_id)
            if not order:
                logger.warning(f"[orderId={order_id}] Order not found, skipping")
                continue

            if order.revenue:
                logger.info(f"[orderId={order_id}] Revenue already present, skipping")
                continue

            revenue, items_with_commission = _compute_revenue(order)
            logger.info(f"[orderId={order_id}] Revenue computed: {json.dumps(revenue)}")

            OrderService.update_order(
                order_id,
                {"revenue": revenue, "items": items_with_commission},
            )
            logger.info(f"[orderId={order_id}] Revenue written to order")

            if order.payment_id:
                PaymentService.update_payment(order.payment_id, {"revenue": revenue})
                logger.info(f"[orderId={order_id}] Revenue written to payment {order.payment_id}")

            if revenue.get("couponApplied") and revenue.get("couponIsOncePerUser") and order.customer_phone:
                used_code = revenue.get("couponCode")
                if used_code:
                    try:
                        dynamodb_client.update_item(
                            TableName=TABLES["USERS"],
                            Key={
                                "phone": {"S": order.customer_phone},
                                "role": {"S": "CUSTOMER"},
                            },
                            UpdateExpression="ADD usedCoupons :c",
                            ExpressionAttributeValues={
                                ":c": {"SS": [used_code]},
                            },
                        )
                        logger.info(
                            f"[orderId={order_id}] Recorded coupon {used_code} "
                            f"in usedCoupons for {order.customer_phone}"
                        )
                    except Exception as e:
                        logger.error(
                            f"[orderId={order_id}] Failed to record coupon usage "
                            f"for {order.customer_phone}: {e}"
                        )

            if revenue.get("couponApplied") and revenue.get("couponIsOncePerDay") and order.customer_phone:
                used_code = revenue.get("couponCode")
                if used_code:
                    try:
                        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                        midnight_today = datetime.now(timezone.utc).replace(
                            hour=0, minute=0, second=0, microsecond=0
                        )
                        ttl_val = int((midnight_today + timedelta(days=2)).timestamp())
                        dynamodb_client.update_item(
                            TableName=TABLES["USERS"],
                            Key={
                                "phone": {"S": order.customer_phone},
                                "role": {"S": f"DAILY_COUPONS#{today_str}"},
                            },
                            UpdateExpression="ADD usedToday :c SET #ttl = :ttl",
                            ExpressionAttributeNames={"#ttl": "ttl"},
                            ExpressionAttributeValues={
                                ":c": {"SS": [used_code]},
                                ":ttl": {"N": str(ttl_val)},
                            },
                        )
                        logger.info(
                            f"[orderId={order_id}] Recorded daily coupon {used_code} "
                            f"for {order.customer_phone} on {today_str}"
                        )
                    except Exception as e:
                        logger.error(
                            f"[orderId={order_id}] Failed to record daily coupon usage "
                            f"for {order.customer_phone}: {e}"
                        )

            processed += 1

        except Exception as e:
            errors += 1
            order_id = record.get("dynamodb", {}).get("NewImage", {}).get("orderId", {}).get("S", "unknown")
            logger.error(f"[orderId={order_id}] Error computing revenue: {e}", exc_info=True)

    logger.info(f"Revenue calculator complete: {processed} processed, {errors} errors")

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": processed, "errors": errors}),
    }
