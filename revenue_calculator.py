"""
Lambda triggered by DynamoDB Orders stream when an order becomes CONFIRMED.
Computes the revenue breakdown (food commission, platform revenue, restaurant settlement)
and writes it to both the Order and Payment records.
"""
import json
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.order_service import OrderService
from services.payment_service import PaymentService
from services.menu_service import MenuService
from utils.dynamodb import dynamodb_client, TABLES

logger = Logger(service="revenue-calculator")


def _compute_revenue(order) -> dict:
    """
    Build the revenue dict from an Order object.
    Mirrors the logic previously in initiate_payment (payment_routes.py).
    """
    total_customer_amount = 0.0
    total_restaurant_amount = 0.0

    for item in (order.items or []):
        item_id = item.get("itemId") or item.get("item_id") or ""
        quantity = int(item.get("quantity", 1))
        try:
            menu_item = MenuService.get_menu_item(order.restaurant_id, item_id)
            if menu_item:
                customer_price = menu_item.price
                restaurant_price = menu_item.restaurant_price
            else:
                customer_price = float(item.get("price", 0))
                restaurant_price = float(item.get("restaurantPrice", 0))
        except Exception:
            customer_price = float(item.get("price", 0))
            restaurant_price = float(item.get("restaurantPrice", 0))

        total_customer_amount += customer_price * quantity
        total_restaurant_amount += restaurant_price * quantity

    food_commission = round(total_customer_amount - total_restaurant_amount, 2)
    platform_fee = order.platform_fee or 0
    total_platform_revenue = round(food_commission + platform_fee, 2)

    coupon_code = None
    coupon_applied = False
    total_discount = 0.0

    fee_resp = order.calculated_fee_response
    if isinstance(fee_resp, dict):
        coupon_applied = bool(fee_resp.get("couponApplied"))
        total_discount = float(fee_resp.get("breakdown", {}).get("discount", 0))

    issued_by = None
    if coupon_applied and coupon_code is None:
        if isinstance(fee_resp, dict):
            coupon_code = fee_resp.get("couponCode")

    if not coupon_code:
        coupon_code = _extract_coupon_from_order(order)

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
                issued_by = coupon_item.get("issuedBy", {}).get("S")
        except Exception as e:
            logger.warning(f"Failed to look up coupon {coupon_code}: {e}")

    restaurant_settlement = round(total_restaurant_amount, 2)
    coupon_discount = 0.0
    if coupon_applied and total_discount > 0:
        coupon_discount = total_discount
        if issued_by == "YUMDUDE":
            total_platform_revenue = round(total_platform_revenue - total_discount, 2)
        elif issued_by == "RESTAURANT":
            restaurant_settlement = round(restaurant_settlement - total_discount, 2)

    return {
        "totalCustomerPaid": round(order.grand_total, 2),
        "totalDiscount": round(total_discount, 2),
        "restaurantSettlement": restaurant_settlement,
        "couponCode": coupon_code,
        "couponApplied": coupon_applied,
        "couponIssuedBy": issued_by,
        "platformRevenue": {
            "foodCommission": food_commission,
            "deliveryFee": order.delivery_fee or 0,
            "platformFee": platform_fee,
            "totalCommission": total_platform_revenue,
            "couponDiscount": round(coupon_discount, 2),
        },
    }


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

            revenue = _compute_revenue(order)
            logger.info(f"[orderId={order_id}] Revenue computed: {json.dumps(revenue)}")

            OrderService.update_order(order_id, {"revenue": revenue})
            logger.info(f"[orderId={order_id}] Revenue written to order")

            if order.payment_id:
                PaymentService.update_payment(order.payment_id, {"revenue": revenue})
                logger.info(f"[orderId={order_id}] Revenue written to payment {order.payment_id}")

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
