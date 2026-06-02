"""
Lambda triggered by DynamoDB Orders stream when an order becomes CONFIRMED.
Computes the revenue breakdown (food commission, platform revenue, restaurant settlement)
and writes it to both the Order and Payment records.

The actual revenue math lives in `services/revenue_service.py` so the ops
item-adjustment endpoint can recompute revenue using the same code path.
"""
import json
from datetime import datetime, timezone, timedelta
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.order_service import OrderService
from services.payment_service import PaymentService
from services.revenue_service import compute_revenue
from utils.dynamodb import dynamodb_client, TABLES

logger = Logger(service="revenue-calculator")


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

            revenue, items_with_commission = compute_revenue(order)
            logger.info(f"[orderId={order_id}] Revenue computed: {json.dumps(revenue)}")

            # Stamp the originalGrandTotal / prepaidAmount snapshot at first
            # CONFIRMED so the ops adjustment endpoint can always compute
            # `delta = newGrandTotal - originalGrandTotal` deterministically.
            pm = (order.payment_method or "").upper()
            pc = (order.payment_channel or "").upper()
            is_cod = pm == "COD" or pc in ("COD_AT_DELIVERY", "UPI_QR_AT_RIDER")
            prepaid_amount = 0.0 if is_cod else float(order.grand_total or 0)
            amount_due = float(order.grand_total or 0) - prepaid_amount

            OrderService.update_order(
                order_id,
                {
                    "revenue": revenue,
                    "items": items_with_commission,
                    "originalGrandTotal": float(order.grand_total or 0),
                    "prepaidAmount": prepaid_amount,
                    "amountDueAtDelivery": amount_due,
                },
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
