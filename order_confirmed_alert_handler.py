"""
Lambda invoked by EventBridge Scheduler x minutes after order becomes CONFIRMED.
If order is still CONFIRMED, publishes an alert to the SNS order-alerts topic (SMS to subscribers).
"""
import json
import os
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.order_service import OrderService
from services.sns_alert_service import publish_order_alert
from models.order import Order

logger = Logger(service="order-confirmed-alert-handler")


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Expected event: { "orderId": "...", "delayMins": 15 }
    """
    try:
        order_id = event.get("orderId")
        if not order_id:
            return {"statusCode": 400, "body": json.dumps({"error": "orderId required"})}

        logger.info(f"[orderId={order_id}] Confirmed-alert check triggered")
        order = OrderService.get_order(order_id)
        if not order:
            logger.warning(f"[orderId={order_id}] Order not found")
            return {"statusCode": 200, "body": json.dumps({"message": "Order not found"})}

        if order.status != Order.STATUS_CONFIRMED:
            logger.info(f"[orderId={order_id}] Status is {order.status}, skipping alert")
            return {"statusCode": 200, "body": json.dumps({"message": "No longer CONFIRMED"})}

        # Toggle: if CONFIRMED alerts are disabled, do not publish
        enabled = (os.environ.get("ORDER_ALERTS_CONFIRMED_ENABLED") or "true").strip().lower() in ("true", "1", "yes")
        if not enabled:
            logger.info(f"[orderId={order_id}] ORDER_ALERTS_CONFIRMED_ENABLED is off, skipping publish")
            return {"statusCode": 200, "body": json.dumps({"message": "Alerts disabled"})}

        delay_mins = event.get("delayMins", 15)
        restaurant_name = order.restaurant_name or "Restaurant"
        message = (
            f"Order {order_id} has been in CONFIRMED state for over {delay_mins} minutes. "
            f"Restaurant: {restaurant_name}. Please follow up."
        )
        success = publish_order_alert(message, subject="Order still confirmed")
        if success:
            logger.info(f"[orderId={order_id}] CONFIRMED alert published to SNS")
        else:
            logger.warning(f"[orderId={order_id}] Failed to publish CONFIRMED alert")

        return {"statusCode": 200, "body": json.dumps({"published": success})}
    except Exception as e:
        logger.error(f"Error in order confirmed alert handler: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
