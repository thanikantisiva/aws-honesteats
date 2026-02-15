"""
Lambda function to delay order assignment after PREPARING
Triggered by EventBridge Scheduler
"""
import json
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.order_service import OrderService
from services.order_assignment_service import OrderAssignmentService
from services.restaurant_service import RestaurantService
from models.order import Order

logger = Logger(service="order-assignment-delay-handler")


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Assign order after delay if still eligible.
    Input: { "orderId": "...", "restaurantId": "..." }
    """
    try:
        order_id = event.get("orderId")
        restaurant_id = event.get("restaurantId")

        if not order_id:
            return {"statusCode": 400, "body": json.dumps({"error": "orderId required"})}

        logger.info(f"[orderId={order_id}] Delay handler triggered restaurantId={restaurant_id}")
        order = OrderService.get_order(order_id)
        if not order:
            logger.warning(f"[orderId={order_id}] Order not found")
            return {"statusCode": 200, "body": json.dumps({"message": "Order not found"})}

        if order.rider_id:
            logger.info(f"[orderId={order_id}] Rider already assigned, skipping")
            return {"statusCode": 200, "body": json.dumps({"message": "Already assigned"})}

        if order.status not in [Order.STATUS_PREPARING]:
            logger.info(f"[orderId={order_id}] Status {order.status} not eligible, skipping")
            return {"statusCode": 200, "body": json.dumps({"message": "Not eligible"})}

        logger.info(f"[orderId={order_id}] Proceeding with delayed assignment status={order.status}")
        restaurant = None
        if restaurant_id:
            restaurant = RestaurantService.get_restaurant_by_id(restaurant_id)
        if not restaurant and order.restaurant_id:
            restaurant = RestaurantService.get_restaurant_by_id(order.restaurant_id)

        if not restaurant:
            logger.error(f"[orderId={order_id}] Restaurant not found")
            return {"statusCode": 500, "body": json.dumps({"error": "Restaurant not found"})}

        logger.info(f"[orderId={order_id}] Restaurant location=({restaurant.latitude},{restaurant.longitude})")
        rider_id = OrderAssignmentService.assign_order_to_rider(
            order_id,
            restaurant.latitude,
            restaurant.longitude
        )

        if rider_id:
            logger.info(f"[orderId={order_id}] Assigned to rider {rider_id}")
        else:
            logger.warning(f"[orderId={order_id}] No available riders found")

        return {"statusCode": 200, "body": json.dumps({"message": "Assignment attempted"})}
    except Exception as e:
        logger.error(f"[orderId={event.get('orderId')}] Error in delayed assignment: {str(e)}", exc_info=True)
        raise
