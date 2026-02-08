"""
Lambda function to verify rider acceptance after offer timeout
Triggered by EventBridge Scheduler
"""
import json
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.order_service import OrderService
from services.order_assignment_service import OrderAssignmentService
from services.restaurant_service import RestaurantService
from models.order import Order

logger = Logger(service="order-accept-reject-checker")


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Check if order moved to RIDER_ASSIGNED after offer; if not, mark rider rejected
    and reassign to next available rider.
    
    Expected event input:
    {
      "orderId": "...",
      "riderId": "..."
    }
    """
    try:
        order_id = event.get("orderId")
        expected_rider_id = event.get("riderId")

        if not order_id:
            return {"statusCode": 400, "body": json.dumps({"error": "orderId required"})}

        order = OrderService.get_order(order_id)
        if not order:
            logger.warning(f"Order not found: {order_id}")
            return {"statusCode": 200, "body": json.dumps({"message": "Order not found"})}

        if order.status == Order.RIDER_ASSIGNED:
            logger.info(f"Order {order_id} already accepted by rider")
            return {"statusCode": 200, "body": json.dumps({"message": "Already assigned"})}

        # If status moved beyond assigned or cancelled, do nothing
        if order.status in [Order.PICKED_UP, Order.STATUS_OUT_FOR_DELIVERY, Order.STATUS_DELIVERED, Order.STATUS_CANCELLED]:
            logger.info(f"Order {order_id} in terminal status {order.status}, skipping")
            return {"statusCode": 200, "body": json.dumps({"message": "Terminal status"})}

        # If rider has changed since offer, skip (stale schedule)
        if expected_rider_id and order.rider_id and order.rider_id != expected_rider_id:
            logger.info(f"Order {order_id} rider changed, skipping stale schedule")
            return {"statusCode": 200, "body": json.dumps({"message": "Stale schedule"})}

        # Add current rider to rejectedByRiders list
        rejected = order.rejected_by_riders or []
        if order.rider_id and order.rider_id not in rejected:
            rejected.append(order.rider_id)

        OrderService.update_order(order_id, {
            "rejectedByRiders": rejected,
            "riderId": None,
            "status": Order.STATUS_AWAITING_RIDER_ASSIGNMENT
        })

        # Reassign to next available rider
        restaurant_lat = order.pickup_lat
        restaurant_lng = order.pickup_lng
        if restaurant_lat is None or restaurant_lng is None:
            restaurant = RestaurantService.get_restaurant_by_id(order.restaurant_id)
            if restaurant:
                restaurant_lat = restaurant.latitude
                restaurant_lng = restaurant.longitude

        if restaurant_lat is None or restaurant_lng is None:
            logger.error(f"Missing restaurant location for order {order_id}")
            return {"statusCode": 500, "body": json.dumps({"error": "Missing restaurant location"})}

        OrderAssignmentService.assign_order_to_rider(order_id, restaurant_lat, restaurant_lng)

        return {"statusCode": 200, "body": json.dumps({"message": "Reassignment attempted"})}
    except Exception as e:
        logger.error(f"Error in OrderAcceptRejectChecker: {str(e)}", exc_info=True)
        raise
