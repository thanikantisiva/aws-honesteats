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
from services.rider_service import RiderService
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

        logger.info(f"[orderId={order_id}] Checker triggered riderId={expected_rider_id}")
        order = OrderService.get_order(order_id)
        if not order:
            logger.warning(f"[orderId={order_id}] Order not found")
            return {"statusCode": 200, "body": json.dumps({"message": "Order not found"})}

        if order.status == Order.RIDER_ASSIGNED:
            logger.info(f"[orderId={order_id}] Already accepted by rider")
            return {"statusCode": 200, "body": json.dumps({"message": "Already assigned"})}

        # If status moved beyond assigned or cancelled, do nothing
        if order.status in [Order.PICKED_UP, Order.STATUS_OUT_FOR_DELIVERY, Order.STATUS_DELIVERED, Order.STATUS_CANCELLED]:
            logger.info(f"[orderId={order_id}] Terminal status {order.status}, skipping")
            return {"statusCode": 200, "body": json.dumps({"message": "Terminal status"})}

        # If rider has changed since offer, skip (stale schedule)
        if expected_rider_id and order.rider_id and order.rider_id != expected_rider_id:
            logger.info(f"[orderId={order_id}] Rider changed, skipping stale schedule")
            return {"statusCode": 200, "body": json.dumps({"message": "Stale schedule"})}

        # Add current rider to rejectedByRiders list
        rejected = order.rejected_by_riders or []
        if order.rider_id and order.rider_id not in rejected:
            rejected.append(order.rider_id)

        logger.info(f"[orderId={order_id}] Marking rider rejected riderId={order.rider_id}")
        OrderService.update_order(order_id, {
            "rejectedByRiders": rejected,
            "riderId": None,
            "status": Order.STATUS_AWAITING_RIDER_ASSIGNMENT
        })

        # Remove order from rider's workingOnOrder if set
        if order.rider_id:
            try:
                rider = RiderService.get_rider(order.rider_id)
                current_orders = rider.working_on_order if rider else []
                if order_id in current_orders:
                    current_orders.remove(order_id)
                if current_orders:
                    from utils.dynamodb import dynamodb_client, TABLES
                    logger.info(f"[orderId={order_id}] Updating workingOnOrder list for riderId={order.rider_id}")
                    dynamodb_client.update_item(
                        TableName=TABLES['RIDERS'],
                        Key={'riderId': {'S': order.rider_id}},
                        UpdateExpression="SET workingOnOrder = :orderIds",
                        ExpressionAttributeValues={
                            ':orderIds': {'L': [{'S': str(v)} for v in current_orders]}
                        }
                    )
                else:
                    from utils.dynamodb import dynamodb_client, TABLES
                    logger.info(f"[orderId={order_id}] Clearing workingOnOrder for riderId={order.rider_id}")
                    dynamodb_client.update_item(
                        TableName=TABLES['RIDERS'],
                        Key={'riderId': {'S': order.rider_id}},
                        UpdateExpression="REMOVE workingOnOrder"
                    )
            except Exception as e:
                logger.error(f"[orderId={order_id}] Failed to update workingOnOrder for riderId={order.rider_id}: {str(e)}")
        

        # Reassign to next available rider
        restaurant_lat = order.pickup_lat
        restaurant_lng = order.pickup_lng
        if restaurant_lat is None or restaurant_lng is None:
            restaurant = RestaurantService.get_restaurant_by_id(order.restaurant_id)
            if restaurant:
                restaurant_lat = restaurant.latitude
                restaurant_lng = restaurant.longitude

        if restaurant_lat is None or restaurant_lng is None:
            logger.error(f"[orderId={order_id}] Missing restaurant location")
            return {"statusCode": 500, "body": json.dumps({"error": "Missing restaurant location"})}

        logger.info(f"[orderId={order_id}] Reassigning order after timeout")
        OrderAssignmentService.assign_order_to_rider(order_id, restaurant_lat, restaurant_lng)

        return {"statusCode": 200, "body": json.dumps({"message": "Reassignment attempted"})}
    except Exception as e:
        logger.error(f"[orderId={event.get('orderId')}] Error in OrderAcceptRejectChecker: {str(e)}", exc_info=True)
        raise
