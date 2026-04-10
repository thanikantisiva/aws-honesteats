"""
Lambda to process queued orders awaiting rider assignment
Triggered by SQS event source mapping from OrderAssignmentQueue
"""
import json
import os
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.order_assignment_service import OrderAssignmentService
from services.order_service import OrderService
from services.sns_alert_service import publish_order_alert

logger = Logger(service="queue-assignment-consumer")

# After this many attempts without finding a rider the message is removed from
# the queue (it stays in AWAITING_RIDER_ASSIGNMENT for manual admin resolution).
MAX_ASSIGNMENT_ATTEMPTS = int(os.environ.get('MAX_ASSIGNMENT_ATTEMPTS', '10'))


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Process queued orders awaiting rider assignment
    Handles SQS event records and attempts to assign riders
    
    Args:
        event: SQS event
        context: Lambda context
        
    Returns:
        Processing results
    """
    processed = 0
    assigned = 0
    still_waiting = 0
    
    records = event.get('Records', [])
    logger.info(f"Received {len(records)} messages from queue")
    
    for record in records:
        try:
            body = json.loads(record['body'])
            order_id = body['orderId']
            restaurant_lat = body['restaurantLat']
            restaurant_lng = body['restaurantLng']
            attempt_number = body.get('attemptNumber', 0)
            
            logger.info(f"[orderId={order_id}] Processing (attempt #{attempt_number})")
            
            # Hard cap: give up after MAX_ASSIGNMENT_ATTEMPTS so the message
            # doesn't loop forever. The order stays in AWAITING_RIDER_ASSIGNMENT
            # for manual admin intervention.
            if attempt_number >= MAX_ASSIGNMENT_ATTEMPTS:
                logger.error(
                    f"[orderId={order_id}] ❌ Max assignment attempts ({MAX_ASSIGNMENT_ATTEMPTS}) "
                    f"reached — giving up, manual intervention required"
                )
                # Alert admins via SNS SMS — same channel used for unaccepted restaurant orders
                try:
                    order_for_alert = OrderService.get_order(order_id)
                    restaurant_name = (order_for_alert.restaurant_name or "Restaurant") if order_for_alert else "Restaurant"
                    publish_order_alert(
                        message=(
                            f"🚨 NO RIDER AVAILABLE\n"
                            f"Order: {order_id}\n"
                            f"Restaurant: {restaurant_name}\n"
                            f"Tried {MAX_ASSIGNMENT_ATTEMPTS} times — no riders found.\n"
                            f"Please assign a rider manually."
                        ),
                        subject="No rider available — manual assignment needed",
                    )
                    logger.info(f"[orderId={order_id}] 📲 Admin SNS alert sent (no rider after {MAX_ASSIGNMENT_ATTEMPTS} attempts)")
                except Exception as alert_err:
                    logger.error(f"[orderId={order_id}] Failed to send admin SNS alert: {alert_err}")
                continue

            # Verify order still needs assignment
            order = OrderService.get_order(order_id)
            
            if not order:
                logger.warning(f"[orderId={order_id}] Order not found, skipping")
                continue
            
            if order.status != 'AWAITING_RIDER_ASSIGNMENT':
                logger.info(f"[orderId={order_id}] Status is {order.status}, no longer awaiting assignment")
                continue
            
            # Try to assign rider
            rider_id = OrderAssignmentService.assign_order_to_rider(
                order_id, restaurant_lat, restaurant_lng
            )
            
            if rider_id:
                assigned += 1
                logger.info(f"[orderId={order_id}] ✅ Assigned to rider {rider_id}")
            else:
                # Still no riders — re-raise so SQS retries after visibility timeout
                still_waiting += 1
                logger.warning(
                    f"[orderId={order_id}] ⏳ No riders available "
                    f"(attempt #{attempt_number}/{MAX_ASSIGNMENT_ATTEMPTS}) — will retry"
                )
                raise Exception(f"No riders available for order {order_id} (attempt {attempt_number})")
            
            processed += 1
            
        except Exception as e:
            logger.error(f"[orderId={order_id}] Error processing message: {str(e)}", exc_info=True)
            raise
    
    logger.info(f"Queue processing complete: {processed} processed, {assigned} assigned, {still_waiting} still waiting")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed,
            'assigned': assigned,
            'still_waiting': still_waiting
        })
    }
