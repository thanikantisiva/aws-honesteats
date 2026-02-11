"""
Lambda to process queued orders awaiting rider assignment
Triggered by SQS event source mapping from OrderAssignmentQueue
"""
import json
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.order_assignment_service import OrderAssignmentService
from services.order_service import OrderService

logger = Logger(service="queue-assignment-consumer")


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
                # Still no riders - raise to keep message for retry
                still_waiting += 1
                logger.info(f"[orderId={order_id}] ⏳ Still awaiting rider (will retry after visibility timeout)")
                raise Exception(f"No riders available for order {order_id}")
            
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
