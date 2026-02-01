"""
Lambda to process queued orders awaiting rider assignment
Triggered by EventBridge schedule every 2 minutes
Polls SQS queue for orders that couldn't be assigned initially
"""
import json
import os
import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.order_assignment_service import OrderAssignmentService
from services.order_service import OrderService

logger = Logger(service="queue-assignment-consumer")
sqs = boto3.client('sqs')


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Process queued orders awaiting rider assignment
    Pulls messages from SQS and attempts to assign riders
    
    Args:
        event: EventBridge scheduled event
        context: Lambda context
        
    Returns:
        Processing results
    """
    queue_url = os.environ.get('ORDER_ASSIGNMENT_QUEUE_URL')
    
    if not queue_url:
        logger.error("ORDER_ASSIGNMENT_QUEUE_URL not configured")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Queue not configured'})
        }
    
    processed = 0
    assigned = 0
    still_waiting = 0
    
    logger.info("Starting queue processing for awaiting orders")
    
    # Pull up to 10 messages from queue
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10  # Long polling
        )
    except Exception as e:
        logger.error(f"Failed to receive messages from queue: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to receive messages'})
        }
    
    messages = response.get('Messages', [])
    logger.info(f"Received {len(messages)} messages from queue")
    
    for message in messages:
        try:
            body = json.loads(message['Body'])
            order_id = body['orderId']
            restaurant_lat = body['restaurantLat']
            restaurant_lng = body['restaurantLng']
            attempt_number = body.get('attemptNumber', 0)
            
            logger.info(f"Processing order {order_id} (attempt #{attempt_number})")
            
            # Verify order still needs assignment
            order = OrderService.get_order(order_id)
            
            if not order:
                logger.warning(f"Order {order_id} not found, removing from queue")
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                continue
            
            if order.status != 'AWAITING_RIDER_ASSIGNMENT':
                logger.info(f"Order {order_id} status is {order.status}, no longer awaiting assignment")
                # Already assigned or cancelled - remove from queue
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                continue
            
            # Try to assign rider
            rider_id = OrderAssignmentService.assign_order_to_rider(
                order_id, restaurant_lat, restaurant_lng
            )
            
            if rider_id:
                # Success! Remove from queue
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                assigned += 1
                logger.info(f"✅ Queued order {order_id} assigned to rider {rider_id}")
            else:
                # Still no riders - message stays in queue (visibility timeout)
                still_waiting += 1
                logger.info(f"⏳ Order {order_id} still awaiting rider (will retry after visibility timeout)")
            
            processed += 1
            
        except Exception as e:
            logger.error(f"Error processing message for order: {str(e)}", exc_info=True)
    
    logger.info(f"Queue processing complete: {processed} processed, {assigned} assigned, {still_waiting} still waiting")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed,
            'assigned': assigned,
            'still_waiting': still_waiting
        })
    }
