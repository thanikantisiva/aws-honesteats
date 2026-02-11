"""
Lambda function to handle DynamoDB Stream events from OrdersTable
Sends push notifications when order status changes
"""
import json
import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.notification_service import NotificationService

logger = Logger(service="notification-handler")
dynamodb_client = boto3.client('dynamodb')


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Process DynamoDB Stream events for order status changes
    
    Args:
        event: DynamoDB Stream event
        context: Lambda context
        
    Returns:
        Processing result
    """
    logger.info(f"Processing {len(event.get('Records', []))} DynamoDB Stream records")
    
    processed = 0
    errors = 0
    
    for record in event.get('Records', []):
        try:
            # Only process MODIFY events (status updates)
            if record['eventName'] != 'MODIFY':
                continue
            
            old_image = record['dynamodb'].get('OldImage', {})
            new_image = record['dynamodb'].get('NewImage', {})
            
            # Check if status changed
            old_status = old_image.get('status', {}).get('S', '')
            new_status = new_image.get('status', {}).get('S', '')
            
            if old_status == new_status:
                logger.info("Status unchanged, skipping notification")
                continue
            
            # Extract order details
            order_id = new_image.get('orderId', {}).get('S', '')
            customer_phone = new_image.get('customerPhone', {}).get('S', '')
            restaurant_name = new_image.get('restaurantName', {}).get('S', 'Restaurant')
            
            logger.info(f"[orderId={order_id}] 📦 Order status changed")
            logger.info(f"[orderId={order_id}] Customer: {customer_phone}")
            logger.info(f"[orderId={order_id}] Status: {old_status} → {new_status}")
            logger.info(f"[orderId={order_id}] Restaurant: {restaurant_name}")

            if new_status == "OFFERED_TO_RIDER":
                logger.info(f"[orderId={order_id}] Skipping customer notification for OFFERED_TO_RIDER")
                continue
            
            # Get user's FCM token from UsersTable
            # UsersTableV2 has composite key: phone (HASH) + role (RANGE)
            users_table = get_users_table_name()
            user_response = dynamodb_client.get_item(
                TableName=users_table,
                Key={
                    'phone': {'S': customer_phone},
                    'role': {'S': 'CUSTOMER'}
                }
            )
            
            if 'Item' not in user_response:
                logger.warning(f"[orderId={order_id}] User not found: {customer_phone}")
                continue
            
            fcm_token = user_response['Item'].get('fcmToken', {}).get('S')
            
            if not fcm_token:
                logger.warning(f"[orderId={order_id}] No FCM token for user: {customer_phone}")
                continue
            
            # Send push notification
            success = NotificationService.send_order_status_notification(
                fcm_token=fcm_token,
                order_id=order_id,
                status=new_status,
                restaurant_name=restaurant_name
            )
            
            if success:
                processed += 1
                logger.info(f"[orderId={order_id}] ✅ Notification sent successfully")
            else:
                errors += 1
                logger.error(f"[orderId={order_id}] ❌ Failed to send notification")
                
        except Exception as e:
            errors += 1
            logger.error(f"[orderId={order_id}] Error processing record: {str(e)}", exc_info=True)
    
    logger.info(f"Stream processing complete: {processed} sent, {errors} errors")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed,
            'errors': errors
        })
    }


def get_users_table_name():
    """Get UsersTable name from environment"""
    import os
    env = os.environ.get('ENVIRONMENT', 'dev')
    return f'food-delivery-users-{env}'
