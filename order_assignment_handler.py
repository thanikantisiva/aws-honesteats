"""
Lambda function to handle DynamoDB Stream events from OrdersTable
Automatically assigns orders to nearest available riders when status = PREPARING
"""
import json
import os
import boto3
from datetime import datetime, timedelta
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.order_assignment_service import OrderAssignmentService
from services.restaurant_service import RestaurantService

logger = Logger(service="order-assignment-handler")


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Process DynamoDB Stream events for PREPARING orders
    Automatically assigns to nearest available rider
    
    Args:
        event: DynamoDB Stream event
        context: Lambda context
        
    Returns:
        Processing result
    """
    logger.info(f"Processing {len(event.get('Records', []))} DynamoDB Stream records")
    
    processed = 0
    errors = 0
    no_riders = 0
    
    for record in event.get('Records', []):
        try:
            # Only process MODIFY events (status updates)
            if record['eventName'] != 'MODIFY':
                logger.info(f"Skipping {record['eventName']} event")
                continue
            
            old_image = record['dynamodb'].get('OldImage', {})
            new_image = record['dynamodb'].get('NewImage', {})
            
            # Check if status changed to PREPARING or READY_FOR_PICKUP
            old_status = old_image.get('status', {}).get('S', '')
            new_status = new_image.get('status', {}).get('S', '')
            
            if new_status not in ['PREPARING', 'READY_FOR_PICKUP']:
                logger.info(f"Status is {new_status}, not PREPARING/READY_FOR_PICKUP - skipping")
                continue
            
            if old_status == new_status:
                logger.info("Status unchanged, skipping assignment")
                continue
            
            # Extract order details
            order_id = new_image.get('orderId', {}).get('S', '')
            restaurant_id = new_image.get('restaurantId', {}).get('S', '')
            customer_phone = new_image.get('customerPhone', {}).get('S', '')
            
            logger.info(f"[orderId={order_id}] 🍽️ Order is preparing")
            logger.info(f"[orderId={order_id}] Restaurant: {restaurant_id}")
            logger.info(f"[orderId={order_id}] Customer: {customer_phone}")
            logger.info(f"[orderId={order_id}] Status: {old_status} → {new_status}")
            
            # Check if already assigned (avoid double assignment)
            if new_image.get('riderId', {}).get('S'):
                logger.info(f"[orderId={order_id}] Order already has rider assigned, skipping")
                continue
            
            # Get restaurant details by restaurantId using GSI
            logger.info(f"[orderId={order_id}] 📍 Fetching restaurant details using restaurantId-index GSI")
            restaurant = RestaurantService.get_restaurant_by_id(restaurant_id)
            
            if not restaurant:
                logger.error(f"[orderId={order_id}] ❌ Restaurant not found: {restaurant_id}")
                errors += 1
                continue
            
            logger.info(f"[orderId={order_id}] ✅ Restaurant found: {restaurant.name}")
            logger.info(f"[orderId={order_id}] Location: ({restaurant.latitude}, {restaurant.longitude})")
            logger.info(f"[orderId={order_id}] Geohash: {restaurant.geohash}")
            
            if new_status == 'PREPARING':
                # Schedule delayed assignment
                try:
                    scheduler = boto3.client('scheduler')
                    delay_seconds = int(os.environ.get('ASSIGNMENT_DELAY_SECONDS', '300'))
                    run_at = datetime.utcnow() + timedelta(seconds=delay_seconds)

                    checker_arn = os.environ.get('ORDER_ASSIGNMENT_DELAY_HANDLER_ARN')
                    checker_role_arn = os.environ.get('ORDER_ASSIGNMENT_DELAY_HANDLER_ROLE_ARN')

                    if checker_arn and checker_role_arn:
                        schedule_name = f"order-assign-delay-{order_id}"
                        logger.info(f"[orderId={order_id}] Scheduling delayed assignment name={schedule_name} runAt={run_at.isoformat()} delaySeconds={delay_seconds}")
                        scheduler.create_schedule(
                            Name=schedule_name,
                            ScheduleExpression=f"at({run_at.strftime('%Y-%m-%dT%H:%M:%S')})",
                            FlexibleTimeWindow={"Mode": "OFF"},
                            Target={
                                "Arn": checker_arn,
                                "RoleArn": checker_role_arn,
                                "Input": json.dumps({
                                    "orderId": order_id,
                                    "restaurantId": restaurant_id
                                })
                            },
                            ActionAfterCompletion="DELETE"
                        )
                        logger.info(f"[orderId={order_id}] Scheduled assignment at {run_at.isoformat()} name={schedule_name}")
                    else:
                        logger.error(f"[orderId={order_id}] Assignment delay handler ARNs not configured")
                except Exception as e:
                    logger.error(f"[orderId={order_id}] Failed to schedule delayed assignment: {str(e)}")
                continue

            # READY_FOR_PICKUP: Assign immediately
            logger.info(f"[orderId={order_id}] 🔍 Searching for available riders using geohash-based GSI queries")
            rider_id = OrderAssignmentService.assign_order_to_rider(
                order_id,
                restaurant.latitude,
                restaurant.longitude
            )

            if rider_id:
                processed += 1
                logger.info(f"[orderId={order_id}] ✅ Assigned to rider {rider_id}")
            else:
                no_riders += 1
                logger.warning(f"[orderId={order_id}] ⚠️ No available riders found")
                logger.warning(f"[orderId={order_id}] Restaurant: {restaurant.name} at ({restaurant.latitude}, {restaurant.longitude})")
                logger.warning(f"[orderId={order_id}] Manual assignment may be required")
                
        except Exception as e:
            errors += 1
            logger.error(f"❌ Error processing record: {str(e)}", exc_info=True)
    
    logger.info(f"Stream processing complete: {processed} assigned, {no_riders} no riders, {errors} errors")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed,
            'no_riders': no_riders,
            'errors': errors
        })
    }
