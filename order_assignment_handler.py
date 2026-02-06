"""
Lambda function to handle DynamoDB Stream events from OrdersTable
Automatically assigns orders to nearest available riders when status = PREPARING
"""
import json
import os
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
            
            # Check if status changed to PREPARING
            old_status = old_image.get('status', {}).get('S', '')
            new_status = new_image.get('status', {}).get('S', '')
            
            if new_status != 'PREPARING':
                logger.info(f"Status is {new_status}, not PREPARING - skipping")
                continue
            
            if old_status == new_status:
                logger.info("Status unchanged, skipping assignment")
                continue
            
            # Extract order details
            order_id = new_image.get('orderId', {}).get('S', '')
            restaurant_id = new_image.get('restaurantId', {}).get('S', '')
            customer_phone = new_image.get('customerPhone', {}).get('S', '')
            
            logger.info(f"🍽️ Order is preparing : {order_id}")
            logger.info(f"   Restaurant: {restaurant_id}")
            logger.info(f"   Customer: {customer_phone}")
            logger.info(f"   Status: {old_status} → {new_status}")
            
            # Check if already assigned (avoid double assignment)
            if new_image.get('riderId', {}).get('S'):
                logger.info(f"Order already has rider assigned, skipping")
                continue
            
            # Get restaurant details by restaurantId using GSI
            logger.info(f"📍 Fetching restaurant details using restaurantId-index GSI")
            restaurant = RestaurantService.get_restaurant_by_id(restaurant_id)
            
            if not restaurant:
                logger.error(f"❌ Restaurant not found: {restaurant_id}")
                errors += 1
                continue
            
            logger.info(f"✅ Restaurant found: {restaurant.name}")
            logger.info(f"   Location: ({restaurant.latitude}, {restaurant.longitude})")
            logger.info(f"   Geohash: {restaurant.geohash}")
            
            # Assign order to nearest available rider using geohash proximity search
            logger.info(f"🔍 Searching for available riders using geohash-based GSI queries")
            rider_id = OrderAssignmentService.assign_order_to_rider(
                order_id,
                restaurant.latitude,
                restaurant.longitude
            )
            
            if rider_id:
                processed += 1
                logger.info(f"✅ Order {order_id} assigned to rider {rider_id}")
            else:
                no_riders += 1
                logger.warning(f"⚠️ No available riders found for order {order_id}")
                logger.warning(f"   Restaurant: {restaurant.name} at ({restaurant.latitude}, {restaurant.longitude})")
                logger.warning(f"   Manual assignment may be required")
                
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
