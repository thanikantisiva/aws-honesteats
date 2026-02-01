"""Order assignment service for automatic rider allocation"""
from typing import Optional
from aws_lambda_powertools import Logger
from services.rider_service import RiderService
from services.order_service import OrderService
from services.notification_service import NotificationService
from models.order import Order
from datetime import datetime
import random
import json
import os
import boto3

logger = Logger()


class OrderAssignmentService:
    """Service for assigning orders to riders"""
    
    @staticmethod
    def assign_order_to_rider(order_id: str, restaurant_lat: float, restaurant_lng: float) -> Optional[str]:
        """
        Auto-assign order to nearest available rider
        
        Algorithm:
        1. Find online riders within 5km of restaurant
        2. Filter riders not currently working on an order
        3. Sort by distance
        4. Assign to nearest rider
        5. Send FCM notification
        6. Generate delivery OTP
        
        Returns:
            rider_id if assigned, None if no riders available
        """
        try:
            logger.info(f"Assigning order {order_id} to rider near ({restaurant_lat}, {restaurant_lng})")
            
            # Find available riders within 5km
            available_riders = RiderService.find_available_riders_near(
                restaurant_lat,
                restaurant_lng,
                radius_km=5
            )
            
            if not available_riders:
                logger.warning(f"No available riders found for order {order_id}")
                
                # Get current order to check assignment attempts
                order = OrderService.get_order(order_id)
                attempts = (order.rider_assignment_attempts if order and hasattr(order, 'rider_assignment_attempts') else 0) + 1
                
                # Update order status to AWAITING_RIDER_ASSIGNMENT
                OrderService.update_order(order_id, {
                    'status': 'AWAITING_RIDER_ASSIGNMENT',
                    'riderAssignmentAttempts': attempts,
                    'lastAssignmentAttemptAt': datetime.utcnow().isoformat()
                })
                logger.info(f"Order {order_id} status updated to AWAITING_RIDER_ASSIGNMENT (attempt #{attempts})")
                
                # Push to SQS queue for retry
                try:
                    sqs = boto3.client('sqs')
                    queue_url = os.environ.get('ORDER_ASSIGNMENT_QUEUE_URL')
                    
                    if queue_url:
                        sqs.send_message(
                            QueueUrl=queue_url,
                            MessageBody=json.dumps({
                                'orderId': order_id,
                                'restaurantLat': restaurant_lat,
                                'restaurantLng': restaurant_lng,
                                'attemptNumber': attempts
                            })
                        )
                        logger.info(f"Order {order_id} queued for rider assignment retry")
                    else:
                        logger.error("ORDER_ASSIGNMENT_QUEUE_URL not configured")
                except Exception as e:
                    logger.error(f"Failed to queue order {order_id}: {str(e)}")
                
                return None
            
            # Get nearest rider (returns list of tuples: (Rider, distance))
            nearest_rider, distance = available_riders[0]
            logger.info(f"Assigning order {order_id} to rider {nearest_rider.rider_id} ({distance:.2f}km away)")
            
            # Generate delivery OTP
            delivery_otp = OrderAssignmentService.generate_delivery_otp()
            
            # Update order with rider assignment, status, and rider's current location
            OrderService.update_order(order_id, {
                'riderId': nearest_rider.rider_id,
                'deliveryOtp': delivery_otp,
                'riderAssignedAt': datetime.utcnow().isoformat(),
                'status': 'RIDER_ASSIGNED',
                # Copy rider's current location for initial tracking
                'riderCurrentLat': nearest_rider.lat,
                'riderCurrentLng': nearest_rider.lng,
                'riderSpeed': nearest_rider.speed,
                'riderHeading': nearest_rider.heading,
                'riderLocationUpdatedAt': datetime.utcnow().isoformat()
            })
            
            # Mark rider as working on order
            RiderService.set_working_on_order(nearest_rider.rider_id, order_id)
            
            # Send notification to rider
            try:
                # Get order details
                order = OrderService.get_order(order_id)
                if order:
                    NotificationService.send_order_assigned_notification(
                        rider_mobile=nearest_rider.phone,
                        order_id=order_id,
                        restaurant_name=order.restaurant_name or "Restaurant",
                        delivery_fee=order.delivery_fee
                    )
            except Exception as e:
                logger.error(f"Failed to send notification to rider: {str(e)}")
            
            logger.info(f"Order {order_id} assigned to rider {nearest_rider.rider_id}")
            return nearest_rider.rider_id
            
        except Exception as e:
            logger.error(f"Error assigning order: {str(e)}", exc_info=True)
            return None
    
    @staticmethod
    def reassign_order(order_id: str) -> Optional[str]:
        """Reassign order to another rider (if rejected or timeout)"""
        try:
            order = OrderService.get_order(order_id)
            if not order:
                return None
            
            # Clear current rider assignment
            if order.rider_id:
                RiderService.set_working_on_order(order.rider_id, None)
            
            OrderService.update_order(order_id, {
                'riderId': None
            })
            
            # Try to assign to another rider
            if order.pickup_lat and order.pickup_lng:
                return OrderAssignmentService.assign_order_to_rider(
                    order_id,
                    order.pickup_lat,
                    order.pickup_lng
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Error reassigning order: {str(e)}", exc_info=True)
            return None
    
    @staticmethod
    def generate_delivery_otp() -> str:
        """Generate 4-digit delivery OTP"""
        return str(random.randint(1000, 9999))
