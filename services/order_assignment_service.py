"""Order assignment service for automatic rider allocation"""
from typing import Optional
from aws_lambda_powertools import Logger
from services.rider_service import RiderService
from services.order_service import OrderService
from services.notification_service import NotificationService
from models.order import Order
from datetime import datetime, timedelta
import random
import json
import os
import boto3
import time

logger = Logger()


class OrderAssignmentService:
    """Service for assigning orders to riders"""
    
    @staticmethod
    def assign_order_to_rider(order_id: str, restaurant_lat: float, restaurant_lng: float) -> Optional[str]:
        """
        Offer order to nearest available rider
        
        Algorithm:
        1. Find online riders within 5km of restaurant
        2. Filter riders not currently working on an order
        3. Sort by distance
        4. Offer to nearest rider (status OFFERED_TO_RIDER)
        5. Send FCM notification to rider
        6. Emit EventBridge event to check acceptance
        
        Returns:
            rider_id if assigned, None if no riders available
        """
        try:
            logger.info(f"[orderId={order_id}] Offering to rider near ({restaurant_lat}, {restaurant_lng})")
            
            # Find available riders within 5km
            available_riders = RiderService.find_available_riders_near(
                restaurant_lat,
                restaurant_lng,
                radius_km=10
            )
            logger.info(f"[orderId={order_id}] Found {len(available_riders)} available riders before filtering")

            # Load order to check rejected riders
            order = OrderService.get_order(order_id)
            rejected_by_riders = order.rejected_by_riders if order else []
            if rejected_by_riders:
                logger.info(f"[orderId={order_id}] rejectedByRiders count: {len(rejected_by_riders)}")
            filtered_riders = available_riders
            if rejected_by_riders:
                filtered_riders = [
                    (r, d) for (r, d) in available_riders
                    if r.rider_id not in rejected_by_riders
                ]
            logger.info(f"[orderId={order_id}] {len(filtered_riders)} riders after filtering rejected riders")
            
            if not available_riders:
                logger.warning(f"[orderId={order_id}] No available riders found")
                
                # Get current order to check assignment attempts
                order = OrderService.get_order(order_id)
                attempts = (order.rider_assignment_attempts if order and hasattr(order, 'rider_assignment_attempts') else 0) + 1
                
                # Update order status to AWAITING_RIDER_ASSIGNMENT
                OrderService.update_order(order_id, {
                    'status': 'AWAITING_RIDER_ASSIGNMENT',
                    'riderAssignmentAttempts': attempts,
                    'lastAssignmentAttemptAt': datetime.utcnow().isoformat()
                })
                logger.info(f"[orderId={order_id}] Status updated to AWAITING_RIDER_ASSIGNMENT (attempt #{attempts})")
                
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
                        logger.info(f"[orderId={order_id}] Queued for rider assignment retry")
                    else:
                        logger.error(f"[orderId={order_id}] ORDER_ASSIGNMENT_QUEUE_URL not configured")
                except Exception as e:
                    logger.error(f"[orderId={order_id}] Failed to queue: {str(e)}")
                
                return None
            
            # If everyone has rejected, assign directly to nearest rider
            if not filtered_riders and available_riders:
                nearest_rider, distance = available_riders[0]
                logger.info(f"[orderId={order_id}] All riders rejected; direct assign to {nearest_rider.rider_id} ({distance:.2f}km)")
                logger.info(f"[orderId={order_id}] Direct assignment rider: id={nearest_rider.rider_id} phone={nearest_rider.phone} lat={nearest_rider.lat} lng={nearest_rider.lng}")

                delivery_otp = OrderAssignmentService.generate_delivery_otp()
                pickup_otp = OrderAssignmentService.generate_delivery_otp()
                OrderService.update_order(order_id, {
                    'riderId': nearest_rider.rider_id,
                    'deliveryOtp': delivery_otp,
                    'pickupOtp': pickup_otp,
                    'riderAssignedAt': datetime.utcnow().isoformat(),
                    'status': Order.RIDER_ASSIGNED,
                    # Copy rider's current location for initial tracking
                    'riderCurrentLat': nearest_rider.lat,
                    'riderCurrentLng': nearest_rider.lng,
                    'riderSpeed': nearest_rider.speed,
                    'riderHeading': nearest_rider.heading,
                    'riderLocationUpdatedAt': datetime.utcnow().isoformat()
                })

                RiderService.set_working_on_order(nearest_rider.rider_id, order_id)

                try:
                    order = OrderService.get_order(order_id)
                    if order:
                        NotificationService.send_order_assigned_notification(
                            rider_mobile=nearest_rider.phone,
                            order_id=order_id,
                            restaurant_name=order.restaurant_name or "Restaurant",
                            delivery_fee=order.delivery_fee
                        )
                        logger.info(f"[orderId={order_id}] Direct assignment notification sent to rider {nearest_rider.phone}")
                except Exception as e:
                    logger.error(f"Failed to send notification to rider: {str(e)}")

                logger.info(f"[orderId={order_id}] Assigned directly to rider {nearest_rider.rider_id}")
                return nearest_rider.rider_id

            # Get nearest rider from filtered list (returns list of tuples: (Rider, distance))
            nearest_rider, distance = filtered_riders[0]
            logger.info(f"[orderId={order_id}] Offering to rider {nearest_rider.rider_id} ({distance:.2f}km)")
            logger.info(f"[orderId={order_id}] Offer rider: id={nearest_rider.rider_id} phone={nearest_rider.phone} lat={nearest_rider.lat} lng={nearest_rider.lng}")
            
            # Update order with offered rider and status
            OrderService.update_order(order_id, {
                'riderId': nearest_rider.rider_id,
                'status': Order.OFFERED_TO_RIDER,
                'offeredAt': datetime.utcnow().isoformat()
            })
            logger.info(f"[orderId={order_id}] Updated to OFFERED_TO_RIDER for rider {nearest_rider.rider_id}")
            
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
                    logger.info(f"[orderId={order_id}] Offer notification sent to rider {nearest_rider.phone}")
            except Exception as e:
                logger.error(f"Failed to send notification to rider: {str(e)}")
            
            # Create EventBridge Scheduler one-time schedule to check acceptance
            try:
                scheduler = boto3.client('scheduler')
                checker_arn = os.environ.get('ORDER_ACCEPT_REJECT_CHECKER_ARN')
                checker_role_arn = os.environ.get('ORDER_ACCEPT_REJECT_CHECKER_ROLE_ARN')
                delay_seconds = int(os.environ.get('OFFER_CHECK_DELAY_SECONDS', '90'))

                if checker_arn and checker_role_arn:
                    run_at = datetime.utcnow() + timedelta(seconds=delay_seconds)
                    schedule_name = f"order-accept-check-{order_id}"
                    scheduler.create_schedule(
                        Name=schedule_name,
                        ScheduleExpression=f"at({run_at.strftime('%Y-%m-%dT%H:%M:%S')})",
                        FlexibleTimeWindow={"Mode": "OFF"},
                        Target={
                            "Arn": checker_arn,
                            "RoleArn": checker_role_arn,
                            "Input": json.dumps({
                                "orderId": order_id,
                                "riderId": nearest_rider.rider_id
                            })
                        },
                        ActionAfterCompletion="DELETE"
                    )
                    logger.info(f"[orderId={order_id}] Offer check scheduled at {run_at.isoformat()} name={schedule_name}")
                else:
                    logger.error(f"[orderId={order_id}] Order accept/reject checker ARNs not configured")
            except Exception as e:
                logger.error(f"[orderId={order_id}] Failed to create EventBridge schedule: {str(e)}")
            
            logger.info(f"[orderId={order_id}] Offered to rider {nearest_rider.rider_id}")
            return nearest_rider.rider_id
            
        except Exception as e:
            logger.error(f"[orderId={order_id}] Error assigning order: {str(e)}", exc_info=True)
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
