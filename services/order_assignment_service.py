"""Order assignment service for automatic rider allocation"""
from typing import Optional, List, Tuple
from aws_lambda_powertools import Logger
from services.rider_service import RiderService
from services.order_service import OrderService
from services.notification_service import NotificationService
from models.rider import Rider
from models.order import Order
from datetime import datetime, timedelta
import json
import os
import boto3

logger = Logger()

W_FAIRNESS = 0.5
W_RATING = 0.2
W_DISTANCE = 0.3
NEW_RIDER_THRESHOLD = 5


def _compute_rider_score(rider: Rider, distance_km: float) -> float:
    """
    Score a rider for assignment. Higher is better.
    - Fairness: riders with fewer assignments in the last 7 days score higher.
    - Rating: slight bias toward higher-rated riders; new riders get neutral score.
    - Distance: closer riders score higher.
    """
    fairness = 1.0 / (1.0 + (rider.orders_assigned_last_7d or 0))

    if rider.rating is not None and (rider.rated_count or 0) >= NEW_RIDER_THRESHOLD:
        rating_score = max(0.0, min(1.0, (rider.rating - 3.0) / 2.0))
    else:
        rating_score = 0.55

    distance_score = 1.0 / (1.0 + distance_km)

    return W_FAIRNESS * fairness + W_RATING * rating_score + W_DISTANCE * distance_score


def _rank_riders(riders: List[Tuple[Rider, float]]) -> List[Tuple[Rider, float, float]]:
    """Return riders sorted by composite score descending. Each element: (Rider, distance, score)."""
    scored = [(r, d, _compute_rider_score(r, d)) for r, d in riders]
    scored.sort(key=lambda x: x[2], reverse=True)
    return scored


class OrderAssignmentService:
    """Service for assigning orders to riders"""
    
    @staticmethod
    def assign_order_to_rider(order_id: str, restaurant_lat: float, restaurant_lng: float) -> Optional[str]:
        """
        Offer order to best-scoring available rider.
        
        Algorithm:
        1. Find online riders within 10km of restaurant
        2. Filter riders not currently working on an order
        3. Filter riders who have rejected this order
        4. Score each rider (fairness + rating bias + distance)
        5. Offer to best-scoring rider (status OFFERED_TO_RIDER)
        6. Send FCM notification to rider
        7. Schedule EventBridge check for acceptance
        
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
            
            # If everyone has rejected, direct-assign to best-scoring from available_riders
            if not filtered_riders and available_riders:
                ranked = _rank_riders(available_riders)
                nearest_rider, distance, score = ranked[0]
                logger.info(f"[orderId={order_id}] All riders rejected; direct assign to {nearest_rider.rider_id} (score={score:.3f}, dist={distance:.2f}km)")

                OrderService.update_order(order_id, {
                    'riderId': nearest_rider.rider_id,
                    'riderAssignedAt': datetime.utcnow().isoformat(),
                    'status': Order.RIDER_ASSIGNED,
                    'riderCurrentLat': nearest_rider.lat,
                    'riderCurrentLng': nearest_rider.lng,
                    'riderSpeed': nearest_rider.speed,
                    'riderHeading': nearest_rider.heading,
                    'riderLocationUpdatedAt': datetime.utcnow().isoformat()
                })

                RiderService.set_working_on_order(nearest_rider.rider_id, order_id)
                RiderService.increment_assignment_count(nearest_rider.rider_id)

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

            # Score filtered riders and pick the best
            ranked = _rank_riders(filtered_riders)
            best_rider, distance, score = ranked[0]
            logger.info(f"[orderId={order_id}] Offering to rider {best_rider.rider_id} (score={score:.3f}, dist={distance:.2f}km)")
            logger.info(f"[orderId={order_id}] Offer rider: id={best_rider.rider_id} phone={best_rider.phone} lat={best_rider.lat} lng={best_rider.lng}")
            nearest_rider = best_rider
            
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

