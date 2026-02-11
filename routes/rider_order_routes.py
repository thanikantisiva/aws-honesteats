"""Rider order management routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.order_service import OrderService
from services.order_assignment_service import OrderAssignmentService
from services.rider_service import RiderService
from services.earnings_service import EarningsService
from services.restaurant_earnings_service import RestaurantEarningsService
from services.restaurant_service import RestaurantService
from services.address_service import AddressService
from models.order import Order
from datetime import datetime
import random

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_rider_order_routes(app):
    """Register rider order management routes"""
    
    @app.get("/api/v1/riders/<rider_id>/orders")
    @tracer.capture_method
    def get_rider_orders(rider_id: str):
        """
        Get orders assigned to rider with enriched restaurant and address data
        
        Query params:
        - status: Filter by status (optional)
        """
        try:
            query_params = app.current_event.query_string_parameters or {}
            status_filter = query_params.get('status')
            
            logger.info(f"Getting orders for rider: {rider_id}, status: {status_filter}")
            
            # Get orders for this rider with optional status filter (DB-level filtering)
            orders = OrderService.get_orders_by_rider(rider_id, status=status_filter)
            
            # Orders are already sorted by createdAt descending from the query
            
            # Enrich orders with restaurant and address data
            enriched_orders = []
            for order in orders:
                order_dict = order.to_dict()
                
                # Fetch restaurant data
                if order.restaurant_id:
                    try:
                        restaurant = RestaurantService.get_restaurant_by_id(order.restaurant_id)
                        if restaurant:
                            order_dict['pickupAddress'] = f"{restaurant.name}, {restaurant.location_id}"
                            order_dict['pickupLat'] = restaurant.latitude
                            order_dict['pickupLng'] = restaurant.longitude
                            logger.info(f"Enriched order {order.order_id} with restaurant: {restaurant.name} at ({restaurant.latitude}, {restaurant.longitude})")
                    except Exception as e:
                        logger.error(f"Failed to fetch restaurant {order.restaurant_id}: {str(e)}")
                
                # Fetch address data
                if order.address_id and order.customer_phone:
                    try:
                        address = AddressService.get_address(order.customer_phone, order.address_id)
                        if address:
                            order_dict['deliveryLat'] = address.lat
                            order_dict['deliveryLng'] = address.lng
                            logger.info(f"Enriched order {order.order_id} with address: ({address.lat}, {address.lng})")
                    except Exception as e:
                        logger.error(f"Failed to fetch address {order.address_id}: {str(e)}")
                
                enriched_orders.append(order_dict)
            
            metrics.add_metric(name="RiderOrdersRetrieved", unit="Count", value=1)
            
            return {
                "orders": enriched_orders,
                "total": len(enriched_orders)
            }, 200
            
        except Exception as e:
            logger.error("Error getting rider orders", exc_info=True)
            return {"error": "Failed to get orders", "message": str(e)}, 500
    
    @app.post("/api/v1/riders/<rider_id>/orders/<order_id>/accept/<status>")
    @tracer.capture_method
    def accept_order(rider_id: str, order_id: str, status: str):
        """Accept an assigned order"""
        try:
            logger.info(f"[orderId={order_id}] Rider {rider_id} accepting order")
            
            # Get order
            order = OrderService.get_order(order_id)
            if not order:
                return {"error": "Order not found"}, 404
            
            if order.rider_id != rider_id:
                return {"error": "Order not assigned to this rider"}, 403
            
            # Update order status to requested status from path param
            new_status = status or Order.RIDER_ASSIGNED
            OrderService.update_order(order_id, {
                'status': new_status
            })
            
            # Update rider working_on_order
            RiderService.set_working_on_order(rider_id, order_id)
            
            # Get rider's current location and copy to order for tracking
            rider = RiderService.get_rider(rider_id)
            update_data = {
                'riderAssignedAt': datetime.utcnow().isoformat()
            }
            
            # Copy rider location if available and not already in order
            if rider and rider.lat is not None and rider.lng is not None:
                if not order.rider_current_lat or not order.rider_current_lng:
                    update_data['riderCurrentLat'] = rider.lat
                    update_data['riderCurrentLng'] = rider.lng
                    update_data['riderSpeed'] = rider.speed or 0.0
                    update_data['riderHeading'] = rider.heading or 0.0
                    update_data['riderLocationUpdatedAt'] = datetime.utcnow().isoformat()
                    logger.info(f"Copied rider location to order: ({rider.lat}, {rider.lng})")
            
            OrderService.update_order(order_id, update_data)
            
            logger.info(f"[orderId={order_id}] Accepted by rider {rider_id}")
            metrics.add_metric(name="OrderAcceptedByRider", unit="Count", value=1)
            
            return {"message": "Order accepted", "orderId": order_id, "status": new_status}, 200
            
        except Exception as e:
            logger.error("Error accepting order", exc_info=True)
            return {"error": "Failed to accept order", "message": str(e)}, 500
    
    @app.post("/api/v1/riders/<rider_id>/orders/<order_id>/reject")
    @tracer.capture_method
    def reject_order(rider_id: str, order_id: str):
        """Reject an assigned order"""
        try:
            body = app.current_event.json_body
            reason = body.get('reason', 'Rider unavailable')
            
            logger.info(f"[orderId={order_id}] Rider {rider_id} rejecting order: {reason}")
            
            # Get order
            order = OrderService.get_order(order_id)
            if not order:
                return {"error": "Order not found"}, 404
            
            if order.rider_id != rider_id:
                return {"error": "Order not assigned to this rider"}, 403
            
            # Only allow rejecting orders in RIDER_ASSIGNED status
            if order.status != 'OFFERED_TO_RIDER':
                return {"error": f"Order cannot be rejected in {order.status} status"}, 400
            
            # Clear rider assignment and mark awaiting reassignment
            OrderService.update_order(order_id, {
                'riderId': None,
                'status': Order.STATUS_AWAITING_RIDER_ASSIGNMENT
            })

            # Reassign to next available rider
            restaurant_lat = order.pickup_lat
            restaurant_lng = order.pickup_lng
            if restaurant_lat is None or restaurant_lng is None:
                restaurant = RestaurantService.get_restaurant_by_id(order.restaurant_id)
                if restaurant:
                    restaurant_lat = restaurant.latitude
                    restaurant_lng = restaurant.longitude

            if restaurant_lat is not None and restaurant_lng is not None:
                OrderAssignmentService.assign_order_to_rider(order_id, restaurant_lat, restaurant_lng)
            else:
                logger.warning(f"[orderId={order_id}] Missing restaurant location, cannot reassign")
            
            logger.info(f"[orderId={order_id}] Rejected by rider {rider_id}")
            metrics.add_metric(name="OrderRejectedByRider", unit="Count", value=1)
            
            return {"message": "Order rejected", "orderId": order_id}, 200
            
        except Exception as e:
            logger.error("Error rejecting order", exc_info=True)
            return {"error": "Failed to reject order", "message": str(e)}, 500
    
    @app.put("/api/v1/riders/<rider_id>/orders/<order_id>/status")
    @tracer.capture_method
    def update_order_status(rider_id: str, order_id: str):
        """
        Update order status by rider
        
        Request:
        {
            "status": "PICKED_UP" | "OUT_FOR_DELIVERY" | "DELIVERED"
        }
        """
        try:
            body = app.current_event.json_body
            new_status = body.get('status')
            
            if not new_status:
                return {"error": "Status required"}, 400
            
            logger.info(f"[orderId={order_id}] Rider {rider_id} updating status to {new_status}")
            
            # Get order
            order = OrderService.get_order(order_id)
            if not order:
                return {"error": "Order not found"}, 404
            
            if order.rider_id != rider_id:
                return {"error": "Order not assigned to this rider"}, 403
            
            # Validate status transition
            if new_status == Order.STATUS_OUT_FOR_DELIVERY:
                if order.status != Order.PICKED_UP:
                    return {"error": "Order must be PICKED_UP first"}, 400
                
                # Mark as out for delivery
                OrderService.update_order(order_id, {
                    'riderPickupAt': datetime.utcnow().isoformat()
                })
            
            elif new_status == Order.STATUS_DELIVERED:
                if order.status != Order.STATUS_OUT_FOR_DELIVERY:
                    return {"error": "Order must be OUT_FOR_DELIVERY first"}, 400
                
                # Mark as delivered (no OTP verification needed)
                OrderService.update_order(order_id, {
                    'riderDeliveredAt': datetime.utcnow().isoformat()
                })
                
                # Clear rider's working_on_order
                RiderService.set_working_on_order(rider_id, None)
                
                # Add to rider's earnings
                EarningsService.add_delivery(rider_id, order_id, order.delivery_fee, 0.0)

                # Add restaurant earnings entry
                restaurant_settlement = 0.0
                if order.revenue and isinstance(order.revenue, dict):
                    restaurant_settlement = float(order.revenue.get('restaurantSettlement', 0))
                RestaurantEarningsService.add_order_earning(order.restaurant_id, order_id, restaurant_settlement)
            
            # Update order status
            OrderService.update_order_status(order_id, new_status)
            
            logger.info(f"[orderId={order_id}] Status updated to {new_status}")
            metrics.add_metric(name=f"Order{new_status}", unit="Count", value=1)
            
            return {"message": "Order status updated", "orderId": order_id, "status": new_status}, 200
            
        except Exception as e:
            logger.error("Error updating order status", exc_info=True)
            return {"error": "Failed to update order status", "message": str(e)}, 500


def generate_delivery_otp() -> str:
    """Generate 4-digit delivery OTP"""
    return str(random.randint(1000, 9999))
