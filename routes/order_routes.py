"""Order routes"""
from datetime import datetime
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.order_service import OrderService
from services.user_service import UserService
from services.restaurant_service import RestaurantService
from services.address_service import AddressService
from models.order import Order
from utils.dynamodb import generate_id

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_order_routes(app):
    """Register order routes"""
    
    @app.get("/api/v1/orders/<order_id>")
    @tracer.capture_method
    def get_order(order_id: str):
        """Get order by ID with enriched rider details"""
        try:
            logger.info(f"[orderId={order_id}] Getting order")
            order = OrderService.get_order(order_id)
            
            if not order:
                return {"error": "Order not found"}, 404
            
            order_dict = order.to_dict()
            
            # Fetch rider data if order has a rider assigned
            if order.rider_id:
                try:
                    # First get rider to get phone number
                    from services.rider_service import RiderService
                    rider = RiderService.get_rider(order.rider_id)
                    if rider:
                        # Update rider's current location in the order for live tracking
                        if rider.lat is not None and rider.lng is not None:
                            # Only update if order is in active delivery states
                            if order.status in ['RIDER_ASSIGNED', 'PICKED_UP', 'OUT_FOR_DELIVERY']:
                                update_data = {
                                    'riderCurrentLat': rider.lat,
                                    'riderCurrentLng': rider.lng,
                                    'riderSpeed': rider.speed or 0.0,
                                    'riderHeading': rider.heading or 0.0,
                                    'riderLocationUpdatedAt': datetime.utcnow().isoformat()
                                }
                                OrderService.update_order(order_id, update_data)
                                # Update the dict we're returning
                                order_dict.update(update_data)
                                logger.info(f"[orderId={order_id}] Updated rider location: ({rider.lat}, {rider.lng})")
                        
                        # Then get user details from Users table using phone and RIDER role
                        rider_user = UserService.get_user_by_role(rider.phone, "RIDER")
                        if rider_user:
                            order_dict['riderName'] = f"{rider_user.first_name} {rider_user.last_name}"
                            order_dict['riderPhone'] = rider.phone
                            logger.info(f"[orderId={order.order_id}] Enriched with rider: {order_dict['riderName']}")
                except Exception as e:
                    logger.error(f"[orderId={order_id}] Failed to fetch rider {order.rider_id}: {str(e)}")
            
            metrics.add_metric(name="OrderRetrieved", unit="Count", value=1)
            return order_dict, 200
        except Exception as e:
            logger.error(f"[orderId={order_id}] Error getting order", exc_info=True)
            return {"error": "Failed to get order", "message": str(e)}, 500
    
    @app.get("/api/v1/orders")
    @tracer.capture_method
    def list_orders():
        """List orders - supports filtering by customer, restaurant, or rider with enriched rider details"""
        try:
            query_params = app.current_event.query_string_parameters or {}
            customer_phone = query_params.get('customerPhone')
            restaurant_id = query_params.get('restaurantId')
            rider_id = query_params.get('riderId')
            limit = int(query_params.get('limit', 20))
            
            # Add '+' prefix if not present and phone doesn't start with '+'
            if customer_phone and not customer_phone.startswith('+'):
                customer_phone = '+' + customer_phone.strip()
            
            # Optional status filter
            status_filter = query_params.get('status')
            
            logger.info(f"Listing orders - customer: {customer_phone}, restaurant: {restaurant_id}, rider: {rider_id}, status: {status_filter}")
            logger.info(f"Query params received: {query_params}")
            
            if customer_phone:
                orders = OrderService.list_orders_by_customer(customer_phone, status=status_filter, limit=limit)
            elif restaurant_id:
                orders = OrderService.list_orders_by_restaurant(restaurant_id, status=status_filter, limit=limit)
            elif rider_id:
                orders = OrderService.list_orders_by_rider(rider_id, status=status_filter, limit=limit)
            else:
                return {"error": "Must provide customerPhone, restaurantId, or riderId"}, 400
            
            # Enrich orders with rider details
            enriched_orders = []
            for order in orders:
                order_dict = order.to_dict()
                
                # Fetch rider data if order has a rider assigned
                if order.rider_id:
                    try:
                        # First get rider to get phone number
                        from services.rider_service import RiderService
                        rider = RiderService.get_rider(order.rider_id)
                        if rider:
                            # Then get user details from Users table using phone and RIDER role
                            rider_user = UserService.get_user_by_role(rider.phone, "RIDER")
                            if rider_user:
                                order_dict['riderName'] = f"{rider_user.first_name} {rider_user.last_name}"
                                order_dict['riderPhone'] = rider.phone
                                logger.info(f"[orderId={order.order_id}] Enriched with rider: {order_dict['riderName']}")
                    except Exception as e:
                        logger.error(f"[orderId={order.order_id}] Failed to fetch rider {order.rider_id}: {str(e)}")
                
                enriched_orders.append(order_dict)
            
            metrics.add_metric(name="OrdersListed", unit="Count", value=1)
            return {
                "orders": enriched_orders,
                "total": len(enriched_orders)
            }, 200
        except Exception as e:
            logger.error("Error listing orders", exc_info=True)
            return {"error": "Failed to list orders", "message": str(e)}, 500
    
    @app.post("/api/v1/orders")
    @tracer.capture_method
    def create_order():
        """Create a new order"""
        try:
            body = app.current_event.json_body
            customer_phone = body.get('customerPhone')
            restaurant_id = body.get('restaurantId')
            items = body.get('items', [])
            food_total = body.get('foodTotal', 0)
            delivery_fee = body.get('deliveryFee', 0)
            platform_fee = body.get('platformFee', 0)
            
            if not all([customer_phone, restaurant_id, items]):
                return {"error": "customerPhone, restaurantId, and items are required"}, 400
            
            grand_total = food_total + delivery_fee + platform_fee
            
            logger.info(f"Creating order - customer: {customer_phone}, restaurant: {restaurant_id}")
            
            # Fetch restaurant location details
            pickup_address = None
            pickup_lat = None
            pickup_lng = None
            
            try:
                restaurant = RestaurantService.get_restaurant_by_id(restaurant_id)
                if restaurant:
                    pickup_address = f"{restaurant.name}, {restaurant.location_id}"
                    pickup_lat = restaurant.latitude
                    pickup_lng = restaurant.longitude
                    logger.info(f"Fetched restaurant location: {restaurant.name} at ({pickup_lat}, {pickup_lng})")
            except Exception as e:
                logger.error(f"Failed to fetch restaurant location: {str(e)}")
                # Continue without location - can be added later
            
            # Fetch delivery address coordinates
            delivery_lat = None
            delivery_lng = None
            address_id = body.get('addressId')
            
            if address_id and customer_phone:
                try:
                    address = AddressService.get_address(customer_phone, address_id)
                    if address:
                        delivery_lat = address.lat
                        delivery_lng = address.lng
                        logger.info(f"Fetched delivery location from address: ({delivery_lat}, {delivery_lng})")
                except Exception as e:
                    logger.error(f"Failed to fetch delivery address location: {str(e)}")
            
            order = Order(
                order_id=generate_id('ORD'),
                customer_phone=customer_phone,
                restaurant_id=restaurant_id,
                items=items,
                food_total=float(food_total),
                delivery_fee=float(delivery_fee),
                platform_fee=float(platform_fee),
                grand_total=grand_total,
                status=Order.STATUS_PENDING,
                rider_id=body.get('riderId'),
                restaurant_name=body.get('restaurantName'),
                restaurant_image=body.get('restaurantImage'),
                delivery_address=body.get('deliveryAddress'),
                formatted_address=body.get('formattedAddress'),
                address_id=address_id,
                # Pickup location (restaurant)
                pickup_address=pickup_address,
                pickup_lat=pickup_lat,
                pickup_lng=pickup_lng,
                # Delivery location (customer address)
                delivery_lat=delivery_lat,
                delivery_lng=delivery_lng
            )
            
            created_order = OrderService.create_order(order)
            metrics.add_metric(name="OrderCreated", unit="Count", value=1)
            
            return created_order.to_dict(), 201
        except Exception as e:
            logger.error("Error creating order", exc_info=True)
            return {"error": "Failed to create order", "message": str(e)}, 500
    
    @app.put("/api/v1/orders/<order_id>/status")
    @tracer.capture_method
    def update_order_status(order_id: str):
        """Update order status"""
        try:
            body = app.current_event.json_body
            status = body.get('status')
            rider_id = body.get('riderId')
            
            if not status:
                return {"error": "Status is required"}, 400
            
            valid_statuses = Order.get_all_statuses()
            
            if status not in valid_statuses:
                return {"error": f"Invalid status. Must be one of: {', '.join(valid_statuses)}"}, 400
            
            logger.info(f"[orderId={order_id}] Updating order status to {status} riderId={rider_id}")
            
            updated_order = OrderService.update_order_status(order_id, status, rider_id)
            metrics.add_metric(name="OrderStatusUpdated", unit="Count", value=1)
            
            return updated_order.to_dict(), 200
        except Exception as e:
            logger.error(f"[orderId={order_id}] Error updating order status", exc_info=True)
            return {"error": "Failed to update order status", "message": str(e)}, 500
