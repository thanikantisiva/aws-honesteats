"""Payment routes for Razorpay integration"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.payment_service import PaymentService
from services.order_service import OrderService
from services.restaurant_service import RestaurantService
from services.address_service import AddressService
from models.payment import Payment
from models.order import Order
from utils.dynamodb import generate_id, dynamodb_client, TABLES
import random

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_payment_routes(app):
    """Register payment routes"""
    
    @app.post("/api/v1/payments/initiate")
    @tracer.capture_method
    def initiate_payment():
        """
        Initiate payment - Creates ORDER with INITIATED status, then creates Razorpay order
        """
        try:
            body = app.current_event.json_body
            customer_phone = body.get('customerPhone')
            restaurant_id = body.get('restaurantId')
            restaurant_name = body.get('restaurantName')
            amount = float(body.get('amount', 0))
            
            # Order details for pre-creation
            items = body.get('items', [])
            delivery_fee = float(body.get('deliveryFee', 0))
            platform_fee = float(body.get('platformFee', 0))
            delivery_address = body.get('deliveryAddress')
            formatted_address = body.get('formattedAddress')
            address_id = body.get('addressId')
            restaurant_image = body.get('restaurantImage')
            coupon_code = body.get('couponCode')
            coupon_applied = bool(body.get('couponApplied'))
            total_discount = float(body.get('totalDiscount', 0))
            
            if not all([customer_phone, restaurant_id, restaurant_name, amount]):
                return {"error": "Missing required fields"}, 400
            
            # Generate IDs
            payment_id = generate_id('PAY')
            order_id = generate_id('ORD')  # Generate order ID now
            
            logger.info(f"[orderId={order_id}] 💳 Initiating payment: {payment_id}, amount: ₹{amount}")
            
            # ENRICH items with restaurantPrice from DB
            from services.menu_service import MenuService
            
            enriched_items = []
            total_customer_amount = 0
            total_restaurant_amount = 0
            
            for item in items:
                item_id = item.get('itemId')
                quantity = item.get('quantity', 1)
                customer_price = float(item.get('price', 0))
                
                try:
                    menu_item = MenuService.get_menu_item(restaurant_id, item_id)
                    restaurant_price = menu_item.restaurant_price if menu_item else customer_price
                except Exception:
                    restaurant_price = customer_price
                
                item_customer_total = customer_price * quantity
                item_restaurant_total = restaurant_price * quantity
                
                enriched_items.append({
                    'itemId': item_id,
                    'name': item.get('name'),
                    'quantity': quantity,
                    'price': customer_price,
                    'restaurantPrice': restaurant_price,
                    'itemCommission': round(item_customer_total - item_restaurant_total, 2)
                })
                
                total_customer_amount += item_customer_total
                total_restaurant_amount += item_restaurant_total
            
            # Calculate revenue
            food_commission = round(total_customer_amount - total_restaurant_amount, 2)
            total_platform_revenue = round(food_commission + platform_fee, 2)

            # Coupon handling based on issuedBy
            issued_by = None
            if coupon_applied and coupon_code:
                try:
                    pk = f"COUPON#{coupon_code}"
                    response = dynamodb_client.query(
                        TableName=TABLES['CONFIG'],
                        KeyConditionExpression='partitionkey = :pk',
                        ExpressionAttributeValues={':pk': {'S': pk}},
                        Limit=1
                    )
                    item = response.get('Items', [None])[0] if response.get('Items') else None
                    if item:
                        issued_by = item.get('issuedBy', {}).get('S')
                except Exception as e:
                    logger.error(f"[orderId={order_id}] Failed to fetch coupon {coupon_code}: {str(e)}")

            # Adjust revenue based on issuedBy
            restaurant_settlement = round(total_restaurant_amount, 2)
            coupon_discount = 0.0
            if coupon_applied and total_discount > 0:
                coupon_discount = total_discount
                if issued_by == "YUMDUDE":
                    total_platform_revenue = round(total_platform_revenue - total_discount, 2)
                elif issued_by == "RESTAURANT":
                    restaurant_settlement = round(restaurant_settlement - total_discount, 2)

            revenue = {
                'totalCustomerPaid': round(amount, 2),
                'totalDiscount': round(total_discount, 2),
                'restaurantSettlement': restaurant_settlement,
                'couponCode': coupon_code,
                'couponApplied': coupon_applied,
                'couponIssuedBy': issued_by,
                'platformRevenue': {
                    'foodCommission': food_commission,
                    'deliveryFee': delivery_fee,
                    'platformFee': platform_fee,
                    'totalCommission': total_platform_revenue,
                    'couponDiscount': round(coupon_discount, 2)
                }
            }
            
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
                    logger.info(f"[orderId={order_id}] Fetched restaurant location: {restaurant.name} at ({pickup_lat}, {pickup_lng})")
            except Exception as e:
                logger.error(f"[orderId={order_id}] Failed to fetch restaurant location: {str(e)}")
                # Continue without location - can be added later
            
            # Fetch delivery address coordinates
            delivery_lat = None
            delivery_lng = None
            
            if address_id and customer_phone:
                try:
                    address = AddressService.get_address(customer_phone, address_id)
                    if address:
                        delivery_lat = address.lat
                        delivery_lng = address.lng
                        logger.info(f"[orderId={order_id}] Fetched delivery location from address: ({delivery_lat}, {delivery_lng})")
                except Exception as e:
                    logger.error(f"[orderId={order_id}] Failed to fetch delivery address location: {str(e)}")
            
            # Create Order in DB with INITIATED status
            order = Order(
                order_id=order_id,
                customer_phone=customer_phone,
                restaurant_id=restaurant_id,
                items=enriched_items,
                food_total=round(total_customer_amount, 2),
                delivery_fee=delivery_fee,
                platform_fee=platform_fee,
                grand_total=amount,
                status=Order.STATUS_INITIATED,  # INITIATED - payment pending
                restaurant_name=restaurant_name,
                restaurant_image=restaurant_image,
                delivery_address=delivery_address,
                formatted_address=formatted_address,
                address_id=address_id,
                revenue=revenue,
                # Pickup location (restaurant)
                pickup_address=pickup_address,
                pickup_lat=pickup_lat,
                pickup_lng=pickup_lng,
                # Delivery location (customer address)
                delivery_lat=delivery_lat,
                delivery_lng=delivery_lng
            )
            
            from services.order_service import OrderService
            OrderService.create_order(order)
            logger.info(f"[orderId={order_id}] 📦 Created with INITIATED status")
            
            # Create Razorpay order with orderId in notes
            razorpay_order = PaymentService.create_razorpay_order(
                amount_in_rupees=amount,
                receipt_id=payment_id,
                customer_phone=customer_phone,
                notes={
                    'order_id': order_id,  # Store orderId in Razorpay notes
                    'restaurant_id': restaurant_id,
                    'restaurant_name': restaurant_name
                }
            )
            
            # Create payment record in DynamoDB with revenue
            payment = Payment(
                payment_id=payment_id,
                customer_phone=customer_phone,
                restaurant_id=restaurant_id,
                restaurant_name=restaurant_name,
                amount=amount,
                razorpay_order_id=razorpay_order['id'],
                payment_status=Payment.STATUS_INITIATED,
                order_id=order_id,  # Link to order
                revenue=revenue  # Store revenue in payment
            )
            
            PaymentService.create_payment(payment)
            metrics.add_metric(name="PaymentInitiated", unit="Count", value=1)
            metrics.add_metric(name="OrderInitiated", unit="Count", value=1)
            
            return {
                'paymentId': payment_id,
                'orderId': order_id,  # Return orderId to frontend
                'razorpayOrderId': razorpay_order['id'],
                'razorpayKeyId': PaymentService.get_razorpay_key_id(),
                'amount': razorpay_order['amount'],  # In paise
                'currency': razorpay_order['currency']
            }, 200
            
        except Exception as e:
            logger.error("Error initiating payment", exc_info=True)
            return {"error": "Failed to initiate payment", "message": str(e)}, 500
    
    @app.post("/api/v1/payments/verify")
    @tracer.capture_method
    def verify_payment():
        """
        Verify payment and create order
        This happens AFTER successful payment
        """
        try:
            body = app.current_event.json_body
            payment_id = body.get('paymentId')
            razorpay_order_id = body.get('razorpayOrderId')
            razorpay_payment_id = body.get('razorpayPaymentId')
            razorpay_signature = body.get('razorpaySignature')
            payment_method = body.get('paymentMethod', 'UNKNOWN')
            upi_app = body.get('upiApp')
            
            # Order details (items, address, fees) from frontend
            items = body.get('items', [])
            delivery_fee = float(body.get('deliveryFee', 0))
            platform_fee = float(body.get('platformFee', 0))
            delivery_address = body.get('deliveryAddress')
            formatted_address = body.get('formattedAddress')
            address_id = body.get('addressId')
            restaurant_image = body.get('restaurantImage')
            
            if not all([payment_id, razorpay_order_id, razorpay_payment_id, razorpay_signature]):
                return {"error": "Missing required fields"}, 400
            
            logger.info(f"Verifying payment: {payment_id}")
            
            # Get payment record
            payment = PaymentService.get_payment(payment_id)
            if not payment:
                return {"error": "Payment not found"}, 404
            
            if payment.payment_status != Payment.STATUS_INITIATED:
                return {"error": f"Payment already {payment.payment_status}"}, 400
            
            # Verify signature with Razorpay
            is_valid = PaymentService.verify_payment_signature(
                razorpay_order_id=razorpay_order_id,
                razorpay_payment_id=razorpay_payment_id,
                razorpay_signature=razorpay_signature
            )
            
            if not is_valid:
                # Update payment as failed
                PaymentService.update_payment(payment_id, {
                    'paymentStatus': Payment.STATUS_FAILED,
                    'errorCode': 'SIGNATURE_VERIFICATION_FAILED',
                    'errorDescription': 'Payment signature verification failed'
                })
                metrics.add_metric(name="PaymentFailed", unit="Count", value=1)
                return {"error": "Payment verification failed", "verified": False}, 400
            
            # Update payment as successful
            PaymentService.update_payment(payment_id, {
                'paymentStatus': Payment.STATUS_SUCCESS,
                'razorpayPaymentId': razorpay_payment_id,
                'razorpaySignature': razorpay_signature,
                'paymentMethod': payment_method,
                'upiApp': upi_app
            })
            
            # Get orderId from payment (order was created during initiate)
            order_id = getattr(payment, 'order_id', None)
            # Payment verified - Update order status from INITIATED to CONFIRMED
            logger.info(f"[orderId={order_id}] ✅ Payment verified, updating order status to CONFIRMED")
            
            if order_id:
                # Order already exists with INITIATED status - just update to CONFIRMED
                logger.info(f"[orderId={order_id}] ✅ Updating to CONFIRMED")
                updated_order = OrderService.update_order_status(order_id, Order.STATUS_CONFIRMED, None)
                
                # Update order with payment details
                OrderService.update_order(order_id, {
                    'paymentId': payment_id,
                    'paymentMethod': payment_method,
                    'deliveryOtp': str(random.randint(1000, 9999)),
                    'pickupOtp': str(random.randint(1000, 9999))
                })
            else:
                # Fallback: Create order if it doesn't exist (shouldn't happen with new flow)
                logger.warning(f"[orderId={order_id}] ⚠️ Order not found in payment, creating new order")
                
                # Fetch restaurant location details
                pickup_address = None
                pickup_lat = None
                pickup_lng = None
                
                try:
                    restaurant = RestaurantService.get_restaurant_by_id(payment.restaurant_id)
                    if restaurant:
                        pickup_address = f"{restaurant.name}, {restaurant.location_id}"
                        pickup_lat = restaurant.latitude
                        pickup_lng = restaurant.longitude
                        logger.info(f"[orderId={order_id}] Fetched restaurant location: {restaurant.name} at ({pickup_lat}, {pickup_lng})")
                except Exception as e:
                    logger.error(f"[orderId={order_id}] Failed to fetch restaurant location: {str(e)}")
                
                # Fetch delivery address coordinates
                delivery_lat = None
                delivery_lng = None
                
                if address_id and payment.customer_phone:
                    try:
                        address = AddressService.get_address(payment.customer_phone, address_id)
                        if address:
                            delivery_lat = address.lat
                            delivery_lng = address.lng
                            logger.info(f"[orderId={order_id}] Fetched delivery location from address: ({delivery_lat}, {delivery_lng})")
                    except Exception as e:
                        logger.error(f"[orderId={order_id}] Failed to fetch delivery address location: {str(e)}")
                
                order = Order(
                    order_id=generate_id('ORD'),
                    customer_phone=payment.customer_phone,
                    restaurant_id=payment.restaurant_id,
                    items=enriched_items,
                    food_total=food_total,
                    delivery_fee=delivery_fee,
                    platform_fee=platform_fee,
                    grand_total=payment.amount,
                    status=Order.STATUS_CONFIRMED,
                    payment_id=payment_id,
                    payment_method=payment_method,
                    restaurant_name=payment.restaurant_name,
                    restaurant_image=restaurant_image,
                    delivery_address=delivery_address,
                    formatted_address=formatted_address,
                    address_id=address_id,
                    revenue=revenue,
                    # Pickup location (restaurant)
                    pickup_address=pickup_address,
                    pickup_lat=pickup_lat,
                    pickup_lng=pickup_lng,
                    # Delivery location (customer address)
                    delivery_lat=delivery_lat,
                    delivery_lng=delivery_lng
                )
                updated_order = OrderService.create_order(order)
                order_id = updated_order.order_id
            
            # Link order to payment
            PaymentService.update_payment(payment_id, {'orderId': order_id})
            
            metrics.add_metric(name="PaymentVerified", unit="Count", value=1)
            metrics.add_metric(name="OrderConfirmed", unit="Count", value=1)
            
            logger.info(f"[orderId={order_id}] ✅ Payment verified and order confirmed")
            
            # Return order details
            return {
                'verified': True,
                'orderId': order_id,
                'orderStatus': Order.STATUS_CONFIRMED,
                'paymentId': payment_id
            }, 200
            
        except Exception as e:
            logger.error("Error verifying payment", exc_info=True)
            return {"error": "Failed to verify payment", "message": str(e)}, 500
    
    @app.get("/api/v1/payments/<payment_id>")
    @tracer.capture_method
    def get_payment(payment_id: str):
        """Get payment details"""
        try:
            logger.info(f"Getting payment: {payment_id}")
            payment = PaymentService.get_payment(payment_id)
            
            if not payment:
                return {"error": "Payment not found"}, 404
            
            metrics.add_metric(name="PaymentRetrieved", unit="Count", value=1)
            return payment.to_dict(), 200
        except Exception as e:
            logger.error("Error getting payment", exc_info=True)
            return {"error": "Failed to get payment", "message": str(e)}, 500
    
    @app.get("/api/v1/payments")
    @tracer.capture_method
    def list_payments():
        """List payments for a customer"""
        try:
            query_params = app.current_event.query_string_parameters or {}
            customer_phone = query_params.get('customerPhone')
            limit = int(query_params.get('limit', 20))
            
            if not customer_phone:
                return {"error": "customerPhone parameter is required"}, 400
            
            # Add '+' prefix if not present
            if not customer_phone.startswith('+'):
                customer_phone = '+' + customer_phone.strip()
            
            logger.info(f"Listing payments for customer: {customer_phone}")
            payments = PaymentService.list_payments_by_customer(customer_phone, limit)
            
            metrics.add_metric(name="PaymentsListed", unit="Count", value=1)
            return {
                "payments": [p.to_dict() for p in payments],
                "total": len(payments)
            }, 200
        except Exception as e:
            logger.error("Error listing payments", exc_info=True)
            return {"error": "Failed to list payments", "message": str(e)}, 500
    
    @app.post("/api/v1/payments/webhook")
    def razorpay_webhook():
        """
        Handle Razorpay webhook events
        This provides additional reliability for payment confirmation
        """
        try:
            import hmac
            import hashlib
            import os
            
            body = app.current_event.body
            signature = app.current_event.get_header_value('X-Razorpay-Signature') or ''
            
            # Verify webhook signature
            from utils.ssm import get_secret
            webhook_secret = get_secret('RAZORPAY_WEBHOOK_SECRET', '')
            if webhook_secret:
                expected_signature = hmac.new(
                    webhook_secret.encode('utf-8'),
                    body.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                
                if signature != expected_signature:
                    logger.error("Invalid webhook signature")
                    return {"error": "Invalid signature"}, 400
            
            # Parse webhook payload
            import json
            payload = json.loads(body)
            event = payload.get('event')
            payment_entity = payload.get('payload', {}).get('payment', {}).get('entity', {})
            
            logger.info(f"📨 Razorpay webhook received: {event}")
            
            if event == 'payment.captured':
                # Payment was successful
                razorpay_payment_id = payment_entity.get('id')
                razorpay_order_id = payment_entity.get('order_id')
                
                logger.info(f"✅ Payment captured: {razorpay_payment_id}")
                
                # Update payment status in database
                # Find payment by razorpayOrderId
                # This is a backup mechanism in case frontend verification fails
                
            elif event == 'payment.failed':
                # Payment failed
                razorpay_payment_id = payment_entity.get('id')
                error_code = payment_entity.get('error_code')
                error_description = payment_entity.get('error_description')
                
                logger.error(f"❌ Payment failed: {razorpay_payment_id}, {error_code}")
                
            elif event == 'refund.created' or event == 'payment.refunded':
                # Refund initiated
                refund_entity = payload.get('payload', {}).get('refund', {}).get('entity', {})
                payment_entity = payload.get('payload', {}).get('payment', {}).get('entity', {})
                
                refund_id = refund_entity.get('id')
                razorpay_payment_id = refund_entity.get('payment_id') or payment_entity.get('id')
                amount_refunded = refund_entity.get('amount', 0) / 100  # Convert paise to rupees
                refund_status = refund_entity.get('status')
                
                logger.info(f"💰 Refund {refund_status}: {refund_id}, payment: {razorpay_payment_id}, amount: ₹{amount_refunded}")
                
                # Find and update payment by razorpayPaymentId
                if razorpay_payment_id:
                    # Query to find payment by razorpayPaymentId
                    try:
                        # Scan to find payment (alternatively, use GSI if needed)
                        scan_response = dynamodb_client.scan(
                            TableName=TABLES['PAYMENTS'],
                            FilterExpression='razorpayPaymentId = :payment_id',
                            ExpressionAttributeValues={
                                ':payment_id': {'S': razorpay_payment_id}
                            },
                            Limit=1
                        )
                        
                        if scan_response.get('Items'):
                            payment_item = scan_response['Items'][0]
                            payment_id = payment_item['paymentId']['S']
                            
                            # Update payment status to REFUNDED
                            PaymentService.update_payment(payment_id, {
                                'paymentStatus': Payment.STATUS_REFUNDED,
                                'errorCode': refund_id,
                                'errorDescription': f'Refund {refund_status}: ₹{amount_refunded}'
                            })
                            
                            logger.info(f"✅ Payment {payment_id} marked as REFUNDED")
                            
                            # Also update associated order status
                            order_id = payment_item.get('orderId', {}).get('S')
                            if order_id:
                                OrderService.update_order_status(order_id, Order.STATUS_CANCELLED, None)
                                logger.info(f"✅ Order {order_id} marked as CANCELLED")
                        else:
                            logger.warning(f"Payment not found for razorpay_payment_id: {razorpay_payment_id}")
                    except Exception as e:
                        logger.error(f"Error updating refund status: {str(e)}", exc_info=True)
                
            metrics.add_metric(name="WebhookReceived", unit="Count", value=1)
            return {"status": "processed"}, 200
            
        except Exception as e:
            logger.error("Error processing webhook", exc_info=True)
            return {"error": "Webhook processing failed"}, 500
