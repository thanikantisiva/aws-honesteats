"""Rider order management routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.order_service import OrderService
from services.order_assignment_service import OrderAssignmentService
from services.rider_service import RiderService
from services.earnings_service import EarningsService
from services.restaurant_earnings_service import RestaurantEarningsService
from services.restaurant_service import RestaurantService
from services.notification_service import NotificationService
from services.address_service import AddressService
from services.payment_service import PaymentService
from models.order import Order
from models.payment import Payment
from datetime import datetime, timezone
import random
from typing import Any, Dict, Optional

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def _effective_payment_status(payment: Payment) -> str:
    """DynamoDB rows may omit paymentStatus (parsed as ''). Treat as pending for rider COD/UPI flows."""
    s = (payment.payment_status or "").strip().upper()
    return s if s else Payment.STATUS_INITIATED


def _revenue_final_payout(order: Order, *path: str) -> float:
    """Read nested scalar from order.revenue (e.g. ('riderRevenue', 'finalPayout'))."""
    rev: Optional[Dict[str, Any]] = getattr(order, "revenue", None)
    if not isinstance(rev, dict):
        return 0.0
    node: Any = rev
    for key in path:
        if not isinstance(node, dict):
            return 0.0
        node = node.get(key)
        if node is None:
            return 0.0
    try:
        return float(node)
    except (TypeError, ValueError):
        return 0.0


def _pending_initiated_payment_for_order(order: Order, order_id: str) -> Optional[Payment]:
    """Prefer the order's paymentId when multiple INITIATED rows exist."""
    candidates = PaymentService.get_initiated_rider_upi_payments_for_order(order_id)
    if not candidates:
        return None
    pid = order.payment_id
    if pid:
        for p in candidates:
            if p.payment_id == pid:
                return p
    if len(candidates) > 1:
        logger.warning(
            f"[orderId={order_id}] Multiple INITIATED payments; using {candidates[0].payment_id}"
        )
    return candidates[0]


def _rider_payout_for_earnings(order: Order) -> float:
    """Rider delivery payout: revenue.riderRevenue.finalPayout, then calculated_fee_response, then delivery_fee."""
    payout = _revenue_final_payout(order, "riderRevenue", "finalPayout")
    if payout:
        return payout
    cfr = getattr(order, "calculated_fee_response", None)
    if isinstance(cfr, dict):
        for key in ("riderSettlementAmount", "riderFare", "deliveryFee"):
            val = cfr.get(key)
            if val is not None:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
    try:
        return float(getattr(order, "delivery_fee", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def _rider_earnings_breakdown(order: Order) -> tuple[float, float]:
    """Return (deliveryFee, incentives) from order.revenue.riderRevenue.

    deliveryFee = riderSettlementAmount (the pure distance-based fee).
    incentives  = longDistanceBonus + any future bonus fields.
    Falls back to treating the full payout as delivery fee when the breakdown is missing.
    """
    delivery_fee = _revenue_final_payout(order, "riderRevenue", "riderSettlementAmount")
    incentives = _revenue_final_payout(order, "riderRevenue", "longDistanceBonus")
    if delivery_fee or incentives:
        return delivery_fee, incentives
    return _rider_payout_for_earnings(order), 0.0


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    """Best-effort ISO timestamp parser for order lifecycle fields."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


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
            
            # Include rider's own rating and count for the Orders tab (from Riders table, raw item)
            rider_rating, rider_rated_count = RiderService.get_rider_rating_and_count(rider_id)
            logger.info(f"[riderId={rider_id}] Returning rating={rider_rating}, ratedCount={rider_rated_count}")
            
            metrics.add_metric(name="RiderOrdersRetrieved", unit="Count", value=1)
            
            return {
                "orders": enriched_orders,
                "total": len(enriched_orders),
                "riderRating": rider_rating,
                "riderRatedCount": rider_rated_count,
            }, 200
            
        except Exception as e:
            logger.error(f"[riderId={rider_id}] Error getting rider orders", exc_info=True)
            return {"error": "Failed to get orders", "message": str(e)}, 500

    @app.post("/api/v1/riders/<rider_id>/orders/<order_id>/upi-qr")
    @tracer.capture_method
    def rider_order_upi_qr(rider_id: str, order_id: str):
        """
        Create a new Razorpay dynamic UPI QR each call (~10 min expiry). Pay-at-delivery COD only.
        Dashboard: enable UPI QR + webhooks (qr_code.credited, payment.captured).
        """
        try:
            order = OrderService.get_order(order_id)
            if not order:
                return {"error": "Order not found"}, 404
            if order.rider_id != rider_id:
                return {"error": "Order not assigned to this rider"}, 403
            if (order.payment_method or "").upper() != Payment.METHOD_COD:
                return {"error": "UPI QR is only for cash on delivery orders"}, 400

            payment = _pending_initiated_payment_for_order(order, order_id)
            if not payment:
                return {"error": "No pending payment for this order"}, 400

            close_by_new = PaymentService.default_qr_close_by_epoch()
            qr_data = PaymentService.create_upi_qr_code(
                float(payment.amount),
                payment.payment_id,
                order_id,
                close_by_new,
            )
            qr_id = qr_data.get("id")
            image_url = qr_data.get("image_url")
            cb = int(qr_data.get("close_by") or close_by_new)
            PaymentService.update_payment(
                payment.payment_id,
                {
                    "razorpayQrCodeId": qr_id,
                    "qrImageUrl": image_url,
                    "qrCloseBy": cb,
                },
            )
            metrics.add_metric(name="RiderUpiQrCreated", unit="Count", value=1)
            return {
                "paymentId": payment.payment_id,
                "qrCodeId": qr_id,
                "imageUrl": image_url,
                "closeBy": cb,
                "amount": max(1, int(round(float(payment.amount) * 100))),
                "amountRupees": float(payment.amount),
            }, 200
        except Exception as e:
            logger.error(
                f"[orderId={order_id}] rider UPI QR failed for rider {rider_id}", exc_info=True
            )
            return {"error": "Failed to create UPI QR", "message": str(e)}, 500

    @app.post("/api/v1/riders/<rider_id>/orders/<order_id>/cash-collected")
    @tracer.capture_method
    def rider_order_cash_collected(rider_id: str, order_id: str):
        """Mark COD order as paid in cash (updates the existing Payment row)."""
        try:
            order = OrderService.get_order(order_id)
            if not order:
                return {"error": "Order not found"}, 404
            if order.rider_id != rider_id:
                return {"error": "Order not assigned to this rider"}, 403
            if (order.payment_method or "").upper() != Payment.METHOD_COD:
                return {"error": "Order is not cash on delivery"}, 400

            payment = None
            if order.payment_id:
                payment = PaymentService.get_payment(order.payment_id)
            if not payment or payment.order_id != order_id:
                payment = _pending_initiated_payment_for_order(order, order_id)
            if not payment:
                return {"error": "No payment found for this order"}, 404

            eff = _effective_payment_status(payment)
            if eff == Payment.STATUS_SUCCESS:
                pm = (payment.payment_method or "").upper()
                if pm == Payment.METHOD_UPI:
                    return {"error": "Payment already completed via UPI"}, 400
                return {
                    "verified": True,
                    "paymentId": payment.payment_id,
                    "orderId": order_id,
                }, 200

            if eff != Payment.STATUS_INITIATED:
                return {
                    "error": f"Payment cannot be marked cash: status is {eff}",
                }, 400

            cod_ref = f"cod_{payment.payment_id}"
            PaymentService.update_payment(
                payment.payment_id,
                {
                    "paymentStatus": Payment.STATUS_SUCCESS,
                    "razorpayPaymentId": cod_ref,
                    "razorpaySignature": "cod_internal",
                    "paymentMethod": Payment.METHOD_COD,
                    "upiApp": None,
                    "razorpayQrCodeId": None,
                    "qrImageUrl": None,
                    "qrCloseBy": None,
                },
            )
            OrderService.update_order(
                order_id,
                {"paymentId": payment.payment_id, "paymentMethod": Payment.METHOD_COD},
            )
            metrics.add_metric(name="RiderCashCollected", unit="Count", value=1)
            logger.info(f"[orderId={order_id}] Cash collected recorded by rider {rider_id} payment={payment.payment_id}")
            return {
                "verified": True,
                "paymentId": payment.payment_id,
                "orderId": order_id,
            }, 200
        except Exception as e:
            logger.error(
                f"[orderId={order_id}] rider cash-collected failed for rider {rider_id}", exc_info=True
            )
            return {"error": "Failed to record cash payment", "message": str(e)}, 500
    
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
            
            
            # Update order status to requested status from path param
            new_status = status or Order.RIDER_ASSIGNED
            OrderService.update_order(order_id, {
                'status': new_status,
                'riderId': rider_id
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

            # Confirmation push (type order_accepted, quieter channel) — distinct from
            # order_assigned ring sent when the offer was first created.
            if rider and rider.phone:
                try:
                    restaurant_name = order.restaurant_name or "Restaurant"
                    NotificationService.send_rider_order_accepted_notification(
                        rider_mobile=rider.phone,
                        order_id=order_id,
                        restaurant_name=restaurant_name,
                    )
                except Exception as notify_err:
                    logger.warning(
                        f"[orderId={order_id}] Accept notification failed (non-fatal): {notify_err}"
                    )

            RiderService.increment_assignment_count(rider_id)

            logger.info(f"[orderId={order_id}] Accepted by rider {rider_id}")
            metrics.add_metric(name="OrderAcceptedByRider", unit="Count", value=1)
            
            return {"message": "Order accepted", "orderId": order_id, "status": new_status}, 200
            
        except Exception as e:
            logger.error(f"[orderId={order_id}] Error accepting order for rider {rider_id}", exc_info=True)
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
            
            # Only allow rejecting orders in OFFERED_TO_RIDER status
            if order.status != 'OFFERED_TO_RIDER':
                return {"error": f"Order cannot be rejected in {order.status} status"}, 400
            
            # Add this rider to rejectedByRiders so they are not offered this order again
            rejected = list(order.rejected_by_riders or [])
            if rider_id not in rejected:
                rejected.append(rider_id)
            
            # Clear rider assignment and mark awaiting reassignment
            OrderService.update_order(order_id, {
                'rejectedByRiders': rejected,
                'riderId': None,
                'status': Order.STATUS_AWAITING_RIDER_ASSIGNMENT
            })

            # Clear the order from rider's workingOnOrder list
            RiderService.set_working_on_order(rider_id, None)

            # Deduct rating for explicit rejection
            RiderService.apply_rejection_penalty(rider_id)

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
            logger.error(f"[orderId={order_id}] Error rejecting order for rider {rider_id}", exc_info=True)
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

                delivered_at = datetime.utcnow()
                # Mark as delivered (no OTP verification needed)
                OrderService.update_order(order_id, {
                    'riderDeliveredAt': delivered_at.isoformat()
                })

                delivery_duration_minutes = 0
                pickup_at = _parse_iso_datetime(order.rider_pickup_at)
                if pickup_at:
                    if pickup_at.tzinfo is not None:
                        pickup_at = pickup_at.astimezone(timezone.utc).replace(tzinfo=None)
                    elapsed_seconds = max(0.0, (delivered_at - pickup_at).total_seconds())
                    delivery_duration_minutes = max(0, round(elapsed_seconds / 60))
                else:
                    logger.warning(
                        f"[orderId={order_id}] Missing/invalid riderPickupAt; storing deliveryDurationMinutes=0"
                    )

                # Clear rider's working_on_order
                RiderService.set_working_on_order(rider_id, None)

                # Add to rider's earnings (order is an Order model; payouts live under revenue.*)
                delivery_fee_portion, incentives_portion = _rider_earnings_breakdown(order)
                EarningsService.add_delivery(
                    rider_id,
                    order_id,
                    delivery_fee_portion,
                    0.0,
                    incentives_portion,
                    delivery_duration_minutes=delivery_duration_minutes,
                )
                metrics.add_metric(
                    name="DeliveryDurationMinutes",
                    unit="Count",
                    value=delivery_duration_minutes,
                )

                restaurant_payout = _revenue_final_payout(order, "restaurantRevenue", "finalPayout")
                RestaurantEarningsService.add_order_earning(
                    order.restaurant_id, order_id, restaurant_payout
                )
            
            # Update order status
            OrderService.update_order_status(order_id, new_status)
            
            logger.info(f"[orderId={order_id}] Status updated to {new_status}")
            metrics.add_metric(name=f"Order{new_status}", unit="Count", value=1)
            
            return {"message": "Order status updated", "orderId": order_id, "status": new_status}, 200
            
        except Exception as e:
            logger.error(f"[orderId={order_id}] Error updating order status for rider {rider_id}", exc_info=True)
            return {"error": "Failed to update order status", "message": str(e)}, 500


def generate_delivery_otp() -> str:
    """Generate 4-digit delivery OTP"""
    return str(random.randint(1000, 9999))
