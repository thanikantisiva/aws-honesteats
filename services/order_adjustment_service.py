"""Ops-initiated item adjustments on an in-flight order.

Single entry point: `OrderAdjustmentService.adjust_items(...)`.

API shape (canonical, not diff):
  - `items`: the desired FINAL list of items on the order after the
    adjustment, each as `{itemId, quantity, ...}`. Internally we diff this
    against the order's current items to figure out what was removed, what
    was added, and what changed quantity. Each itemId must appear at most
    once; quantity must be > 0. To remove an item, simply leave it out of
    the list.

Example: order has 1x2, 2x3, 3x5. Customer swaps 1x1 for 4x1.
  -> input items = [1x1, 2x3, 3x5, 4x1]

Flow:
  1. Load + validate order (must be in a status that allows adjustment).
  2. Diff current items vs requested:
       - Same itemId on both sides         → keep original price snapshot
         (only update `quantity` if it changed)
       - Requested but not currently on    → re-enrich from `MenuService`
         for authoritative pricing
       - On the order but not requested    → dropped
  3. Recompute foodTotal, grandTotal and revenue using `services.revenue_service`.
  4. Settle the delta on Payments:
       - original COD             → update the existing Payment.amount in place
       - prepaid + delta > 0      → new COD-at-delivery Payment row (rider collects)
       - prepaid + delta < 0      → new ADJUSTMENT_PENDING_REFUND row (ops refunds manually)
  5. Persist updated items / totals / revenue / amountDueAtDelivery /
     adjustments[] on the Order in a single update_order call.
  6. Write a signed delta row to RestaurantEarningsTable.
  7. Fire FCM pushes to the customer and (if assigned) the rider.

Re-adjustments are supported: `delta` is always computed against
`originalGrandTotal` (snapshot taken at first CONFIRMED), never against the
current grandTotal, so two consecutive ops adjustments stay correct.
"""
from typing import Any, Dict, List, Optional, Tuple
from aws_lambda_powertools import Logger

from models.order import Order
from models.payment import Payment
from services.order_service import OrderService
from services.payment_service import PaymentService
from services.menu_service import MenuService
from services.coupon_service import CouponService
from services.revenue_service import compute_revenue
from services.restaurant_earnings_service import RestaurantEarningsService
from services.notification_service import NotificationService
from utils.dynamodb import generate_id
from utils.datetime_ist import now_ist_iso

logger = Logger()


class OrderAdjustmentError(Exception):
    """Raised when an ops adjustment can't proceed (validation failure)."""

    def __init__(self, code: str, message: str, http_status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


# Statuses where ops adjustment is allowed. Once the order is in the rider's
# hands (PICKED_UP onwards) the bill is essentially locked.
_ADJUSTABLE_STATUSES = {
    Order.STATUS_CONFIRMED,
    Order.STATUS_ACCEPTED,
    Order.STATUS_PREPARING,
    Order.READY_FOR_PICKUP,
    Order.STATUS_AWAITING_RIDER_ASSIGNMENT,
    Order.OFFERED_TO_RIDER,
    Order.RIDER_ASSIGNED,
}


SETTLEMENT_COD_IN_PLACE = "COD_IN_PLACE"
SETTLEMENT_COD_TOPUP = "COD_TOPUP"
SETTLEMENT_REFUND_ADJUSTMENT = "REFUND_ADJUSTMENT"


class OrderAdjustmentService:
    """Service for ops-initiated item adjustments."""

    @staticmethod
    def adjust_items(
        order_id: str,
        items: List[Dict[str, Any]],
        reason: str,
        ops_user: str,
    ) -> Dict[str, Any]:
        """Apply an ops adjustment (canonical-list style).

        `items` is the FINAL desired state of the order's items after the
        adjustment. See the module docstring for the full flow.
        """
        items = list(items or [])
        if not items:
            raise OrderAdjustmentError(
                "EMPTY_ITEMS",
                "items list is required and must contain at least one entry",
            )
        if not reason or not str(reason).strip():
            raise OrderAdjustmentError("MISSING_REASON", "reason is required")
        if not ops_user or not str(ops_user).strip():
            raise OrderAdjustmentError("MISSING_OPS_USER", "opsUser is required")

        # Validate every incoming row + reject duplicate itemIds. The caller
        # is expected to consolidate duplicate lines (sum quantities) before
        # calling us; we don't guess intent here.
        seen_ids: set = set()
        for raw in items:
            iid = str(raw.get("itemId") or "").strip()
            if not iid:
                raise OrderAdjustmentError("ITEM_ID_REQUIRED", "Every item must have an itemId")
            if iid in seen_ids:
                raise OrderAdjustmentError(
                    "DUPLICATE_ITEM_ID",
                    f"Item {iid} appears multiple times in items; consolidate into a single entry with summed quantity",
                )
            seen_ids.add(iid)
            try:
                q = int(raw.get("quantity") or 0)
            except (TypeError, ValueError):
                q = 0
            if q <= 0:
                raise OrderAdjustmentError(
                    "INVALID_QUANTITY", f"Quantity for {iid} must be > 0 (use exclusion to remove)"
                )

        order = OrderService.get_order(order_id)
        if not order:
            raise OrderAdjustmentError("ORDER_NOT_FOUND", "Order not found", http_status=404)
        if order.status not in _ADJUSTABLE_STATUSES:
            raise OrderAdjustmentError(
                "ORDER_NOT_ADJUSTABLE",
                f"Order status {order.status} does not allow item adjustments",
                http_status=409,
            )

        existing_pending = OrderAdjustmentService._has_unresolved_refund_row(order_id)
        if existing_pending:
            raise OrderAdjustmentError(
                "PRIOR_REFUND_UNRESOLVED",
                "A prior adjustment refund is still pending; resolve it before stacking another",
                http_status=409,
            )

        original_grand_total = (
            float(order.original_grand_total)
            if order.original_grand_total is not None
            else float(order.grand_total or 0)
        )
        prepaid_amount = (
            float(order.prepaid_amount)
            if order.prepaid_amount is not None
            else OrderAdjustmentService._infer_prepaid_amount(order)
        )

        # 1) Diff requested canonical list against the current order. We never
        # re-price something the customer already accepted: existing lines
        # keep their snapshot (only quantity may move). Genuinely-new lines
        # are enriched fresh against MenuService for authoritative pricing.
        current_items_list: List[Dict[str, Any]] = list(order.items or [])
        current_by_id: Dict[str, Dict[str, Any]] = {}
        for it in current_items_list:
            iid = str(it.get("itemId") or it.get("item_id") or "").strip()
            if iid:
                current_by_id[iid] = it

        requested_id_set = set(str(r.get("itemId") or "").strip() for r in items)
        current_id_set = set(current_by_id.keys())

        removed_item_ids: List[str] = sorted(current_id_set - requested_id_set)
        added_item_ids: List[str] = []
        quantity_changes: List[Dict[str, Any]] = []

        new_items_to_enrich: List[Dict[str, Any]] = []
        merged_slots: List[Optional[Dict[str, Any]]] = []

        for raw in items:
            iid = str(raw.get("itemId") or "").strip()
            new_qty = int(raw.get("quantity") or 1)
            existing = current_by_id.get(iid)
            if existing:
                old_qty = int(existing.get("quantity") or 1)
                if old_qty != new_qty:
                    snapshot = dict(existing)
                    snapshot["quantity"] = new_qty
                    merged_slots.append(snapshot)
                    quantity_changes.append(
                        {"itemId": iid, "oldQuantity": old_qty, "newQuantity": new_qty}
                    )
                else:
                    merged_slots.append(dict(existing))
            else:
                merged_slots.append(None)  # placeholder filled after enrichment
                new_items_to_enrich.append(raw)
                added_item_ids.append(iid)

        # No-op guard: if nothing was removed, added, or quantity-changed, the
        # caller submitted the current state verbatim. Reject so we don't
        # mint an empty audit row + zero-delta settlement.
        if not removed_item_ids and not added_item_ids and not quantity_changes:
            raise OrderAdjustmentError(
                "NO_CHANGE",
                "items list is identical to the current order; nothing to adjust",
            )

        enriched_new_items, _ = OrderAdjustmentService._enrich_items(
            order.restaurant_id, new_items_to_enrich, order.order_type
        )
        enriched_by_id = {str(e["itemId"]): e for e in enriched_new_items}

        merged_items: List[Dict[str, Any]] = []
        for i, slot in enumerate(merged_slots):
            if slot is not None:
                merged_items.append(slot)
            else:
                iid = str(items[i].get("itemId") or "").strip()
                merged_items.append(enriched_by_id[iid])

        if not merged_items:
            raise OrderAdjustmentError(
                "ORDER_WOULD_BE_EMPTY",
                "Adjustment would leave the order with zero items; cancel the order instead",
            )

        new_food_total = round(
            sum(
                (float(it.get("price") or 0) + float(it.get("addOnTotal") or 0))
                * int(it.get("quantity") or 1)
                for it in merged_items
            ),
            2,
        )

        delivery_fee = float(order.delivery_fee or 0)
        platform_fee = float(order.platform_fee or 0)
        new_grand_total = round(new_food_total + delivery_fee + platform_fee, 2)
        delta = round(new_grand_total - original_grand_total, 2)

        new_amount_due_at_delivery = round(max(new_grand_total - prepaid_amount, 0.0), 2)

        synthetic_order = OrderAdjustmentService._make_synthetic_order(order, merged_items, new_grand_total)
        new_revenue, items_with_commission = compute_revenue(synthetic_order)

        adjustment_id = generate_id("ADJ")
        settlement_type, payment_ids_affected = OrderAdjustmentService._apply_payment_settlement(
            order=order,
            adjustment_id=adjustment_id,
            delta=delta,
            new_grand_total=new_grand_total,
        )

        old_restaurant_payout = OrderAdjustmentService._restaurant_payout(order.revenue)
        new_restaurant_payout = OrderAdjustmentService._restaurant_payout(new_revenue)
        restaurant_delta = round(new_restaurant_payout - old_restaurant_payout, 2)

        adjustment_record: Dict[str, Any] = {
            "adjustmentId": adjustment_id,
            "at": now_ist_iso(),
            "reason": reason,
            "opsUser": ops_user,
            "removedItemIds": removed_item_ids,
            "addedItemIds": added_item_ids,
            "addedItems": enriched_new_items,
            "quantityChanges": quantity_changes,
            "oldItems": current_items_list,
            "newItems": items_with_commission,
            "oldGrandTotal": original_grand_total,
            "newGrandTotal": new_grand_total,
            "previousGrandTotal": float(order.grand_total or 0),
            "delta": delta,
            "settlementType": settlement_type,
            "paymentIdsAffected": payment_ids_affected,
            "restaurantPayoutDelta": restaurant_delta,
        }

        appended_adjustments = list(order.adjustments or []) + [adjustment_record]

        order_updates: Dict[str, Any] = {
            "items": items_with_commission,
            "foodTotal": round(new_food_total, 2),
            "grandTotal": new_grand_total,
            "revenue": new_revenue,
            "originalGrandTotal": original_grand_total,
            "prepaidAmount": prepaid_amount,
            "amountDueAtDelivery": new_amount_due_at_delivery,
            "adjustments": appended_adjustments,
            "wasAdjusted": True,
        }
        OrderService.update_order(order_id, order_updates)
        logger.info(
            f"[orderId={order_id}] Ops adjustment {adjustment_id} applied: "
            f"delta={delta} settlement={settlement_type} newGrandTotal={new_grand_total} "
            f"removed={removed_item_ids} added={added_item_ids} qtyChanges={len(quantity_changes)}"
        )

        if abs(restaurant_delta) > 0.005:
            try:
                RestaurantEarningsService.add_item_adjustment(
                    restaurant_id=order.restaurant_id,
                    order_id=order_id,
                    adjustment_id=adjustment_id,
                    delta_amount=restaurant_delta,
                )
            except Exception as e:
                logger.error(
                    f"[orderId={order_id}] Failed to write restaurant earnings delta "
                    f"for adjustment {adjustment_id}: {e}",
                    exc_info=True,
                )

        OrderAdjustmentService._fire_notifications(
            order=order,
            adjustment_id=adjustment_id,
            delta=delta,
            new_grand_total=new_grand_total,
            amount_due_at_delivery=new_amount_due_at_delivery,
            settlement_type=settlement_type,
        )

        return {
            "adjustmentId": adjustment_id,
            "orderId": order_id,
            "delta": delta,
            "newGrandTotal": new_grand_total,
            "originalGrandTotal": original_grand_total,
            "amountDueAtDelivery": new_amount_due_at_delivery,
            "settlementType": settlement_type,
            "paymentIdsAffected": payment_ids_affected,
            "restaurantPayoutDelta": restaurant_delta,
        }

    # ---------- helpers ---------------------------------------------------

    @staticmethod
    def _infer_prepaid_amount(order: Order) -> float:
        """Best-effort prepaidAmount for orders created before the field existed.

        COD orders pay 0 online. Prepaid orders paid the full grandTotal at
        CONFIRMED time. This is only used as a fallback when the snapshot
        hasn't been stamped yet (e.g., very old orders).
        """
        pm = (order.payment_method or "").upper()
        pc = (order.payment_channel or "").upper()
        if pm == "COD" or pc in ("COD_AT_DELIVERY", "UPI_QR_AT_RIDER"):
            return 0.0
        return float(order.grand_total or 0)

    @staticmethod
    def _has_unresolved_refund_row(order_id: str) -> bool:
        try:
            rows = PaymentService.get_payments_for_order(order_id)
        except Exception as e:
            logger.warning(f"[orderId={order_id}] Could not check pending refund rows: {e}")
            return False
        for p in rows:
            if (p.payment_status or "").upper() == Payment.STATUS_ADJUSTMENT_PENDING_REFUND:
                return True
        return False

    @staticmethod
    def _enrich_items(
        restaurant_id: str,
        items_input: List[Dict[str, Any]],
        order_type: str,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Mirror the enrichment block in /payments/initiate so revenue math
        stays consistent. Returns (enriched_items, food_total).
        """
        enriched: List[Dict[str, Any]] = []
        food_total = 0.0
        for raw in items_input:
            item_id = raw.get("itemId")
            if not item_id:
                raise OrderAdjustmentError("ITEM_ID_REQUIRED", "Every item must have an itemId")
            try:
                quantity = int(raw.get("quantity", 1) or 1)
            except (TypeError, ValueError):
                quantity = 1
            if quantity <= 0:
                raise OrderAdjustmentError("INVALID_QUANTITY", f"Quantity for {item_id} must be > 0")

            menu_item = MenuService.get_menu_item(restaurant_id, item_id)
            if not menu_item:
                raise OrderAdjustmentError(
                    "MENU_ITEM_NOT_FOUND",
                    f"Item {item_id} not found on restaurant {restaurant_id}",
                )

            # Defence-in-depth: a delivery order must not contain theater items
            # and vice-versa (mirrors the check in /payments/initiate).
            if order_type == Order.ORDER_TYPE_PICKUP and not menu_item.theater_mode:
                raise OrderAdjustmentError(
                    "NOT_A_THEATER_ITEM",
                    f"Item {item_id} is not a theater item",
                )
            if order_type == Order.ORDER_TYPE_DELIVERY and menu_item.theater_mode:
                raise OrderAdjustmentError(
                    "THEATER_ITEM_IN_DELIVERY",
                    f"Theater item {item_id} cannot be in a delivery order",
                )

            pricing = CouponService.get_menu_item_prices(menu_item)
            customer_price = float(pricing.get("price") or 0.0)
            restaurant_price = float(getattr(menu_item, "restaurant_price", 0) or 0)
            hike_percentage = float(getattr(menu_item, "hike_percentage", 0) or 0)
            item_offer_coupon_code = getattr(menu_item, "item_offer_coupon_code", None)
            coupon_issued_by = pricing.get("couponIssuedBy")
            item_discount_amount = float(pricing.get("discountAmount", 0.0) or 0.0)

            # Server-authoritative add-on resolution. Client only sends the
            # selected optionIds; the menu owns the names + extra prices.
            try:
                resolved_addons, add_on_total = menu_item.resolve_requested_addons(
                    raw.get("addOns")
                )
            except ValueError as ve:
                raise OrderAdjustmentError("INVALID_ADDON", str(ve))

            # MenuItem uses `item_name` (DynamoDB field itemName), not `name`.
            # Always use the menu's name so the line label can't be spoofed.
            item_display_name = str(menu_item.item_name or "").strip()
            if not item_display_name:
                raise OrderAdjustmentError(
                    "MENU_ITEM_NAME_MISSING",
                    f"Item {item_id} has no display name on the menu record",
                )

            enriched.append({
                "itemId": item_id,
                "name": item_display_name,
                "quantity": quantity,
                "price": customer_price,
                "isVeg": getattr(menu_item, "is_veg", None),
                "restaurantPrice": restaurant_price,
                "hikePercentage": hike_percentage,
                "itemOfferCouponCode": item_offer_coupon_code,
                "couponIssuedBy": coupon_issued_by,
                "itemDiscountAmount": item_discount_amount,
                "addOns": resolved_addons,
                "addOnTotal": add_on_total,
            })
            food_total += (customer_price + add_on_total) * quantity

        return enriched, round(food_total, 2)

    @staticmethod
    def _make_synthetic_order(order: Order, items: List[Dict[str, Any]], grand_total: float) -> Order:
        """Build a transient Order with the new items/totals so we can hand
        it to `compute_revenue` without mutating the persisted row first."""
        return Order(
            order_id=order.order_id,
            customer_phone=order.customer_phone,
            receiver_phone=order.receiver_phone,
            restaurant_id=order.restaurant_id,
            items=items,
            food_total=float(grand_total - (order.delivery_fee or 0) - (order.platform_fee or 0)),
            delivery_fee=float(order.delivery_fee or 0),
            platform_fee=float(order.platform_fee or 0),
            grand_total=grand_total,
            status=order.status,
            rider_id=order.rider_id,
            restaurant_name=order.restaurant_name,
            payment_id=order.payment_id,
            payment_method=order.payment_method,
            payment_channel=order.payment_channel,
            calculated_fee_response=order.calculated_fee_response,
            created_at=order.created_at,
            order_type=order.order_type,
        )

    @staticmethod
    def _restaurant_payout(revenue: Optional[Dict[str, Any]]) -> float:
        if not revenue or not isinstance(revenue, dict):
            return 0.0
        rr = revenue.get("restaurantRevenue") or {}
        try:
            return float(rr.get("finalPayout") or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _apply_payment_settlement(
        order: Order,
        adjustment_id: str,
        delta: float,
        new_grand_total: float,
    ) -> Tuple[str, List[str]]:
        """Write the payment-side of the adjustment. Returns (settlement_type, [paymentIds])."""
        original_payment_id = order.payment_id
        is_cod_order = OrderAdjustmentService._is_cod_order(order)

        if is_cod_order:
            # Update the existing COD payment row's amount in place. Rider
            # will collect the new total at delivery.
            if not original_payment_id:
                raise OrderAdjustmentError(
                    "MISSING_PAYMENT_ID",
                    "COD order has no linked payment row to update",
                    http_status=409,
                )
            PaymentService.update_payment(
                original_payment_id,
                {"amount": float(new_grand_total)},
            )
            logger.info(
                f"[orderId={order.order_id}] COD payment {original_payment_id} amount "
                f"updated in place to {new_grand_total}"
            )
            return SETTLEMENT_COD_IN_PLACE, [original_payment_id]

        # Prepaid path
        if abs(delta) < 0.005:
            return SETTLEMENT_COD_IN_PLACE, [original_payment_id] if original_payment_id else []

        if delta > 0:
            # Customer owes more; rider collects the delta as a COD top-up.
            new_payment_id = generate_id("PAY")
            top_up = Payment(
                payment_id=new_payment_id,
                customer_phone=order.customer_phone,
                restaurant_id=order.restaurant_id,
                restaurant_name=order.restaurant_name or "",
                amount=round(delta, 2),
                payment_status=Payment.STATUS_INITIATED,
                payment_method=Payment.METHOD_COD,
                payment_channel=Payment.PAYMENT_CHANNEL_COD_AT_DELIVERY,
                order_id=order.order_id,
                parent_payment_id=original_payment_id,
                adjustment_id=adjustment_id,
            )
            PaymentService.create_payment(top_up)
            logger.info(
                f"[orderId={order.order_id}] Prepaid upward adjustment: created COD top-up "
                f"payment {new_payment_id} amount={delta}"
            )
            ids = [new_payment_id]
            if original_payment_id:
                ids.insert(0, original_payment_id)
            return SETTLEMENT_COD_TOPUP, ids

        # delta < 0: prepaid downward → ADJUSTMENT_PENDING_REFUND row.
        new_payment_id = generate_id("PAY")
        refund_row = Payment(
            payment_id=new_payment_id,
            customer_phone=order.customer_phone,
            restaurant_id=order.restaurant_id,
            restaurant_name=order.restaurant_name or "",
            amount=round(abs(delta), 2),
            payment_status=Payment.STATUS_ADJUSTMENT_PENDING_REFUND,
            payment_method=Payment.METHOD_REFUND_ADJUSTMENT,
            order_id=order.order_id,
            parent_payment_id=original_payment_id,
            adjustment_id=adjustment_id,
        )
        PaymentService.create_payment(refund_row)
        logger.info(
            f"[orderId={order.order_id}] Prepaid downward adjustment: created refund row "
            f"{new_payment_id} amount={abs(delta)} (status=ADJUSTMENT_PENDING_REFUND)"
        )
        ids = [new_payment_id]
        if original_payment_id:
            ids.insert(0, original_payment_id)
        return SETTLEMENT_REFUND_ADJUSTMENT, ids

    @staticmethod
    def _is_cod_order(order: Order) -> bool:
        pm = (order.payment_method or "").upper()
        pc = (order.payment_channel or "").upper()
        return pm == Payment.METHOD_COD or pc in (
            Payment.PAYMENT_CHANNEL_COD_AT_DELIVERY,
            Payment.PAYMENT_CHANNEL_UPI_QR_AT_RIDER,
        )

    @staticmethod
    def _fire_notifications(
        order: Order,
        adjustment_id: str,
        delta: float,
        new_grand_total: float,
        amount_due_at_delivery: float,
        settlement_type: str,
    ) -> None:
        """Best-effort FCM pushes. Failures are logged but don't roll back."""
        try:
            customer_token = OrderAdjustmentService._lookup_customer_fcm_token(order.customer_phone)
            if customer_token:
                NotificationService.send_order_adjusted_notification(
                    fcm_token=customer_token,
                    order_id=order.order_id,
                    restaurant_name=order.restaurant_name or "",
                    delta=delta,
                    new_grand_total=new_grand_total,
                    amount_due_at_delivery=amount_due_at_delivery,
                    settlement_type=settlement_type,
                    audience="CUSTOMER",
                )
        except Exception as e:
            logger.warning(
                f"[orderId={order.order_id}] customer adjustment notification failed: {e}"
            )

        if not order.rider_id:
            return
        try:
            rider_token = OrderAdjustmentService._lookup_rider_fcm_token(order.rider_id)
            if rider_token:
                NotificationService.send_order_adjusted_notification(
                    fcm_token=rider_token,
                    order_id=order.order_id,
                    restaurant_name=order.restaurant_name or "",
                    delta=delta,
                    new_grand_total=new_grand_total,
                    amount_due_at_delivery=amount_due_at_delivery,
                    settlement_type=settlement_type,
                    audience="RIDER",
                )
        except Exception as e:
            logger.warning(
                f"[orderId={order.order_id}] rider adjustment notification failed: {e}"
            )

    @staticmethod
    def _lookup_customer_fcm_token(customer_phone: str) -> Optional[str]:
        try:
            from services.user_service import UserService
            user = UserService.get_user_by_role(customer_phone, "CUSTOMER")
            return getattr(user, "fcm_token", None) if user else None
        except Exception:
            return None

    @staticmethod
    def _lookup_rider_fcm_token(rider_id: str) -> Optional[str]:
        try:
            from services.rider_service import RiderService
            from services.user_service import UserService
            rider = RiderService.get_rider(rider_id)
            if not rider or not rider.phone:
                return None
            user = UserService.get_user_by_role(rider.phone, "RIDER")
            return getattr(user, "fcm_token", None) if user else None
        except Exception:
            return None

    @staticmethod
    def mark_refunded(payment_id: str, ops_user: str, refund_reference: Optional[str] = None) -> Dict[str, Any]:
        """Flip an ADJUSTMENT_PENDING_REFUND row to REFUNDED once ops has
        executed the manual refund in their external system."""
        payment = PaymentService.get_payment(payment_id)
        if not payment:
            raise OrderAdjustmentError("PAYMENT_NOT_FOUND", "Payment not found", http_status=404)
        if (payment.payment_status or "").upper() != Payment.STATUS_ADJUSTMENT_PENDING_REFUND:
            raise OrderAdjustmentError(
                "INVALID_PAYMENT_STATUS",
                f"Payment is in status {payment.payment_status}, expected ADJUSTMENT_PENDING_REFUND",
                http_status=409,
            )
        updates: Dict[str, Any] = {
            "paymentStatus": Payment.STATUS_REFUNDED,
            "refundAmount": payment.amount,
            "errorCode": refund_reference or f"OPS_REFUND_{ops_user}",
            "errorDescription": (
                f"Manual refund executed by ops user={ops_user} ref={refund_reference or 'n/a'}"
            ),
        }
        PaymentService.update_payment(payment_id, updates)
        logger.info(
            f"[paymentId={payment_id}] Marked REFUNDED by ops user={ops_user} ref={refund_reference}"
        )
        return {
            "paymentId": payment_id,
            "paymentStatus": Payment.STATUS_REFUNDED,
            "refundAmount": payment.amount,
        }
