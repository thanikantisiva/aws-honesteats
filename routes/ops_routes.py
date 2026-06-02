"""Ops-facing routes.

These endpoints sit under `/api/v1/ops/*` and are protected by the
ADMIN_API_KEY (validated in `app.py` auth middleware before the request
reaches the handler). They are designed to be called by an internal admin
tool (e.g., Retool).

Restaurant app equivalent: POST
`/api/v1/restaurants/{restaurantId}/orders/{orderId}/adjust-items`
(restaurant JWT — see `restaurant_routes.py`).
"""
from aws_lambda_powertools import Logger, Tracer, Metrics

from services.order_adjustment_service import (
    OrderAdjustmentService,
    OrderAdjustmentError,
)

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_ops_routes(app):
    """Register ops admin routes (require admin API key via app.py middleware)."""

    @app.post("/api/v1/ops/orders/<order_id>/adjust-items")
    @tracer.capture_method
    def adjust_order_items(order_id: str):
        """Apply a diff (remove + add) to items on an in-flight order, recompute
        revenue, and post the payment-side adjustment (in-place for COD;
        delta row for prepaid).

        Body:
          - removeItemIds: list of itemIds currently on the order to remove
          - addItems: list of `{itemId, quantity, ...}` to add (re-priced from menu)
          - reason: free-text justification (audit)
          - opsUser: identifier of the ops user (audit)

        At least one of removeItemIds or addItems must be provided.
        """
        try:
            body = app.current_event.json_body or {}
            remove_item_ids = body.get("removeItemIds") or []
            add_items = body.get("addItems") or []
            reason = body.get("reason") or ""
            ops_user = body.get("opsUser") or ""

            logger.info(
                f"[orderId={order_id}] Ops adjust-items request "
                f"opsUser={ops_user} remove={remove_item_ids} addCount={len(add_items)} "
                f"reason='{reason[:60]}'"
            )

            result = OrderAdjustmentService.adjust_items(
                order_id=order_id,
                remove_item_ids=remove_item_ids,
                add_items=add_items,
                reason=reason,
                ops_user=ops_user,
            )

            metrics.add_metric(name="OpsOrderAdjusted", unit="Count", value=1)
            return result, 200
        except OrderAdjustmentError as e:
            logger.warning(
                f"[orderId={order_id}] Ops adjust-items rejected: code={e.code} msg={e.message}"
            )
            metrics.add_metric(name="OpsOrderAdjustRejected", unit="Count", value=1)
            return {"error": e.code, "message": e.message}, e.http_status
        except Exception as e:
            logger.error(
                f"[orderId={order_id}] Ops adjust-items failed", exc_info=True
            )
            return {"error": "AdjustItemsFailed", "message": str(e)}, 500

    @app.post("/api/v1/ops/payments/<payment_id>/mark-refunded")
    @tracer.capture_method
    def mark_payment_refunded(payment_id: str):
        """Confirm that ops has executed the manual refund for an
        ADJUSTMENT_PENDING_REFUND payment row. Flips the row to REFUNDED.

        Body:
          - opsUser: ops user identifier (audit)
          - refundReference: optional external reference (bank txn id, etc.)
        """
        try:
            body = app.current_event.json_body or {}
            ops_user = body.get("opsUser") or ""
            refund_reference = body.get("refundReference")

            logger.info(
                f"[paymentId={payment_id}] Ops mark-refunded request opsUser={ops_user} "
                f"ref={refund_reference}"
            )

            result = OrderAdjustmentService.mark_refunded(
                payment_id=payment_id,
                ops_user=ops_user,
                refund_reference=refund_reference,
            )

            metrics.add_metric(name="OpsPaymentMarkedRefunded", unit="Count", value=1)
            return result, 200
        except OrderAdjustmentError as e:
            logger.warning(
                f"[paymentId={payment_id}] Ops mark-refunded rejected: "
                f"code={e.code} msg={e.message}"
            )
            return {"error": e.code, "message": e.message}, e.http_status
        except Exception as e:
            logger.error(
                f"[paymentId={payment_id}] Ops mark-refunded failed", exc_info=True
            )
            return {"error": "MarkRefundedFailed", "message": str(e)}, 500
