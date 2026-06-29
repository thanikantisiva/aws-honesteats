"""Ops-facing routes.

These endpoints sit under `/api/v1/ops/*` and are protected by the
ADMIN_API_KEY (validated in `app.py` auth middleware before the request
reaches the handler). They are designed to be called by an internal admin
tool (e.g., Retool).

Restaurant app equivalent: POST
`/api/v1/restaurants/{restaurantId}/orders/{orderId}/adjust-items`
(restaurant JWT — see `restaurant_routes.py`).
"""
import json
import os

import boto3
from aws_lambda_powertools import Logger, Tracer, Metrics

from services.order_adjustment_service import (
    OrderAdjustmentService,
    OrderAdjustmentError,
)
from services.notification_service import NotificationService
from services.user_service import UserService
from utils import normalize_phone

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def _coerce_bool(value) -> bool:
    """Coerce JSON / admin-tool values to a real bool (handles "false" strings)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "on"}
    return bool(value)


def register_ops_routes(app):
    """Register ops admin routes (require admin API key via app.py middleware)."""

    @app.post("/api/v1/ops/orders/<order_id>/adjust-items")
    @tracer.capture_method
    def adjust_order_items(order_id: str):
        """Replace the items on an in-flight order with a canonical desired
        final list, recompute revenue, and post the payment-side adjustment
        (in-place for COD; delta row for prepaid).

        Body:
          - items: FULL desired final list of items on the order, each
            `{itemId, quantity, ...}`. To remove an item, omit it. To change
            quantity, send the new quantity. itemIds must be unique within
            the list and every quantity must be > 0.
          - reason: free-text justification (audit)
          - opsUser: identifier of the ops user (audit)

        Example: order currently has 1x2, 2x3, 3x5. To swap 1 unit of item 1
        for item 4, send items = [1x1, 2x3, 3x5, 4x1].
        """
        try:
            body = app.current_event.json_body or {}
            items = body.get("items") or []
            reason = body.get("reason") or ""
            ops_user = body.get("opsUser") or ""

            logger.info(
                f"[orderId={order_id}] Ops adjust-items request "
                f"opsUser={ops_user} itemCount={len(items)} "
                f"reason='{str(reason)[:60]}'"
            )

            result = OrderAdjustmentService.adjust_items(
                order_id=order_id,
                items=items,
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

    @app.post("/api/v1/ops/users/<phone>/cod-toggles")
    @tracer.capture_method
    def set_user_cod_toggles(phone: str):
        """Set a customer's COD risk-control flags (disableCod / forceCod).

        Admin-only: gated by the ADMIN_API_KEY in the app.py auth middleware
        (the `/api/v1/ops` prefix). These flags must never be settable by the
        customer themselves — forceCod would let a customer bypass COD limits.

        Body:
          - disableCod: optional bool — deny COD for this customer
          - forceCod:   optional bool — always allow COD for this customer
                        (wins over disableCod and all global rules)
          - opsUser:    identifier of the ops user (audit)

        At least one of disableCod / forceCod must be present.
        """
        try:
            normalized = normalize_phone(phone)
            if not normalized:
                return {"error": "Invalid phone number"}, 400

            body = app.current_event.json_body or {}
            ops_user = body.get("opsUser") or ""

            updates = {}
            if "disableCod" in body:
                updates["disableCod"] = _coerce_bool(body.get("disableCod"))
            if "forceCod" in body:
                updates["forceCod"] = _coerce_bool(body.get("forceCod"))

            if not updates:
                return {
                    "error": "At least one of disableCod or forceCod is required"
                }, 400

            # 404 on a missing customer — update_user would otherwise upsert a
            # phantom CUSTOMER row from these toggle-only updates.
            existing = UserService.get_user_by_role(normalized, "CUSTOMER")
            if not existing:
                return {"error": "Customer not found"}, 404

            logger.info(
                f"[phone={normalized[:5]}***] Ops set COD toggles "
                f"opsUser={ops_user} updates={updates}"
            )

            updated_user = UserService.update_user(normalized, "CUSTOMER", updates)
            metrics.add_metric(name="OpsCodTogglesUpdated", unit="Count", value=1)
            return updated_user.to_dict(), 200
        except Exception as e:
            logger.error(
                f"[phone={phone}] Ops set COD toggles failed", exc_info=True
            )
            return {"error": "SetCodTogglesFailed", "message": str(e)}, 500

    @app.post("/api/v1/ops/broadcast")
    @tracer.capture_method
    def send_custom_broadcast():
        """Fire the customer broadcast notification.

        Admin-only (ADMIN_API_KEY via the `/api/v1/ops` prefix). Sends to all active
        CUSTOMER users in the operating-area geohash partition. The send can take
        minutes for thousands of recipients — far past the API Gateway timeout — so we
        invoke the `custom_notification_handler` Lambda ASYNCHRONOUSLY and return at
        once. The payload mirrors what the admin Notifications page builds.

        Body:
          - customMessage: required body text ("{{name}}" -> recipient first name)
          - title:         optional title (default "Notification")
          - imageUrl:      optional https image shown as a large notification image
          - data:          optional FCM data payload object
        """
        try:
            body = app.current_event.json_body or {}

            custom_message = str(body.get("customMessage") or "").strip()
            if not custom_message:
                return {"error": "customMessage is required"}, 400

            event = {"customMessage": custom_message}
            title = str(body.get("title") or "").strip()
            if title:
                event["title"] = title
            image_url = str(body.get("imageUrl") or "").strip()
            if image_url:
                if not image_url.lower().startswith("https://"):
                    return {"error": "imageUrl must be an https URL"}, 400
                event["imageUrl"] = image_url
            data = body.get("data")
            if isinstance(data, dict) and data:
                event["data"] = data

            function_name = os.environ.get("CUSTOM_NOTIFICATION_FUNCTION_NAME") or (
                f"rork-honesteats-custom-notification-{os.environ.get('ENVIRONMENT', 'dev')}"
            )

            logger.info(
                f"Ops broadcast queued -> {function_name} "
                f"title='{title[:40]}' hasImage={bool(image_url)}"
            )
            response = boto3.client("lambda").invoke(
                FunctionName=function_name,
                InvocationType="Event",  # async fire-and-forget (don't block the request)
                Payload=json.dumps(event).encode("utf-8"),
            )

            metrics.add_metric(name="OpsBroadcastQueued", unit="Count", value=1)
            return {
                "status": "queued",
                "message": "Broadcast started — sending to all active customers.",
                "function": function_name,
                "invokeStatus": response.get("StatusCode"),
            }, 202
        except Exception as e:
            logger.error("Ops broadcast failed", exc_info=True)
            return {"error": "BroadcastFailed", "message": str(e)}, 500

    @app.post("/api/v1/ops/notifications/test-customer")
    @tracer.capture_method
    def send_customer_test_notification():
        """Send one customer push notification for admin testing.

        Admin-only (ADMIN_API_KEY via the `/api/v1/ops` prefix). This mirrors
        the broadcast payload, but resolves exactly one CUSTOMER by phone and
        sends to that customer's latest FCM token.

        Body:
          - phone:         required customer phone number
          - customMessage: required body text ("{{name}}" -> first name)
          - title:         optional title (default "Notification")
          - imageUrl:      optional https image shown as a large notification image
          - data:          optional FCM data payload object
        """
        try:
            body = app.current_event.json_body or {}
            phone = normalize_phone(body.get("phone"))
            if not phone:
                return {"error": "phone is required"}, 400

            custom_message = str(body.get("customMessage") or "").strip()
            if not custom_message:
                return {"error": "customMessage is required"}, 400

            title = str(body.get("title") or "").strip() or "Notification"
            image_url = str(body.get("imageUrl") or "").strip()
            if image_url and not image_url.lower().startswith("https://"):
                return {"error": "imageUrl must be an https URL"}, 400

            user = UserService.get_user_by_role(phone, "CUSTOMER")
            if not user:
                return {"error": "Customer not found"}, 404
            if not user.fcm_token:
                return {
                    "error": "Customer has no FCM token",
                    "message": "Open the customer app on this phone and allow notifications, then try again.",
                }, 409

            first_name = ((user.name or "").strip().split() or ["there"])[0]
            rendered_title = title.replace("{{name}}", first_name)
            rendered_body = custom_message.replace("{{name}}", first_name)

            payload = body.get("data") if isinstance(body.get("data"), dict) else {}
            payload = dict(payload)
            payload.setdefault("type", "custom_test")
            payload["targetPhone"] = phone

            logger.info(
                f"Ops test notification sending to customer {phone[:5]}*** "
                f"hasImage={bool(image_url)}"
            )
            ok = NotificationService.send_via_firebase(
                fcm_token=user.fcm_token,
                title=rendered_title,
                data=payload,
                body=rendered_body,
                image_url=image_url or None,
            )
            if not ok:
                return {"error": "NotificationSendFailed"}, 502

            metrics.add_metric(name="OpsCustomerTestNotificationSent", unit="Count", value=1)
            return {
                "status": "sent",
                "message": "Test notification sent to the selected customer.",
                "phone": phone,
                "hasImage": bool(image_url),
            }, 200
        except Exception as e:
            logger.error("Ops customer test notification failed", exc_info=True)
            return {"error": "TestNotificationFailed", "message": str(e)}, 500
