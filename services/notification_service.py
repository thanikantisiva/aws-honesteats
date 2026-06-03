"""Notification service for sending push notifications via Firebase FCM"""
import os
import json
from typing import Optional
from datetime import timedelta
from utils import normalize_phone
from utils.datetime_ist import now_ist_iso
from aws_lambda_powertools import Logger

logger = Logger()
RIDER_NOTIFICATION_CHANNEL_ID = "rider_orders_ring_v2"
RIDER_NOTIFICATION_SOUND = "new_order_ring"

# Import Firebase Admin SDK
try:
    import firebase_admin
    from firebase_admin import credentials
    FIREBASE_AVAILABLE = True
except ImportError:
    logger.warning("firebase-admin not installed, Firebase notifications disabled")
    FIREBASE_AVAILABLE = False

# Initialize Firebase Admin SDK (singleton) - only if using Firebase
_firebase_initialized = False


def initialize_firebase():
    """Initialize Firebase Admin SDK with service account"""
    global _firebase_initialized
    
    if _firebase_initialized:
        return
    
    if not FIREBASE_AVAILABLE:
        logger.error("Firebase Admin SDK not available")
        return
    
    try:
        # Prefer service account JSON from environment (SSM)
        from utils.ssm import get_secret
        service_account_json = get_secret("FIREBASE_SERVICE_ACCOUNT_JSON", "")
        if service_account_json:
            service_account_info = json.loads(service_account_json)
            cred = credentials.Certificate(service_account_info)
            firebase_admin.initialize_app(cred)
            _firebase_initialized = True
            logger.info("✅ Firebase Admin SDK initialized from environment")
            return

        # Fallback to file for local/dev
        import os.path
        service_account_path = os.path.join(os.path.dirname(__file__), '..', 'firebase-service-account.json')
        if os.path.exists(service_account_path):
            logger.info(f"📄 Loading Firebase service account from: {service_account_path}")
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred)
            _firebase_initialized = True
            logger.info("✅ Firebase Admin SDK initialized from file")
            return
        
        logger.warning("⚠️ Firebase service account not configured")
    except Exception as e:
        logger.warning(f"⚠️ Firebase initialization failed (push notifications disabled): {str(e)}")


class NotificationService:
    """Service for sending push notifications via Firebase FCM"""
    
    @staticmethod
    def send_via_firebase(
        fcm_token: str,
        title: str,
        data: dict,
        body: Optional[str] = None
    ) -> bool:
        """Send notification via Firebase Admin SDK (generic; use send_order_status_notification for order updates)."""
        if not FIREBASE_AVAILABLE:
            logger.warning("⚠️ Firebase Admin SDK not available - notification skipped")
            return False
            
        try:
            from firebase_admin import messaging
            
            initialize_firebase()
            
            if not _firebase_initialized:
                logger.warning("⚠️ Firebase not initialized - notification skipped")
                return False
            
            # Convert data dict values to strings (FCM requirement)
            string_data = {k: str(v) for k, v in data.items()}
            body_text = (body or "").strip()
            if body_text:
                string_data["title"] = title
                string_data["body"] = body_text
            android_channel_id = string_data.get("channelId") or None

            # Data-only messages let Notifee handle rendering on both foreground AND
            # background/killed app states, giving consistent sound + action buttons.
            # order_status and order_assigned go via this path so the JS handler
            # (displayNotificationFromRemoteMessage) always controls the notification.
            is_data_only = string_data.get("type") in (
                "order_status",
                "order_assigned",
            )

            # iOS: ensure distinct title and body so notification shows two lines (title bold, body below)
            if body_text:
                ios_alert_title = title
                ios_alert_body = body_text
            else:
                # No body passed (e.g. order status): split title on " : " or use full as body
                if " : " in title:
                    ios_alert_title, _, ios_alert_body = title.partition(" : ")
                elif ": " in title:
                    ios_alert_title, _, ios_alert_body = title.partition(": ")
                else:
                    ios_alert_title = "Order Update"
                    ios_alert_body = title

            # Send both Android and APNS config so FCM delivers correctly to iOS or Android.
            # iOS receives a visible-alert APNs payload regardless of `is_data_only` so the
            # system renders the notification in foreground/background/killed without
            # depending on the JS background handler (which today only logs).
            # `apns-push-type: alert` is required by APNs HTTP/2 for visible alerts.
            # `mutable_content=True` allows future Notification Service Extensions to enrich
            # the payload (e.g. attach images) without another backend deploy.
            logger.info("📱 Sending FCM message (Android + APNS config)")
            apns_config = messaging.APNSConfig(
                headers={
                    'apns-priority': '10',
                    'apns-push-type': 'alert',
                },
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        alert=messaging.ApsAlert(title=ios_alert_title, body=ios_alert_body),
                        sound='default',
                        badge=1,
                        mutable_content=True,
                    )
                )
            )
            if is_data_only:
                message = messaging.Message(
                    token=fcm_token,
                    data=string_data,
                    android=messaging.AndroidConfig(
                        priority="high",
                        collapse_key=string_data.get("orderId"),
                        ttl=timedelta(seconds=2419200)
                    ),
                    apns=apns_config
                )
            else:
                message = messaging.Message(
                    token=fcm_token,
                    data=string_data,
                    notification=messaging.Notification(title=title, body=body_text),
                    android=messaging.AndroidConfig(
                        priority="high",
                        notification=messaging.AndroidNotification(
                            title=title,
                            body=body_text,
                            sound=string_data.get("sound") or RIDER_NOTIFICATION_SOUND,
                            channel_id=android_channel_id,
                            icon="ic_launcher"
                        )
                    ),
                    apns=apns_config
                )

            logger.info(f"📤 Sending Firebase message to token: {fcm_token[:20]}...")
            response = messaging.send(message)
            logger.info(f"✅ Firebase notification sent successfully: {response}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending via Firebase: {str(e)}", exc_info=True)
            return False
    
    @staticmethod
    def send_order_status_notification(
        fcm_token: str,
        order_id: str,
        status: str,
        restaurant_name: str,
        item_name: Optional[str] = None,
        item_image_url: Optional[str] = None,
        updated_at: Optional[str] = None,
        rider_id: Optional[str] = None,
        rider_name: Optional[str] = None,
        customer_phone: Optional[str] = None
    ) -> bool:
        """
        Send order status update notification via FCM
        
        Args:
            fcm_token: User's FCM device token
            order_id: Order ID
            status: New order status
            restaurant_name: Restaurant name
            rider_id: Rider ID if assigned (optional)
            rider_name: Rider display name if available (optional)
            customer_phone: Customer phone for rating payload (optional)
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            # Map status to user-friendly messages (include rider name when available)
            rider_text = f" {rider_name}" if rider_name else ""
            status_messages = {
                'CONFIRMED': f'Order Confirmed : {restaurant_name} confirmed  your order🎉',
                'PREPARING': f'Preparing : {restaurant_name} is preparing your order',
                'READY_FOR_PICKUP': f'READY: Your order from {restaurant_name} is ready for pickup',
                'OUT_FOR_DELIVERY': f'Out For Delivery: {rider_text} is your delivery partner',
                'DELIVERED': f'Delivered: Your order from {restaurant_name} is delivered',
                'AWAITING_RIDER_ASSIGNMENT': f'Please wait: We are searching near by delivery partners',
                'RIDER_ASSIGNED': f'Order Assigned: Your order from {restaurant_name} has been assigned to a rider {rider_text}'
            }
            
            title = status_messages.get(status)
            
            logger.info(f"📱 Sending Firebase FCM notification")
            logger.info(f"   Token: {fcm_token[:30]}...")
            logger.info(f"   Title: {title}")

            api_base_url = os.environ.get("API_BASE_URL", "https://api.yumdude.com").rstrip("/")
            rating_endpoint = "/api/v1/ratings"

            notification_data = {
                "type": "order_status",
                "orderId": order_id,
                "status": status,
                "restaurantName": restaurant_name,
                "itemName": item_name or "",
                "itemImageUrl": item_image_url or "",
                "updatedAt": updated_at or now_ist_iso(),
                "riderId": rider_id or "",
                "riderName": rider_name or "",
                "title": title,
                "apiBaseUrl": api_base_url,
                "ratingEndpoint": rating_endpoint,
                "customerPhone": customer_phone or "",
            }
            
            # Send via Firebase FCM
            return NotificationService.send_via_firebase(
                fcm_token=fcm_token,
                title=title,
                data=notification_data
            )
            
        except Exception as e:
            logger.error(f"Error sending notification: {str(e)}")
            return False
    
    # The rider receives ONLY two notification statuses, both via this method:
    #   - OFFERED_TO_RIDER : an offer that rider can accept/reject
    #   - RIDER_ASSIGNED   : a direct assignment (e.g. all offers were rejected
    #                        and the system force-assigned the nearest rider)
    # No other rider-facing pushes are sent.
    RIDER_NOTIFY_OFFERED = "OFFERED_TO_RIDER"
    RIDER_NOTIFY_ASSIGNED = "RIDER_ASSIGNED"
    _RIDER_NOTIFY_ALLOWED = {RIDER_NOTIFY_OFFERED, RIDER_NOTIFY_ASSIGNED}

    @staticmethod
    def send_order_assigned_notification(
        rider_mobile: str,
        order_id: str,
        restaurant_name: str,
        delivery_fee: float,
        notification_status: str = "RIDER_ASSIGNED",
    ) -> bool:
        """
        Send a ride-alert notification to the rider.

        Only two values of ``notification_status`` are accepted:
          - ``OFFERED_TO_RIDER`` : offer, rider can accept/reject
          - ``RIDER_ASSIGNED``   : direct/force assignment
        Any other value is rejected to keep rider pushes constrained to the
        two ride-alert statuses the rider app actually handles.

        Args:
            rider_mobile: Rider's phone number
            order_id: Order ID
            restaurant_name: Restaurant name
            delivery_fee: Delivery fee amount
            notification_status: OFFERED_TO_RIDER | RIDER_ASSIGNED

        Returns:
            True if sent successfully, False otherwise
        """
        try:
            from utils.dynamodb import dynamodb_client

            if notification_status not in NotificationService._RIDER_NOTIFY_ALLOWED:
                logger.warning(
                    f"Refusing rider notification with unsupported status "
                    f"{notification_status!r}; allowed: "
                    f"{sorted(NotificationService._RIDER_NOTIFY_ALLOWED)}"
                )
                return False

            phone_key = normalize_phone(rider_mobile)
            if not phone_key:
                logger.warning(f"Invalid rider phone for assignment notification: {rider_mobile!r}")
                return False

            logger.info(
                f"📱 Sending rider notification status={notification_status} "
                f"to rider: {phone_key}"
            )

            # Get rider's FCM token from UsersTableV2 (composite key: phone + role)
            users_table = os.environ.get(
                "USERS_TABLE_NAME",
                f"food-delivery-users-{os.environ.get('ENVIRONMENT', 'dev')}",
            )

            user_response = dynamodb_client.get_item(
                TableName=users_table,
                Key={
                    "phone": {"S": phone_key},
                    "role": {"S": "RIDER"},
                },
            )

            if "Item" not in user_response:
                logger.warning(f"Rider not found in users table (normalized={phone_key}, raw={rider_mobile!r})")
                return False

            fcm_token = user_response["Item"].get("fcmToken", {}).get("S")
            if not fcm_token:
                logger.warning(f"No FCM token for rider: {phone_key}")
                return False

            is_offer = notification_status == NotificationService.RIDER_NOTIFY_OFFERED
            title = "New Order Offered 🛵" if is_offer else "New Order Assigned! 🛵"
            body = f"Pickup from {restaurant_name} • Earn ₹{delivery_fee:.0f}"

            logger.info(f"   Token: {fcm_token[:30]}...")
            logger.info(f"   Title: {title}")
            logger.info(f"   Body : {body}")

            notification_data = {
                "type": "order_assigned",
                "status": notification_status,
                "orderId": order_id,
                "restaurantName": restaurant_name,
                "deliveryFee": str(delivery_fee),
                "title": title,
                "body": body,
                "channelId": RIDER_NOTIFICATION_CHANNEL_ID,
                "sound": RIDER_NOTIFICATION_SOUND,
            }

            success = NotificationService.send_via_firebase(
                fcm_token=fcm_token,
                title=title,
                body=body,
                data=notification_data,
            )

            if success:
                logger.info(
                    f"✅ Rider notification sent (status={notification_status}, "
                    f"orderId={order_id})"
                )
            else:
                logger.error(
                    f"❌ Failed to send rider notification "
                    f"(status={notification_status}, orderId={order_id})"
                )

            return success

        except Exception as e:
            logger.error(f"Error sending rider notification: {str(e)}", exc_info=True)
            return False

    @staticmethod
    def is_invalid_fcm_token_error(error: Exception) -> bool:
        """Best-effort classifier for invalid/unregistered FCM registration tokens."""
        text = str(error or "").lower()
        return any(
            marker in text
            for marker in [
                "registration-token-not-registered",
                "notregistered",
                "unregistered",
                "invalid registration token",
                "invalidargument",
                "not a valid fcm registration token",
                "not a valid registration token",
                "requested entity was not found",
                "registration token is not a valid",
            ]
        )

    @staticmethod
    def send_restaurant_new_order_notification_with_result(
        fcm_token: str,
        order_id: str,
        restaurant_name: str,
        customer_phone: str,
        item_summary: str,
        item_count: int,
        amount: float,
        created_at: Optional[str] = None
    ) -> dict:
        """Send restaurant new-order notification and return structured result."""
        if not FIREBASE_AVAILABLE:
            logger.warning("Firebase Admin SDK not available - restaurant notification skipped")
            return {"success": False, "invalidToken": False}

        try:
            from firebase_admin import messaging

            initialize_firebase()

            if not _firebase_initialized:
                logger.warning("Firebase not initialized - restaurant notification skipped")
                return {"success": False, "invalidToken": False}

            item_label = "1 item" if item_count == 1 else f"{item_count} items"
            order_tail = (order_id or "").strip()[-4:] or "----"
            title_text = f"Rs.{amount:.0f} New Order"
            body_text = f"{item_label} | #{order_tail}"
            channel_id = "new_orders_critical"
            data = {
                "type": "restaurant_new_order",
                "orderId": order_id,
                "restaurantName": restaurant_name,
                "customerPhone": customer_phone or "",
                "itemSummary": item_summary or "",
                "itemCount": str(item_count),
                "amount": str(amount),
                "orderTail": order_tail,
                "status": "CONFIRMED",
                "createdAt": created_at or now_ist_iso(),
                "channelId": channel_id,
                "title": title_text,
                "body": body_text
            }

            # Notification block alongside data: Play Services renders the
            # high-importance system notification (with sound) even when the
            # app process is dead or stopped by an OEM task killer, removing
            # the dependency on the foreground OrderPollingService being alive
            # for the ring path. Data block is preserved so the in-app handler
            # can still refresh the orders list and launch OrderAlarmActivity
            # when the process is alive.
            message = messaging.Message(
                token=fcm_token,
                data=data,
                notification=messaging.Notification(title=title_text, body=body_text),
                android=messaging.AndroidConfig(
                    priority="high",
                    ttl=timedelta(seconds=900),
                    collapse_key=order_id,
                    notification=messaging.AndroidNotification(
                        title=title_text,
                        body=body_text,
                        channel_id=channel_id,
                        sound="telephone_ring",
                        icon="ic_launcher",
                        priority="max",
                        visibility="public",
                        default_vibrate_timings=True
                    )
                )
            )

            logger.info(f"Sending restaurant notification for orderId={order_id}")
            response = messaging.send(message)
            logger.info(f"Restaurant notification sent successfully: {response}")
            return {"success": True, "invalidToken": False}
        except Exception as e:
            invalid = NotificationService.is_invalid_fcm_token_error(e)
            logger.error(f"Error sending restaurant notification: {str(e)}", exc_info=True)
            return {"success": False, "invalidToken": invalid}

    @staticmethod
    def send_restaurant_new_order_notification(
        fcm_token: str,
        order_id: str,
        restaurant_name: str,
        customer_phone: str,
        item_summary: str,
        item_count: int,
        amount: float,
        created_at: Optional[str] = None
    ) -> bool:
        """Send a restaurant-side new order notification for the mobile restaurant app."""
        result = NotificationService.send_restaurant_new_order_notification_with_result(
            fcm_token=fcm_token,
            order_id=order_id,
            restaurant_name=restaurant_name,
            customer_phone=customer_phone,
            item_summary=item_summary,
            item_count=item_count,
            amount=amount,
            created_at=created_at
        )
        return bool(result.get("success"))

    @staticmethod
    def send_order_adjusted_notification(
        fcm_token: str,
        order_id: str,
        restaurant_name: str,
        delta: float,
        new_grand_total: float,
        amount_due_at_delivery: float,
        settlement_type: str,
        audience: str,
    ) -> bool:
        """Notify customer or rider that ops adjusted items on an in-flight order.

        Args:
            fcm_token: recipient device FCM token
            order_id: order id (always included in the data payload so the
                receiving app can re-fetch the order)
            restaurant_name: display name of the restaurant
            delta: newGrandTotal - originalGrandTotal (signed; positive when
                customer owes more, negative on a refund)
            new_grand_total: recomputed order total after adjustment
            amount_due_at_delivery: rupees the rider should collect at delivery
                (0 for a downward prepaid refund; full new total for COD)
            settlement_type: one of "COD_IN_PLACE" | "COD_TOPUP" | "REFUND_ADJUSTMENT"
            audience: "CUSTOMER" or "RIDER" — controls copy only

        The rider app keys off `type=order_adjusted` to re-fetch the order
        before reaching the customer; the customer app uses the same payload
        to refresh the order details screen and show the new amount.
        """
        try:
            audience_norm = (audience or "").upper()
            sign = "+" if delta > 0 else ("-" if delta < 0 else "")
            amount_abs = abs(round(delta, 2))

            if audience_norm == "RIDER":
                if settlement_type == "COD_TOPUP":
                    title = "Order updated — collect extra at delivery"
                    body = f"{restaurant_name}: collect ₹{amount_due_at_delivery:.0f} (incl. ₹{amount_abs:.0f} top-up)."
                elif settlement_type == "COD_IN_PLACE":
                    title = "Order updated — new amount to collect"
                    body = f"{restaurant_name}: now ₹{amount_due_at_delivery:.0f} at delivery."
                else:
                    title = "Order updated"
                    body = f"{restaurant_name}: items changed. Refund will be handled by ops."
            else:
                if delta > 0:
                    title = "Order updated — extra to pay at delivery"
                    body = f"{restaurant_name} updated your order (+₹{amount_abs:.0f}). Pay the new total of ₹{new_grand_total:.0f} at delivery."
                elif delta < 0:
                    title = "Order updated — refund on the way"
                    body = f"{restaurant_name} updated your order (-₹{amount_abs:.0f}). We will refund ₹{amount_abs:.0f}."
                else:
                    title = "Order updated"
                    body = f"{restaurant_name} updated items on your order."

            data = {
                "type": "order_adjusted",
                "orderId": order_id,
                "restaurantName": restaurant_name,
                "delta": str(round(delta, 2)),
                "deltaSign": sign,
                "newGrandTotal": str(round(new_grand_total, 2)),
                "amountDueAtDelivery": str(round(amount_due_at_delivery, 2)),
                "settlementType": settlement_type or "",
                "audience": audience_norm,
                "title": title,
                "body": body,
            }
            return NotificationService.send_via_firebase(
                fcm_token=fcm_token,
                title=title,
                body=body,
                data=data,
            )
        except Exception as e:
            logger.error(f"Error sending order_adjusted notification: {str(e)}", exc_info=True)
            return False

    @staticmethod
    def validate_registration_token(fcm_token: str) -> dict:
        """
        Validate an FCM registration token against the configured Firebase project.
        Uses dry_run so no real notification is delivered.
        Returns: {"valid": bool, "reason": str}
        """
        if not FIREBASE_AVAILABLE:
            return {"valid": False, "reason": "firebase_admin_unavailable"}

        try:
            from firebase_admin import messaging

            initialize_firebase()
            if not _firebase_initialized:
                return {"valid": False, "reason": "firebase_not_initialized"}

            probe_message = messaging.Message(
                token=fcm_token,
                data={"type": "token_probe", "ts": now_ist_iso()}
            )
            messaging.send(probe_message, dry_run=True)
            return {"valid": True, "reason": "ok"}
        except Exception as e:
            if NotificationService.is_invalid_fcm_token_error(e):
                return {"valid": False, "reason": "invalid_token"}
            return {"valid": False, "reason": "validation_failed"}
