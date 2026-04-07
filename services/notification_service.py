"""Notification service for sending push notifications via Firebase FCM"""
import os
import json
from typing import Optional
from datetime import timedelta
from utils import normalize_phone
from utils.datetime_ist import now_ist_iso
from aws_lambda_powertools import Logger

logger = Logger()
RIDER_NOTIFICATION_CHANNEL_ID = "rider_orders_ring"
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
            android_channel_id = string_data.get("channelId") or None

            is_order_status_update = string_data.get("type") == "order_status"

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

            # Send both Android and APNS config so FCM delivers correctly to iOS or Android
            logger.info("📱 Sending FCM message (Android + APNS config)")
            apns_config = messaging.APNSConfig(
                headers={'apns-priority': '10'},
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        alert=messaging.ApsAlert(title=ios_alert_title, body=ios_alert_body),
                        sound='default',
                        badge=1
                    )
                )
            )
            if is_order_status_update:
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
    
    @staticmethod
    def send_order_assigned_notification(
        rider_mobile: str,
        order_id: str,
        restaurant_name: str,
        delivery_fee: float
    ) -> bool:
        """
        Send notification to rider when order is assigned
        
        Args:
            rider_mobile: Rider's phone number
            order_id: Order ID
            restaurant_name: Restaurant name
            delivery_fee: Delivery fee amount
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            from utils.dynamodb import dynamodb_client
            
            phone_key = normalize_phone(rider_mobile)
            if not phone_key:
                logger.warning(f"Invalid rider phone for assignment notification: {rider_mobile!r}")
                return False

            logger.info(f"📱 Sending order assignment notification to rider: {phone_key}")
            
            # Get rider's FCM token from UsersTableV2 (composite key: phone + role)
            users_table = os.environ.get('USERS_TABLE_NAME', f'food-delivery-users-{os.environ.get("ENVIRONMENT", "dev")}')
            
            user_response = dynamodb_client.get_item(
                TableName=users_table,
                Key={
                    'phone': {'S': phone_key},
                    'role': {'S': 'RIDER'}
                }
            )
            
            if 'Item' not in user_response:
                logger.warning(f"Rider not found in users table (normalized={phone_key}, raw={rider_mobile!r})")
                return False
            
            fcm_token = user_response['Item'].get('fcmToken', {}).get('S')
            
            if not fcm_token:
                logger.warning(f"No FCM token for rider: {phone_key}")
                return False
            
            # Prepare notification
            title = "New Order Assigned! 🛵"
            body = f"Pickup from {restaurant_name} • Earn ₹{delivery_fee:.0f}"
            
            logger.info(f"   Token: {fcm_token[:30]}...")
            logger.info(f"   Title: {title}")
            logger.info(f"   Body: {body}")
            
            notification_data = {
                "type": "order_assigned",
                "orderId": order_id,
                "restaurantName": restaurant_name,
                "deliveryFee": str(delivery_fee),
                "channelId": RIDER_NOTIFICATION_CHANNEL_ID,
                "sound": RIDER_NOTIFICATION_SOUND,
            }
            
            # Send via Firebase FCM
            success = NotificationService.send_via_firebase(
                fcm_token=fcm_token,
                title=title,
                body=body,
                data=notification_data
            )
            
            if success:
                logger.info(f"✅ Rider notification sent successfully")
            else:
                logger.error(f"❌ Failed to send rider notification")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending rider notification: {str(e)}", exc_info=True)
            return False

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
        if not FIREBASE_AVAILABLE:
            logger.warning("Firebase Admin SDK not available - restaurant notification skipped")
            return False

        try:
            from firebase_admin import messaging

            initialize_firebase()

            if not _firebase_initialized:
                logger.warning("Firebase not initialized - restaurant notification skipped")
                return False

            item_label = "1 item" if item_count == 1 else f"{item_count} items"
            order_tail = (order_id or "").strip()[-4:] or "----"
            title_text = f"Rs.{amount:.0f} New Order"
            body_text = f"{item_label} | #{order_tail}"
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
                "channelId": "new_orders",
                "title": title_text,
                "body": body_text
            }

            message = messaging.Message(
                token=fcm_token,
                data=data,
                notification=messaging.Notification(
                    title=title_text,
                    body=body_text
                ),
                android=messaging.AndroidConfig(
                    priority="high",
                    notification=messaging.AndroidNotification(
                        title=title_text,
                        body=body_text,
                        sound="telephone_ring",
                        channel_id="new_orders",
                        icon="ic_launcher",
                        color="#F59E0B",
                        tag=order_id
                    )
                )
            )

            logger.info(f"Sending restaurant notification for orderId={order_id}")
            response = messaging.send(message)
            logger.info(f"Restaurant notification sent successfully: {response}")
            return True
        except Exception as e:
            logger.error(f"Error sending restaurant notification: {str(e)}", exc_info=True)
            return False
