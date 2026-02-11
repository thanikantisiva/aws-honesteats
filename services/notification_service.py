"""Notification service for sending push notifications via Firebase FCM"""
import os
import json
from typing import Optional
from aws_lambda_powertools import Logger

logger = Logger()

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
        # Load service account from file in Lambda deployment package
        import os.path
        service_account_path = os.path.join(os.path.dirname(__file__), '..', 'firebase-service-account.json')
        
        if os.path.exists(service_account_path):
            logger.info(f"📄 Loading Firebase service account from: {service_account_path}")
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred)
        
        _firebase_initialized = True
        logger.info("✅ Firebase Admin SDK initialized successfully")
        
    except Exception as e:
        logger.warning(f"⚠️ Firebase initialization failed (push notifications disabled): {str(e)}")


class NotificationService:
    """Service for sending push notifications via Firebase FCM"""
    
    @staticmethod
    def send_via_firebase(
        fcm_token: str,
        title: str,
        body: str,
        data: dict
    ) -> bool:
        """Send notification via Firebase Admin SDK"""
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
            
            # Detect token type
            is_apns_token = len(fcm_token) == 64 and all(c in '0123456789abcdef' for c in fcm_token.lower())
            
            if is_apns_token:
                logger.info("📱 Detected APNs token (iOS)")
                message = messaging.Message(
                    token=fcm_token,
                    notification=messaging.Notification(title=title, body=body),
                    data=string_data,
                    apns=messaging.APNSConfig(
                        headers={'apns-priority': '10'},
                        payload=messaging.APNSPayload(
                            aps=messaging.Aps(
                                alert=messaging.ApsAlert(title=title, body=body),
                                sound='default',
                                badge=1
                            )
                        )
                    )
                )
            else:
                logger.info("📱 Detected FCM token (Android)")
                message = messaging.Message(
                    token=fcm_token,
                    notification=messaging.Notification(title=title, body=body),
                    data=string_data,
                    android=messaging.AndroidConfig(
                        priority="high",
                        notification=messaging.AndroidNotification(
                            sound="default",
                            color="#EF4444"
                        )
                    )
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
        restaurant_name: str
    ) -> bool:
        """
        Send order status update notification via FCM
        
        Args:
            fcm_token: User's FCM device token
            order_id: Order ID
            status: New order status
            restaurant_name: Restaurant name
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            # Map status to user-friendly messages
            status_messages = {
                'CONFIRMED': ('Order Confirmed! 🎉', f'Your order from {restaurant_name} has been confirmed'),
                'PREPARING': ('Food is Being Prepared', f'{restaurant_name} is preparing your order'),
                'READY_FOR_PICKUP': ('Order Ready!', f'Your order from {restaurant_name} is ready'),
                'OUT_FOR_DELIVERY': ('On the Way! 🛵', f'Your order from {restaurant_name} is out for delivery'),
                'DELIVERED': ('Order Delivered ✅', f'Your order from {restaurant_name} has been delivered'),
                'AWAITING_RIDER_ASSIGNMENT': ('Order Awaiting Rider Assignment', f'Your order from {restaurant_name} is awaiting a rider assignment'),
                'RIDER_ASSIGNED': ('Order Assigned to Rider', f'Your order from {restaurant_name} has been assigned to a rider')
            }
            
            title, body = status_messages.get(status, ('Order Update', f'Your order status: {status}'))
            
            logger.info(f"📱 Sending Firebase FCM notification")
            logger.info(f"   Token: {fcm_token[:30]}...")
            logger.info(f"   Title: {title}")
            logger.info(f"   Body: {body}")
            
            notification_data = {
                "type": "order_status",
                "orderId": order_id,
                "status": status,
                "restaurantName": restaurant_name
            }
            
            # Send via Firebase FCM
            return NotificationService.send_via_firebase(
                fcm_token=fcm_token,
                title=title,
                body=body,
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
            
            logger.info(f"📱 Sending order assignment notification to rider: {rider_mobile}")
            
            # Get rider's FCM token from UsersTableV2 (composite key: phone + role)
            users_table = os.environ.get('USERS_TABLE_NAME', f'food-delivery-users-{os.environ.get("ENVIRONMENT", "dev")}')
            
            user_response = dynamodb_client.get_item(
                TableName=users_table,
                Key={
                    'phone': {'S': rider_mobile},
                    'role': {'S': 'RIDER'}
                }
            )
            
            if 'Item' not in user_response:
                logger.warning(f"Rider not found in users table: {rider_mobile}")
                return False
            
            fcm_token = user_response['Item'].get('fcmToken', {}).get('S')
            
            if not fcm_token:
                logger.warning(f"No FCM token for rider: {rider_mobile}")
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
                "deliveryFee": str(delivery_fee)
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

