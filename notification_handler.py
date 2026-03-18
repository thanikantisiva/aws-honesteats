"""
Lambda function to handle DynamoDB Stream events from OrdersTable
Sends push notifications when order status changes
"""
import json
import os
import re
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from aws_lambda_powertools import Logger
from utils.datetime_ist import now_ist_iso, epoch_ms_to_ist_iso
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.notification_service import NotificationService
from services.sns_alert_service import publish_order_alert
from utils.dynamodb_helpers import dynamodb_to_python
from utils.dynamodb import TABLES

logger = Logger(service="notification-handler")
dynamodb_client = boto3.client('dynamodb')


def _order_alert_enabled(env_key: str, default: bool = True) -> bool:
    """True if env is true/1/yes (case-insensitive); otherwise False. Missing => default."""
    v = (os.environ.get(env_key) or ("true" if default else "false")).strip().lower()
    return v in ("true", "1", "yes")


def _extract_order_item_summary(new_image: dict) -> tuple[str, str]:
    """Pick the costliest item and build compact item summary."""
    items_attr = new_image.get("items")
    if not items_attr:
        return "", ""

    try:
        items = dynamodb_to_python(items_attr)
        if not isinstance(items, list) or not items:
            return "", ""
    except Exception:
        return "", ""

    costliest = None
    costliest_price = -1.0

    for item in items:
        if not isinstance(item, dict):
            continue
        # Prefer unit selling price, fallback to restaurant price.
        raw_price = item.get("price")
        if raw_price is None:
            raw_price = item.get("restaurantPrice")
        try:
            price = float(raw_price)
        except Exception:
            price = 0.0
        if price > costliest_price:
            costliest = item
            costliest_price = price

    if not costliest:
        return "", ""

    item_name = str(costliest.get("name") or costliest.get("itemName") or "Item")
    quantity = costliest.get("quantity", 1)
    try:
        quantity = int(quantity)
    except Exception:
        quantity = 1

    remaining_count = max(len(items) - 1, 0)
    if remaining_count > 0:
        summary = f"{quantity}x {item_name} and {remaining_count} remaining Items"
    else:
        summary = f"{quantity}x {item_name}"

    restaurant_id = _extract_string_attr(new_image.get("restaurantId") or new_image.get("restaurant_id"))
    item_id = str(
        costliest.get("itemId")
        or costliest.get("item_id")
        or costliest.get("id")
        or ""
    )
    item_image_url = _fetch_menu_item_image_url(restaurant_id, item_id)
    if not item_image_url:
        item_image_url = str(
            costliest.get("image")
            or costliest.get("itemImageUrl")
            or costliest.get("imageUrl")
            or ""
        )

    return summary, item_image_url


def _extract_string_attr(attr) -> str:
    """Extract string from raw DynamoDB attribute (S or N)."""
    if not attr or not isinstance(attr, dict):
        return ""
    if "S" in attr and attr["S"]:
        return str(attr["S"]).strip()
    if "N" in attr:
        return str(attr["N"]).strip()
    return ""


def _first_image_url_from_attr(image_attr) -> str:
    """Extract first non-empty image URL from DynamoDB image attribute (L or S)."""
    if not image_attr or not isinstance(image_attr, dict) or image_attr.get("NULL"):
        return ""
    if "L" in image_attr:
        for img in image_attr["L"]:
            if not isinstance(img, dict):
                continue
            url = img.get("S") or (img.get("M", {}).get("url", {}).get("S") if isinstance(img.get("M"), dict) else None)
            if url and str(url).strip():
                return str(url).strip()
        return ""
    if "S" in image_attr and image_attr["S"]:
        return str(image_attr["S"]).strip()
    return ""


def _fetch_menu_item_image_url(restaurant_id: str, item_id: str) -> str:
    """Fetch menu item image from menu table for the selected costliest item."""
    if not restaurant_id or not item_id:
        return ""
    try:
        logger.info(f"Fetching menu image for restaurantId={restaurant_id}, itemId={item_id}")
        response = dynamodb_client.get_item(
            TableName=TABLES["MENU_ITEMS"],
            Key={
                "PK": {"S": f"RESTAURANT#{restaurant_id}"},
                "SK": {"S": f"ITEM#{item_id}"}
            }
        )
        item = response.get("Item", {})
        if not item:
            logger.warning(f"Menu item not found for restaurantId={restaurant_id}, itemId={item_id}")
            return ""
        for attr_name in ("image", "imageUrl", "images"):
            image_attr = item.get(attr_name)
            url = _first_image_url_from_attr(image_attr)
            if url:
                return url
        logger.warning(f"Menu item has no image attribute for itemId={item_id}")
        return ""
    except Exception as e:
        logger.warning(f"Failed to fetch menu item image for itemId={item_id}: {str(e)}")
        return ""


def _extract_updated_at(record: dict, new_image: dict) -> str:
    """Resolve updatedAt in ISO-8601 IST."""
    from datetime import datetime
    raw_updated = new_image.get("updatedAt", {})
    if "S" in raw_updated and raw_updated["S"]:
        return raw_updated["S"]
    if "N" in raw_updated:
        try:
            millis = int(float(raw_updated["N"]))
            return epoch_ms_to_ist_iso(millis)
        except Exception:
            pass

    approx = record.get("dynamodb", {}).get("ApproximateCreationDateTime")
    if approx:
        try:
            return datetime.fromtimestamp(float(approx), tz=ZoneInfo("Asia/Kolkata")).isoformat()
        except Exception:
            pass

    return now_ist_iso()


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Process DynamoDB Stream events for order status changes
    
    Args:
        event: DynamoDB Stream event
        context: Lambda context
        
    Returns:
        Processing result
    """
    logger.info(f"Processing {len(event.get('Records', []))} DynamoDB Stream records")
    
    processed = 0
    errors = 0
    
    for record in event.get('Records', []):
        try:
            # Only process MODIFY events (status updates)
            if record['eventName'] != 'MODIFY':
                continue
            
            old_image = record['dynamodb'].get('OldImage', {})
            new_image = record['dynamodb'].get('NewImage', {})
            
            # Check if status changed
            old_status = old_image.get('status', {}).get('S', '')
            new_status = new_image.get('status', {}).get('S', '')
            
            if old_status == new_status:
                logger.info("Status unchanged, skipping notification")
                continue
            
            # Extract order details
            order_id = new_image.get('orderId', {}).get('S', '')
            customer_phone = new_image.get('customerPhone', {}).get('S', '')
            restaurant_name = new_image.get('restaurantName', {}).get('S', 'Restaurant')
            rider_id = _extract_string_attr(new_image.get("riderId") or new_image.get("rider_id"))
            rider_name = _fetch_rider_name(rider_id) if rider_id else ""
            item_name, item_image_url = _extract_order_item_summary(new_image)
            updated_at = _extract_updated_at(record, new_image)
            
            logger.info(f"[orderId={order_id}] 📦 Order status changed")
            logger.info(f"[orderId={order_id}] Customer: {customer_phone}")
            logger.info(f"[orderId={order_id}] Status: {old_status} → {new_status}")
            logger.info(f"[orderId={order_id}] Restaurant: {restaurant_name}")

            if new_status == "OFFERED_TO_RIDER":
                logger.info(f"[orderId={order_id}] Skipping customer notification for OFFERED_TO_RIDER")
                continue

            # Publish to SNS order-alerts topic (subscribers get SMS) — gated by toggle
            if new_status == "AWAITING_RIDER_ASSIGNMENT" and _order_alert_enabled("ORDER_ALERTS_RIDER_ENABLED"):
                sms_text = f"Order {order_id} is awaiting rider assignment. Restaurant: {restaurant_name}"
                try:
                    publish_order_alert(sms_text, subject="Order awaiting rider")
                except Exception as sns_err:
                    logger.warning(f"[orderId={order_id}] SNS alert failed: {sns_err}")

            # Schedule a one-time check in x mins: if order still CONFIRMED, send SNS alert — gated by toggle
            if new_status == "CONFIRMED" and _order_alert_enabled("ORDER_ALERTS_CONFIRMED_ENABLED"):
                try:
                    _schedule_confirmed_alert(order_id)
                except Exception as schedule_err:
                    logger.warning(f"[orderId={order_id}] Schedule CONFIRMED alert failed: {schedule_err}")
            
            # Get user's FCM token from UsersTable
            # UsersTableV2 has composite key: phone (HASH) + role (RANGE)
            users_table = get_users_table_name()
            user_response = dynamodb_client.get_item(
                TableName=users_table,
                Key={
                    'phone': {'S': customer_phone},
                    'role': {'S': 'CUSTOMER'}
                }
            )
            
            if 'Item' not in user_response:
                logger.warning(f"[orderId={order_id}] User not found: {customer_phone}")
                continue
            
            fcm_token = user_response['Item'].get('fcmToken', {}).get('S')
            
            if not fcm_token:
                logger.warning(f"[orderId={order_id}] No FCM token for user: {customer_phone}")
                continue
            
            # Send push notification
            success = NotificationService.send_order_status_notification(
                fcm_token=fcm_token,
                order_id=order_id,
                status=new_status,
                restaurant_name=restaurant_name,
                item_name=item_name,
                item_image_url=item_image_url,
                updated_at=updated_at,
                rider_id=rider_id or None,
                rider_name=rider_name or None,
                customer_phone=customer_phone or None,
            )
            
            if success:
                processed += 1
                logger.info(f"[orderId={order_id}] ✅ Notification sent successfully")
            else:
                errors += 1
                logger.error(f"[orderId={order_id}] ❌ Failed to send notification")
                
        except Exception as e:
            errors += 1
            logger.error(f"[orderId={order_id}] Error processing record: {str(e)}", exc_info=True)
    
    logger.info(f"Stream processing complete: {processed} sent, {errors} errors")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed,
            'errors': errors
        })
    }


def _schedule_confirmed_alert(order_id: str) -> None:
    """Create a one-time EventBridge Scheduler to run in x mins; handler checks if order still CONFIRMED and sends SNS."""
    handler_arn = os.environ.get("ORDER_CONFIRMED_ALERT_HANDLER_ARN", "").strip()
    role_arn = os.environ.get("ORDER_CONFIRMED_ALERT_HANDLER_ROLE_ARN", "").strip()
    delay_mins = int(os.environ.get("ORDER_CONFIRMED_ALERT_DELAY_MINS", "15") or "15")
    if not handler_arn or not role_arn:
        logger.warning("ORDER_CONFIRMED_ALERT_HANDLER_ARN or ROLE_ARN not set, skipping schedule")
        return
    run_at = datetime.now(timezone.utc) + timedelta(minutes=delay_mins)
    schedule_name = "order-confirmed-alert-" + re.sub(r"[^a-zA-Z0-9_-]", "-", order_id)[:48]
    try:
        scheduler = boto3.client("scheduler")
        scheduler.create_schedule(
            Name=schedule_name,
            ScheduleExpression=f"at({run_at.strftime('%Y-%m-%dT%H:%M:%S')})",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": handler_arn,
                "RoleArn": role_arn,
                "Input": json.dumps({"orderId": order_id, "delayMins": delay_mins}),
            },
            ActionAfterCompletion="DELETE",
        )
        logger.info(f"[orderId={order_id}] Scheduled CONFIRMED alert in {delay_mins} mins at {run_at.isoformat()}")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConflictException":
            logger.info(f"[orderId={order_id}] Schedule already exists: {schedule_name}")
        else:
            raise


def _fetch_rider_name(rider_id: str) -> str:
    """Fetch rider display name from Users table by riderId (GSI). Returns empty string if not found."""
    if not rider_id or not rider_id.strip():
        return ""
    try:
        from services.user_service import UserService
        rider_user = UserService.get_rider_by_rider_id(rider_id.strip())
        if not rider_user:
            return ""
        first = (rider_user.first_name or "").strip()
        last = (rider_user.last_name or "").strip()
        name = f"{first} {last}".strip()
        return name or (rider_user.name or "").strip()
    except Exception as e:
        logger.warning(f"Failed to fetch rider name for riderId={rider_id}: {str(e)}")
        return ""


def get_users_table_name():
    """Get UsersTable name from environment"""
    import os
    env = os.environ.get('ENVIRONMENT', 'dev')
    return f'food-delivery-users-{env}'
