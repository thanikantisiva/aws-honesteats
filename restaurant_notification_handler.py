"""
Lambda function to send restaurant mobile push notifications for newly confirmed orders.
This is intentionally separate from the customer notification flow.
"""
import json
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.notification_service import NotificationService
from services.restaurant_service import RestaurantService
from utils.dynamodb_helpers import dynamodb_to_python

logger = Logger(service="restaurant-notification-handler")


def _extract_string_attr(attr) -> str:
    if not attr or not isinstance(attr, dict):
        return ""
    if "S" in attr and attr["S"]:
        return str(attr["S"]).strip()
    if "N" in attr:
        return str(attr["N"]).strip()
    return ""


def _summarize_items(new_image: dict) -> str:
    items_attr = new_image.get("items")
    if not items_attr:
        return ""

    try:
        items = dynamodb_to_python(items_attr)
    except Exception:
        return ""

    if not isinstance(items, list) or not items:
        return ""

    first_item = items[0] if isinstance(items[0], dict) else {}
    first_name = str(first_item.get("name") or first_item.get("itemName") or "Item")
    try:
        first_qty = int(first_item.get("quantity", 1))
    except Exception:
        first_qty = 1

    if len(items) == 1:
        return f"{first_qty}x {first_name}"

    return f"{first_qty}x {first_name} and {len(items) - 1} more"


def _count_items(new_image: dict) -> int:
    items_attr = new_image.get("items")
    if not items_attr:
        return 0

    try:
        items = dynamodb_to_python(items_attr)
    except Exception:
        return 0

    if not isinstance(items, list) or not items:
        return 0

    total = 0
    for item in items:
        if not isinstance(item, dict):
            continue

        try:
            total += int(item.get("quantity", 1))
        except Exception:
            total += 1

    return total


def _is_new_restaurant_order(record: dict, new_image: dict, old_image: dict) -> bool:
    event_name = record.get("eventName")
    new_status = _extract_string_attr(new_image.get("status"))
    old_status = _extract_string_attr(old_image.get("status"))

    if event_name == "INSERT":
        return new_status == "CONFIRMED"

    if event_name == "MODIFY":
        return old_status != "CONFIRMED" and new_status == "CONFIRMED"

    return False


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    logger.info(f"Processing {len(event.get('Records', []))} order stream records for restaurant notifications")

    processed = 0
    errors = 0

    for record in event.get("Records", []):
        order_id = ""
        try:
            if record.get("eventName") not in ("INSERT", "MODIFY"):
                continue

            old_image = record.get("dynamodb", {}).get("OldImage", {})
            new_image = record.get("dynamodb", {}).get("NewImage", {})

            if not _is_new_restaurant_order(record, new_image, old_image):
                continue

            order_id = _extract_string_attr(new_image.get("orderId"))
            restaurant_id = _extract_string_attr(new_image.get("restaurantId"))
            restaurant_name = _extract_string_attr(new_image.get("restaurantName")) or "Restaurant"
            customer_phone = _extract_string_attr(new_image.get("customerPhone"))
            created_at = _extract_string_attr(new_image.get("createdAt"))
            amount_raw = _extract_string_attr(new_image.get("grandTotal"))
            item_summary = _summarize_items(new_image)
            item_count = _count_items(new_image)

            try:
                amount = float(amount_raw or "0")
            except Exception:
                amount = 0.0

            if not restaurant_id:
                logger.warning(f"[orderId={order_id}] Missing restaurantId, skipping")
                continue

            restaurant = RestaurantService.get_restaurant_by_id(restaurant_id)
            if not restaurant:
                logger.warning(f"[orderId={order_id}] Restaurant not found: {restaurant_id}")
                continue

            if not restaurant.fcm_token:
                logger.info(f"[orderId={order_id}] No mobile FCM token registered for restaurant {restaurant_id}")
                continue

            success = NotificationService.send_restaurant_new_order_notification(
                fcm_token=restaurant.fcm_token,
                order_id=order_id,
                restaurant_name=restaurant_name,
                customer_phone=customer_phone,
                item_summary=item_summary,
                item_count=item_count,
                amount=amount,
                created_at=created_at or None
            )

            if success:
                processed += 1
            else:
                errors += 1
        except Exception as e:
            errors += 1
            logger.error(f"[orderId={order_id}] Error processing restaurant notification: {str(e)}", exc_info=True)

    logger.info(f"Restaurant notification processing complete: {processed} sent, {errors} errors")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": processed,
            "errors": errors
        })
    }
