"""
Lambda function to handle DynamoDB Stream events from MenuItemsTable.
When menu items change, insert a marker row per restaurant with 24h TTL.
"""
import os
import time
import json
import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger(service="restaurant-menu-change-handler")
dynamodb_client = boto3.client("dynamodb")

MENU_ITEMS_TABLE = os.environ.get("MENU_ITEMS_TABLE_NAME", "food-delivery-menu-items")

TTL_SECONDS = 24 * 60 * 60
MARKER_SK = "CHANGED"


def _put_marker_row(restaurant_id: str) -> bool:
    """Put marker row if it does not already exist."""
    ttl = int(time.time()) + TTL_SECONDS
    pk_value = f"RESTAURANT#{restaurant_id}"
    try:
        dynamodb_client.put_item(
            TableName=MENU_ITEMS_TABLE,
            Item={
                "PK": {"S": pk_value},
                "SK": {"S": MARKER_SK},
                "recordType": {"S": "MENU_CHANGE"},
                "ttl": {"N": str(ttl)}
            },
            ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(SK)"
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            logger.info(f"Marker row already exists for restaurantId={restaurant_id}, skipping write")
            return False
        raise


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    records = event.get("Records", [])
    logger.info(f"Processing {len(records)} menu stream records")

    processed = 0
    skipped = 0
    errors = 0

    for record in records:
        try:
            event_name = record.get("eventName")
            if event_name not in ("INSERT", "MODIFY", "REMOVE"):
                skipped += 1
                continue

            new_image = record.get("dynamodb", {}).get("NewImage", {})
            old_image = record.get("dynamodb", {}).get("OldImage", {})
            image = new_image or old_image

            pk = image.get("PK", {}).get("S", "")
            sk = image.get("SK", {}).get("S", "")

            # Skip non-menu items or our own marker rows
            if sk == MARKER_SK or not pk.startswith("RESTAURANT#"):
                skipped += 1
                continue

            restaurant_id = pk.replace("RESTAURANT#", "")
            if not restaurant_id:
                skipped += 1
                continue

            created = _put_marker_row(restaurant_id)
            if created:
                logger.info(
                    f"Inserted menu change marker for restaurantId={restaurant_id}"
                )
            processed += 1
        except Exception as e:
            errors += 1
            logger.error(f"Error processing menu change record: {str(e)}", exc_info=True)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": processed,
            "skipped": skipped,
            "errors": errors
        })
    }
