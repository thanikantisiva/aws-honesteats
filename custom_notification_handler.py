"""
Lambda function to broadcast a custom notification to all CUSTOMERs in a fixed
geographic partition.

The partition is hard-coded to geohash "td" — the precision-2 cell that covers
our current operating area. Recipients are resolved by querying UsersTableV2's
`geohash-index` (PK=geohash, ProjectionType=ALL); rows are then filtered
in-memory for role=="CUSTOMER", isActive, and a non-empty fcmToken. FCM tokens
are deduplicated and notified sequentially via NotificationService.send_via_firebase.

Expected event:
{
  "title": "Optional title",  // default "Notification"
  "customMessage": "Body",    // required
  "data": { "any": "payload" }// optional; merged into FCM data payload
}
"""
import json
from typing import List

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from services.notification_service import NotificationService
from utils.dynamodb import dynamodb_client, TABLES

logger = Logger(service="custom-notification-handler")

# Hard-coded precision-2 geohash partition (~1250km cell) covering our current
# operating area. Customer rows are written with this same precision by
# /api/v1/users/location (routes/user_routes.py).
TARGET_GEOHASH = "td"
CUSTOMER_ROLE = "CUSTOMER"
USERS_GEOHASH_INDEX = "geohash-index"


def _query_customer_fcm_tokens(geohash: str) -> List[str]:
    """Paginated Query on UsersTableV2.geohash-index → unique CUSTOMER fcmTokens."""
    seen = set()
    tokens: List[str] = []
    last_evaluated_key = None

    while True:
        kwargs = {
            "TableName": TABLES["USERS"],
            "IndexName": USERS_GEOHASH_INDEX,
            "KeyConditionExpression": "geohash = :gh",
            "ExpressionAttributeValues": {":gh": {"S": geohash}},
            "ProjectionExpression": "#r, fcmToken",
            "ExpressionAttributeNames": {"#r": "role"},
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = dynamodb_client.query(**kwargs)

        for item in response.get("Items", []):
            if item.get("role", {}).get("S") != CUSTOMER_ROLE:
                continue
            token = item.get("fcmToken", {}).get("S")
            if not token or token in seen:
                continue
            seen.add(token)
            tokens.append(token)

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    return tokens


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    try:
        custom_message = event.get("customMessage")
        title = event.get("title", "Notification")
        data = event.get("data") or {}

        if not custom_message:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "customMessage required"}),
            }

        geohash = TARGET_GEOHASH
        logger.info(f"Querying CUSTOMER fcmTokens for geohash partition '{geohash}'")
        tokens = _query_customer_fcm_tokens(geohash)
        logger.info(f"Found {len(tokens)} unique CUSTOMER fcmTokens in partition '{geohash}'")

        if not tokens:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "geohash": geohash,
                    "matched": 0,
                    "sent": 0,
                    "failed": 0,
                }),
            }

        # Differentiate from order-status FCM payloads (which set type="order_status")
        payload = dict(data)
        payload.setdefault("type", "custom")

        sent = 0
        failed = 0
        for token in tokens:
            ok = NotificationService.send_via_firebase(
                fcm_token=token,
                title=title,
                data=payload,
                body=custom_message,
            )
            if ok:
                sent += 1
            else:
                failed += 1

        logger.info(
            f"Custom notification complete: geohash={geohash} matched={len(tokens)} "
            f"sent={sent} failed={failed}"
        )
        return {
            "statusCode": 200,
            "body": json.dumps({
                "geohash": geohash,
                "matched": len(tokens),
                "sent": sent,
                "failed": failed,
            }),
        }
    except Exception as e:
        logger.error(f"Error in custom notification handler: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to send notifications"}),
        }
