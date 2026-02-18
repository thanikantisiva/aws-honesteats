"""
Lambda function to send custom notifications to a list of FCM tokens
Expected event:
{
  "fcmTokens": ["token1", "token2"],
  "customMessage": "Your message",
  "title": "Optional title",
  "data": { "any": "payload" }
}
"""
import json
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from services.notification_service import NotificationService

logger = Logger(service="custom-notification-handler")


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    try:
        fcm_tokens = event.get("fcmTokens") or []
        custom_message = event.get("customMessage")
        title = event.get("title", "Notification")
        data = event.get("data") or {}

        if not fcm_tokens or not isinstance(fcm_tokens, list):
            return {"statusCode": 400, "body": json.dumps({"error": "fcmTokens (list) required"})}
        if not custom_message:
            return {"statusCode": 400, "body": json.dumps({"error": "customMessage required"})}

        logger.info(f"Sending custom notification to {len(fcm_tokens)} tokens")

        sent = 0
        failed = 0
        for token in fcm_tokens:
            if not token:
                failed += 1
                continue
            ok = NotificationService.send_via_firebase(
                fcm_token=token,
                title=title,
                body=custom_message,
                data=data
            )
            if ok:
                sent += 1
            else:
                failed += 1

        logger.info(f"Custom notification complete: sent={sent}, failed={failed}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "sent": sent,
                "failed": failed,
                "total": len(fcm_tokens)
            })
        }
    except Exception as e:
        logger.error(f"Error in custom notification handler: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": "Failed to send notifications"})}
