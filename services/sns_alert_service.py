"""Publish order alerts to SNS topic. Subscribers (e.g. SMS) receive the message."""
import os
import boto3
from aws_lambda_powertools import Logger

logger = Logger()

_sns_client = None


def _get_sns_client():
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns")
    return _sns_client


def publish_order_alert(message: str, subject: str | None = None) -> bool:
    """
    Publish a text message to the order alerts SNS topic.
    Subscribers to the topic (e.g. phone numbers with SMS protocol) receive an SMS.

    Args:
        message: Body of the alert (SMS text).
        subject: Optional subject (for email subscribers; SMS uses message only).

    Returns:
        True if publish succeeded, False otherwise.
    """
    topic_arn = os.environ.get("ORDER_ALERTS_TOPIC_ARN", "").strip()
    if not topic_arn:
        logger.warning("ORDER_ALERTS_TOPIC_ARN not set, skipping SNS publish")
        return False

    try:
        client = _get_sns_client()
        params = {
            "TopicArn": topic_arn,
            "Message": message,
        }
        if subject:
            params["Subject"] = subject
        client.publish(**params)
        logger.info("Order alert published to SNS topic")
        return True
    except Exception as e:
        logger.error(f"Failed to publish order alert to SNS: {e}", exc_info=True)
        return False
