"""Lambda invoked late each night (EventBridge schedule, 23:55 IST) to settle the
SAME day's rider slot guarantees.

By 23:55 IST the day's slots have ended and every order accepted during them has been
delivered, so each booked rider's slot delivery earnings are final. For each compliant
booking it credits the guarantee top-up (max(0, guarantee - earnings)); non-compliant
bookings forfeit with no penalty. Settling the same night means the amounts are ready
for the next day's rider settlement.

The heavy lifting lives in ``RiderSlotsService.settle_day`` so it can be unit-tested
with a stubbed DynamoDB client.

Event (all optional):
    { "date": "YYYY-MM-DD" }   # settle a specific IST date; default = today (IST)
"""
import json
from datetime import datetime

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from services.rider_slots_service import RiderSlotsService
from utils.datetime_ist import IST

logger = Logger(service="eod-slot-settlement-handler")


def _target_date(event: dict) -> str:
    explicit = (event or {}).get("date")
    if explicit:
        return str(explicit).strip()
    # Default: today in IST — the schedule fires at 23:55 IST, settling the day that is ending.
    return datetime.now(IST).strftime("%Y-%m-%d")


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    try:
        date = _target_date(event)
        logger.info(f"[date={date}] EOD slot settlement triggered")
        result = RiderSlotsService.settle_day(date)
        return {"statusCode": 200, "body": json.dumps(result)}
    except Exception as e:
        logger.error(f"Error in EOD slot settlement handler: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
