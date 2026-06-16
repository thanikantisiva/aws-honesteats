"""Lambda invoked by EventBridge Scheduler at a rider slot's end time.

Evaluates every booking on the slot for compliance (online >= threshold AND rejected
offers <= cap) and stamps the verdict on the booking; records a no-show (which can
trigger a temporary booking ban) for the rest. It does NOT credit the guarantee —
that is deferred to the end-of-day batch (``eod_slot_settlement_handler`` →
``RiderSlotsService.settle_day``) so order earnings during the slot are fully realised.

The heavy lifting lives in ``RiderSlotsService.evaluate_slot_compliance`` so it can
be unit-tested with a stubbed DynamoDB client.
"""
import json

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from services.rider_slots_service import RiderSlotsService

logger = Logger(service="slot-compliance-handler")


def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """Expected event: { "slotId": "SLOT-..." }"""
    try:
        slot_id = (event or {}).get("slotId")
        if not slot_id:
            return {"statusCode": 400, "body": json.dumps({"error": "slotId required"})}

        logger.info(f"[slotId={slot_id}] Slot compliance check triggered")
        result = RiderSlotsService.evaluate_slot_compliance(slot_id)
        return {"statusCode": 200, "body": json.dumps(result)}
    except Exception as e:
        logger.error(f"Error in slot compliance handler: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
