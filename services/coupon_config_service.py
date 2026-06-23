"""Accessor for the global coupon-usage config row.

A platform-wide switch (with an optional time window) to enable/disable customers
from using coupons. Lives in its own config row:

    CONFIG#COUPONS / CONFIG  ->  { config: { enabled, availableFrom, availableTo } }

When the row is absent, coupons are enabled with no time restriction (current
behaviour) — this is a brand-new control with no legacy location, so there's no
fallback. Enforced at coupon apply (calculate-fee) and in the available-coupons
list; item-level menu offers are a separate surface and are not gated here.
"""
from datetime import datetime
from typing import Optional

from aws_lambda_powertools import Logger

from utils.datetime_ist import IST
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python
from utils.time_window import parse_hhmm, within_window

logger = Logger()

COUPON_CONFIG_PK = "CONFIG#COUPONS"
COUPON_CONFIG_SK = "CONFIG"


def fetch_coupon_config() -> dict:
    """Return the coupon config ``{enabled, availableFrom, availableTo}``.

    Empty dict (meaning: enabled, no window) when the row doesn't exist.
    """
    try:
        response = dynamodb_client.get_item(
            TableName=TABLES["CONFIG"],
            Key={"partitionkey": {"S": COUPON_CONFIG_PK}, "sortKey": {"S": COUPON_CONFIG_SK}},
        )
        item = response.get("Item")
        if not item:
            return {}
        config = dynamodb_to_python(item.get("config", {"NULL": True}))
        return config if isinstance(config, dict) else {}
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to fetch coupon config: {e}")
        return {}


def coupons_allowed(config: dict, now_minutes: Optional[int]) -> bool:
    """Pure: are coupons usable given ``config`` + current minute-of-day (IST)?

    ``enabled`` defaults to True when absent (no config row => coupons on). When a
    time window is set, coupons are usable only inside it.
    """
    if not isinstance(config, dict):
        return True
    enabled = config.get("enabled", True)
    if isinstance(enabled, str):
        enabled = enabled.strip().lower() in ("true", "1", "yes", "on")
    if not enabled:
        return False
    return within_window(
        parse_hhmm(config.get("availableFrom")),
        parse_hhmm(config.get("availableTo")),
        now_minutes,
    )


def coupons_enabled_now() -> bool:
    """Convenience: fetch the config and evaluate it against the current IST time."""
    now = datetime.now(IST)
    return coupons_allowed(fetch_coupon_config(), now.hour * 60 + now.minute)


def strip_coupon_from_fee_response(fee_response) -> None:
    """Neutralize any checkout-coupon discount in a (client-supplied) fee response.

    Called at ORDER CREATION when coupons are globally disabled, so a fabricated or
    stale ``calculatedFeeResponse`` can't carry a discount into the order (and thus
    into settlement / usage commits). Mutates ``fee_response`` in place. This is the
    correct boundary to enforce the switch — settlement must faithfully reflect what
    was charged, so it is NOT re-checked there.
    """
    if not isinstance(fee_response, dict):
        return
    fee_response["couponApplied"] = False
    fee_response.pop("couponCode", None)
    breakdown = fee_response.get("breakdown")
    if isinstance(breakdown, dict):
        try:
            coupon_discount = float(breakdown.get("couponDiscount") or 0)
        except (TypeError, ValueError):
            coupon_discount = 0.0
        if coupon_discount:
            try:
                total = float(breakdown.get("totalDiscount") or 0)
            except (TypeError, ValueError):
                total = 0.0
            new_total = round(max(total - coupon_discount, 0.0), 2)
            breakdown["totalDiscount"] = new_total
            breakdown["discount"] = new_total  # backward-compat alias
        breakdown["couponDiscount"] = 0
