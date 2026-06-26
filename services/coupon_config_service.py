"""Accessor for the dedicated coupon config row.

Holds all coupon-level config:
  * the platform-wide usage switch + optional time window (enabled / availableFrom /
    availableTo) — gates coupon apply + the available-coupons list, and
  * ``blockedCouponsByRestaurant`` — per-restaurant coupon blocklist.

    CONFIG#COUPONS / CONFIG  ->  { config: { enabled, availableFrom, availableTo,
                                             blockedCouponsByRestaurant } }

The usage switch is a new control (no legacy location → defaults on when absent).
``blockedCouponsByRestaurant`` was moved out of CONFIG#GLOBAL, so its reader falls
back to the legacy global keys until migrated (see scripts/migrate_coupon_blocks_config.py).
"""
from datetime import datetime
from typing import Optional

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from utils.datetime_ist import IST, now_ist_iso
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python, python_to_dynamodb
from utils.time_window import parse_hhmm, within_window

logger = Logger()

COUPON_CONFIG_PK = "CONFIG#COUPONS"
COUPON_CONFIG_SK = "CONFIG"
LEGACY_CONFIG_PK = "CONFIG#GLOBAL"
LEGACY_CONFIG_SK = "CONFIG"

BLOCKED_KEY = "blockedCouponsByRestaurant"
# CouponService historically accepted these variants on the global config.
LEGACY_BLOCKED_KEYS = (
    "blockedCouponsByRestaurant",
    "couponBlocklistByRestaurant",
    "restaurantCouponBlocklist",
)


def _read_config_map(pk: str, sk: str) -> dict:
    """Return a config row's nested ``config`` map (empty dict if absent)."""
    response = dynamodb_client.get_item(
        TableName=TABLES["CONFIG"],
        Key={"partitionkey": {"S": pk}, "sortKey": {"S": sk}},
    )
    item = response.get("Item")
    if not item:
        return {}
    config = dynamodb_to_python(item.get("config", {"NULL": True}))
    return config if isinstance(config, dict) else {}


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


def fetch_blocked_config_map() -> dict:
    """Return a config map carrying the per-restaurant coupon blocklist.

    Reads ``CONFIG#COUPONS`` first; if it has none of the blocklist keys yet (not
    migrated), falls back to the legacy ``CONFIG#GLOBAL`` config map. The caller
    scans it for any of LEGACY_BLOCKED_KEYS (kept so CouponService's existing
    variant handling still works).
    """
    try:
        config = _read_config_map(COUPON_CONFIG_PK, COUPON_CONFIG_SK)
        if any(key in config for key in LEGACY_BLOCKED_KEYS):
            return config
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to fetch coupon-blocks row: {e}")
    try:
        return _read_config_map(LEGACY_CONFIG_PK, LEGACY_CONFIG_SK)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to fetch legacy coupon-blocks: {e}")
        return {}


def fetch_blocked_coupons() -> dict:
    """Return the resolved ``{restaurantId: [codes]}`` blocklist map (empty if none)."""
    config = fetch_blocked_config_map()
    for key in LEGACY_BLOCKED_KEYS:
        value = config.get(key)
        if isinstance(value, dict):
            return value
    return {}


def write_config_keys(updates: dict) -> None:
    """Targeted update of one or more keys inside CONFIG#COUPONS.config.

    Sets each key under the ``config`` map without touching its siblings, so the
    usage switch and the blocklist never clobber each other. Falls back to a full
    merge-and-put when the row/``config`` attribute doesn't exist yet.
    """
    if not updates:
        return
    names = {"#config": "config"}
    set_parts = []
    values = {}
    for i, (key, val) in enumerate(updates.items()):
        nk, vk = f"#k{i}", f":v{i}"
        names[nk] = key
        set_parts.append(f"#config.{nk} = {vk}")
        values[vk] = python_to_dynamodb(val)
    values[":u"] = {"S": now_ist_iso()}
    set_parts.append("updatedAt = :u")

    key = {"partitionkey": {"S": COUPON_CONFIG_PK}, "sortKey": {"S": COUPON_CONFIG_SK}}
    try:
        dynamodb_client.update_item(
            TableName=TABLES["CONFIG"],
            Key=key,
            UpdateExpression="SET " + ", ".join(set_parts),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ConditionExpression="attribute_exists(#config)",
        )
    except ClientError:
        # Row (or its config map) doesn't exist yet — create it with a full put.
        config = fetch_coupon_config()
        config.update(updates)
        dynamodb_client.update_item(
            TableName=TABLES["CONFIG"],
            Key=key,
            UpdateExpression="SET #config = :cfg, updatedAt = :u",
            ExpressionAttributeNames={"#config": "config"},
            ExpressionAttributeValues={
                ":cfg": python_to_dynamodb(config),
                ":u": {"S": now_ist_iso()},
            },
        )


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
