"""Accessor for the dedicated rider config row.

Rider tunables — ``riderBonusConfig`` (bonus campaign), ``riderSlotsSettings``
(slot compliance/booking settings) and the legacy ``riderSlots`` list — live in
their own config row, separate from the big global config map:

    CONFIG#RIDER / CONFIG  ->  { config: { riderSlots, riderBonusConfig, riderSlotsSettings } }

Reads fall back to the legacy ``CONFIG#GLOBAL`` keys so nothing breaks before the
data is migrated (see scripts/migrate_rider_config.py).

Note: individual rider slot *definitions* and bookings are their own items
(``RIDER_SLOT#<id>`` / ``RIDER_SLOT_BOOKINGS#<rider>``) and are NOT affected here.
"""
from aws_lambda_powertools import Logger

from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python

logger = Logger()

RIDER_CONFIG_PK = "CONFIG#RIDER"
RIDER_CONFIG_SK = "CONFIG"
LEGACY_CONFIG_PK = "CONFIG#GLOBAL"
LEGACY_CONFIG_SK = "CONFIG"

# Top-level keys that make up the rider config.
RIDER_KEYS = ("riderSlots", "riderBonusConfig", "riderSlotsSettings")


def _read_config_map(pk: str, sk: str) -> dict:
    """Return the nested ``config`` map of a config row (empty dict if absent)."""
    response = dynamodb_client.get_item(
        TableName=TABLES["CONFIG"],
        Key={"partitionkey": {"S": pk}, "sortKey": {"S": sk}},
    )
    item = response.get("Item")
    if not item:
        return {}
    config = dynamodb_to_python(item.get("config", {"NULL": True}))
    return config if isinstance(config, dict) else {}


def fetch_rider_config() -> dict:
    """Return ``{riderSlots, riderBonusConfig, riderSlotsSettings}``.

    Reads the dedicated ``CONFIG#RIDER`` row first; if that row doesn't carry any
    of the rider keys yet (not migrated), falls back to the same keys on the legacy
    ``CONFIG#GLOBAL`` row. Returns whatever keys exist — callers default the rest.
    """
    try:
        config = _read_config_map(RIDER_CONFIG_PK, RIDER_CONFIG_SK)
        if any(key in config for key in RIDER_KEYS):
            return config
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to fetch rider config row: {e}")

    try:
        legacy = _read_config_map(LEGACY_CONFIG_PK, LEGACY_CONFIG_SK)
        return {key: legacy[key] for key in RIDER_KEYS if key in legacy}
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to fetch legacy rider config: {e}")
        return {}
