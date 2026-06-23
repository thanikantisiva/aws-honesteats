"""Accessor for the dedicated COD (cash-on-delivery) config row.

The global COD settings — ``disableCod`` (kill switch), ``minAmount`` and
``maxAmount`` (eligible bill range) — live in their own config row so they can be
fetched/edited independently of the big global config map:

    CONFIG#COD / CONFIG  ->  { config: { disableCod, minAmount, maxAmount } }

Reads fall back to the legacy ``CONFIG#GLOBAL.config.codConfig`` block so nothing
breaks before the data is migrated (see scripts/migrate_cod_config.py).

Note: per-customer COD risk flags (disableCod / forceCod on the USERS row) are a
separate concern and are NOT handled here.
"""
from typing import Optional

from aws_lambda_powertools import Logger

from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python

logger = Logger()

COD_CONFIG_PK = "CONFIG#COD"
COD_CONFIG_SK = "CONFIG"
LEGACY_CONFIG_PK = "CONFIG#GLOBAL"
LEGACY_CONFIG_SK = "CONFIG"
LEGACY_COD_KEY = "codConfig"


def parse_hhmm(value) -> Optional[int]:
    """Parse a 24h ``"HH:MM"`` string into minutes-since-midnight.

    Returns None for absent / malformed / out-of-range values.
    """
    if not isinstance(value, str):
        return None
    parts = value.strip().split(":")
    if len(parts) != 2:
        return None
    try:
        hours, minutes = int(parts[0]), int(parts[1])
    except (TypeError, ValueError):
        return None
    if not (0 <= hours <= 23 and 0 <= minutes <= 59):
        return None
    return hours * 60 + minutes


def _read_config_map(pk: str, sk: str) -> Optional[dict]:
    """Return a config row's nested ``config`` map, or None if the row is absent."""
    response = dynamodb_client.get_item(
        TableName=TABLES["CONFIG"],
        Key={"partitionkey": {"S": pk}, "sortKey": {"S": sk}},
    )
    item = response.get("Item")
    if not item:
        return None
    config = dynamodb_to_python(item.get("config", {"NULL": True}))
    return config if isinstance(config, dict) else {}


def fetch_cod_config() -> dict:
    """Return the COD settings ``{disableCod, minAmount, maxAmount}``.

    Reads the dedicated ``CONFIG#COD`` row first (using it even if empty, so an
    intentionally-empty config isn't overridden); if that row doesn't exist yet,
    falls back to the legacy ``CONFIG#GLOBAL.config.codConfig`` block.
    """
    try:
        config = _read_config_map(COD_CONFIG_PK, COD_CONFIG_SK)
        if config is not None:
            return config
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to fetch COD config row: {e}")

    try:
        legacy = _read_config_map(LEGACY_CONFIG_PK, LEGACY_CONFIG_SK) or {}
        cod = legacy.get(LEGACY_COD_KEY)
        return cod if isinstance(cod, dict) else {}
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to fetch legacy COD config: {e}")
        return {}
