"""Accessor for the dedicated YumCoins config row.

The YumCoins tunables — ``walletConfig`` (redemption), ``referralConfig``
(refer-and-earn) and ``orderCashbackConfig`` (delivery cashback) — live in their
own config row so they can be fetched/edited independently of the big global
config map:

    CONFIG#YUMCOINS / CONFIG  ->  { config: { walletConfig, referralConfig, orderCashbackConfig } }

Reads fall back to the legacy ``CONFIG#GLOBAL`` keys so nothing breaks before the
data is migrated over (see scripts/migrate_yumcoins_config.py).
"""
from aws_lambda_powertools import Logger

from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python

logger = Logger()

YUMCOINS_CONFIG_PK = "CONFIG#YUMCOINS"
YUMCOINS_CONFIG_SK = "CONFIG"
LEGACY_CONFIG_PK = "CONFIG#GLOBAL"
LEGACY_CONFIG_SK = "CONFIG"

# Top-level keys that make up the YumCoins config.
YUMCOINS_KEYS = ("walletConfig", "referralConfig", "orderCashbackConfig")


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


def fetch_yumcoins_config() -> dict:
    """Return ``{walletConfig, referralConfig, orderCashbackConfig}``.

    Reads the dedicated ``CONFIG#YUMCOINS`` row first; if that row is absent (not
    yet migrated), falls back to the same keys on the legacy ``CONFIG#GLOBAL`` row.
    Returns whatever keys exist — callers default missing pieces themselves.
    """
    try:
        config = _read_config_map(YUMCOINS_CONFIG_PK, YUMCOINS_CONFIG_SK)
        if any(key in config for key in YUMCOINS_KEYS):
            return config
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to fetch YumCoins config row: {e}")

    try:
        legacy = _read_config_map(LEGACY_CONFIG_PK, LEGACY_CONFIG_SK)
        return {key: legacy[key] for key in YUMCOINS_KEYS if key in legacy}
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to fetch legacy YumCoins config: {e}")
        return {}
