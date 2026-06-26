"""Effective config = global config with per-restaurant overrides (per field).

Reads ``CONFIG#GLOBAL`` and ``CONFIG#RESTAURANT#<id>`` and shallow-merges them so any
field set on the restaurant row wins, and any field absent there falls back to the
global value — exactly how ``restaurantCommissionPercentage`` already resolves
(restaurant → global). Used for delivery-fee config, hike thresholds, etc.
"""
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from utils.datetime_ist import now_ist_iso
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python, python_to_dynamodb

logger = Logger()

GLOBAL_CONFIG_PK = "CONFIG#GLOBAL"
CONFIG_SK = "CONFIG"

# Numeric fields the structured config editor manages (global + per-restaurant).
# Anything not listed is left untouched on the row.
EDITABLE_FIELDS = (
    "platformFee",
    "riderBaseFare",
    "riderBaseFareApplicableUnderKms",
    "riderFarePerKm",
    "customerViewRiderFarePerKm",
    "riderFreeDeliveryBelowKm",
    "freeDeliveryAboveThreshold",
    "maxDeliveryRadiusKm",
    "restaurantCommissionPercentage",
    "default",
    "below100",
    "below200",
    "below300",
    "below400",
)


def _restaurant_config_pk(restaurant_id: str) -> str:
    return f"CONFIG#RESTAURANT#{str(restaurant_id).strip()}"


def _read_config_map(pk: str) -> dict:
    try:
        response = dynamodb_client.get_item(
            TableName=TABLES["CONFIG"],
            Key={"partitionkey": {"S": pk}, "sortKey": {"S": CONFIG_SK}},
        )
        item = response.get("Item")
        if not item:
            return {}
        config = dynamodb_to_python(item.get("config", {"NULL": True}))
        return config if isinstance(config, dict) else {}
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to read config row {pk}: {e}")
        return {}


def fetch_effective_config(restaurant_id=None) -> dict:
    """Return global config merged with the restaurant's per-field overrides.

    A field present on ``CONFIG#RESTAURANT#<id>`` overrides global; a field absent
    there uses the global value. With no restaurant id, returns the global config.
    """
    global_cfg = _read_config_map(GLOBAL_CONFIG_PK)
    rid = str(restaurant_id or "").strip()
    if not rid:
        return global_cfg
    resto_cfg = _read_config_map(_restaurant_config_pk(rid))
    return {**global_cfg, **resto_cfg}


def restaurant_has_own_config(restaurant_id) -> bool:
    """True when the restaurant has its own (non-empty) ``CONFIG#RESTAURANT#<id>`` row."""
    rid = str(restaurant_id or "").strip()
    if not rid:
        return False
    return bool(_read_config_map(_restaurant_config_pk(rid)))


def patch_config_fields(restaurant_id, fields: dict) -> dict:
    """Write ``fields`` onto the target config row, preserving every other key.

    Target is ``CONFIG#RESTAURANT#<id>`` when a restaurant id is given, else
    ``CONFIG#GLOBAL``. Each field is SET under the ``config`` map without touching
    siblings (so global keys like ``codConfig`` or a restaurant's item-commission
    overrides are never clobbered). When the row/``config`` map doesn't exist yet —
    e.g. a restaurant being configured for the first time — it's created from the
    provided fields. Returns the resulting effective config for the target.
    """
    rid = str(restaurant_id or "").strip()
    if not isinstance(fields, dict) or not fields:
        return fetch_effective_config(rid or None)

    pk = _restaurant_config_pk(rid) if rid else GLOBAL_CONFIG_PK
    key = {"partitionkey": {"S": pk}, "sortKey": {"S": CONFIG_SK}}

    names = {"#config": "config"}
    set_parts = []
    values = {}
    for i, (field, val) in enumerate(fields.items()):
        nk, vk = f"#k{i}", f":v{i}"
        names[nk] = field
        set_parts.append(f"#config.{nk} = {vk}")
        values[vk] = python_to_dynamodb(val)
    values[":u"] = {"S": now_ist_iso()}
    set_parts.append("updatedAt = :u")

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
        # Row (or its config map) doesn't exist yet — create it via a full merge-put.
        config = _read_config_map(pk)
        config.update(fields)
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

    return fetch_effective_config(rid or None)
