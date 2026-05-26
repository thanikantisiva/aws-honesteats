"""
Toggle the per-restaurant `theaterMode` flag in DynamoDB.

When `theaterMode = "AVAILABLE"`, the restaurant POV web app exposes a
separate "Theater Menu" tab on the Menu page, and the customer app's
theater QR flow becomes usable. When the flag is absent (or set to
anything else), the restaurant operates as a normal delivery-only outlet
and no theater UI is rendered.

Usage examples
--------------

# Enable theater mode for one restaurant (default action when --state omitted)
ENVIRONMENT=prod python scripts/set_restaurant_theater_mode.py \\
    --restaurant-id RES-1774074885558-3227

# Explicit enable
ENVIRONMENT=prod python scripts/set_restaurant_theater_mode.py \\
    --restaurant-id RES-1774074885558-3227 --state AVAILABLE

# Disable (removes the attribute from the item)
ENVIRONMENT=prod python scripts/set_restaurant_theater_mode.py \\
    --restaurant-id RES-1774074885558-3227 --state OFF

The script reads the restaurant's PK (geohash) by scanning the RESTAURANTS
table on the SK `RESTAURANT#<id>` so you don't have to know the geohash.
"""

from __future__ import annotations

import argparse
import os
import sys

import boto3
from botocore.exceptions import ClientError

ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
TABLE_NAME = f"food-delivery-restaurants-{ENVIRONMENT}"
REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")


def _find_restaurant_pk(client, restaurant_id: str) -> str:
    """Locate the geohash PK for a given restaurantId.

    Uses a Scan with a SK filter because the table is keyed on geohash (PK)
    and SK=`RESTAURANT#<id>`. For one-off ops this is cheap; if you call this
    a lot, consider switching to GSI1 on `restaurantId`.
    """
    sk = f"RESTAURANT#{restaurant_id}"
    paginator = client.get_paginator("scan")
    for page in paginator.paginate(
        TableName=TABLE_NAME,
        FilterExpression="SK = :sk",
        ExpressionAttributeValues={":sk": {"S": sk}},
        ProjectionExpression="PK, SK",
    ):
        for item in page.get("Items", []):
            return item["PK"]["S"]
    raise SystemExit(
        f"Restaurant '{restaurant_id}' not found in table {TABLE_NAME!r}. "
        "Double-check the ID and ENVIRONMENT variables."
    )


def set_theater_mode(restaurant_id: str, state: str) -> None:
    client = boto3.client("dynamodb", region_name=REGION)
    pk = _find_restaurant_pk(client, restaurant_id)
    sk = f"RESTAURANT#{restaurant_id}"

    state_upper = state.strip().upper()
    if state_upper in ("OFF", "NONE", "REMOVE", "DISABLED"):
        client.update_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": pk}, "SK": {"S": sk}},
            UpdateExpression="REMOVE theaterMode",
        )
        print(
            f"Removed theaterMode flag from {restaurant_id} "
            f"(env={ENVIRONMENT}, pk={pk}). Theater Menu tab will disappear."
        )
        return

    if state_upper != "AVAILABLE":
        raise SystemExit(
            f"--state must be 'AVAILABLE' or 'OFF', got {state!r}."
        )

    try:
        client.update_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": pk}, "SK": {"S": sk}},
            UpdateExpression="SET theaterMode = :v",
            ExpressionAttributeValues={":v": {"S": "AVAILABLE"}},
        )
    except ClientError as e:
        raise SystemExit(f"DynamoDB update failed: {e}") from e

    print(
        f"Set theaterMode=AVAILABLE on {restaurant_id} "
        f"(env={ENVIRONMENT}, pk={pk}). Theater Menu tab is now active "
        "in the restaurant POV web app."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Toggle the per-restaurant theaterMode flag in DynamoDB.",
    )
    parser.add_argument(
        "--restaurant-id",
        required=True,
        help="Restaurant ID (e.g. RES-1774074885558-3227). Do NOT prefix with 'RESTAURANT#'.",
    )
    parser.add_argument(
        "--state",
        default="AVAILABLE",
        help="'AVAILABLE' to enable theater mode (default), or 'OFF' to remove the flag.",
    )
    args = parser.parse_args()

    rid = args.restaurant_id.strip()
    if rid.startswith("RESTAURANT#"):
        rid = rid.split("#", 1)[1]

    set_theater_mode(rid, args.state)


if __name__ == "__main__":
    main()
