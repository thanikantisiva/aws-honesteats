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


def _find_all_restaurant_pks(client, restaurant_id: str) -> list[str]:
    """Locate ALL geohash PKs for a given restaurantId via the GSI.

    Historically a few restaurants ended up with duplicate rows (one at the
    old geohash, one at the new) when the location was updated by an early
    version of `update_restaurant`. The API reads through `restaurantId-index`
    and may return EITHER row, so this script also writes to every match —
    that way `theaterMode` is set regardless of which row the API picks up.

    Returns:
        List of PK (geohash) strings. Always at least one; may be more.
    """
    response = client.query(
        TableName=TABLE_NAME,
        IndexName="restaurantId-index",
        KeyConditionExpression="restaurantId = :rid",
        ExpressionAttributeValues={":rid": {"S": restaurant_id}},
        ProjectionExpression="PK",
    )
    items = response.get("Items", [])
    if not items:
        raise SystemExit(
            f"Restaurant '{restaurant_id}' not found in table {TABLE_NAME!r} "
            f"via restaurantId-index. Double-check the ID and ENVIRONMENT."
        )
    return [it["PK"]["S"] for it in items]


def set_theater_mode(restaurant_id: str, state: str) -> None:
    client = boto3.client("dynamodb", region_name=REGION)
    pks = _find_all_restaurant_pks(client, restaurant_id)
    sk = f"RESTAURANT#{restaurant_id}"

    if len(pks) > 1:
        print(
            f"⚠️  Found {len(pks)} rows for {restaurant_id} (duplicate geohashes: {pks}). "
            "Writing to ALL of them so the API can't pick up a stale one."
        )

    state_upper = state.strip().upper()
    if state_upper in ("OFF", "NONE", "REMOVE", "DISABLED"):
        for pk in pks:
            client.update_item(
                TableName=TABLE_NAME,
                Key={"PK": {"S": pk}, "SK": {"S": sk}},
                UpdateExpression="REMOVE theaterMode",
            )
            print(f"  removed theaterMode (env={ENVIRONMENT}, pk={pk})")
        print(
            f"Removed theaterMode from {restaurant_id}. "
            "Theater Menu tab will disappear."
        )
        return

    if state_upper != "AVAILABLE":
        raise SystemExit(
            f"--state must be 'AVAILABLE' or 'OFF', got {state!r}."
        )

    for pk in pks:
        try:
            client.update_item(
                TableName=TABLE_NAME,
                Key={"PK": {"S": pk}, "SK": {"S": sk}},
                UpdateExpression="SET theaterMode = :v",
                ExpressionAttributeValues={":v": {"S": "AVAILABLE"}},
            )
            print(f"  set theaterMode=AVAILABLE (env={ENVIRONMENT}, pk={pk})")
        except ClientError as e:
            raise SystemExit(f"DynamoDB update failed for pk={pk}: {e}") from e

    print(
        f"Set theaterMode=AVAILABLE on {restaurant_id}. "
        "Theater Menu tab is now active in the restaurant POV web app."
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
