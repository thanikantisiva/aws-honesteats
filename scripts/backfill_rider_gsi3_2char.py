"""Backfill GSI3PK on the riders table to a 2-character geohash prefix.

Riders normally refresh their location every ~25 seconds, so writers will
naturally migrate `GSI3PK` to the 2-char prefix within a few minutes after
deploy. This one-time script reduces the transient empty-result window for
`find_available_riders_near` immediately after the switch.

For each rider whose `GSI3PK` is not already exactly 2 characters long, this
script computes the new prefix from the rider's current `geohash` (or `lat`/
`lng` as a fallback) and updates `GSI3PK` only. `GSI3SK` is left as-is.

Usage:
  python3 scripts/backfill_rider_gsi3_2char.py --table food-delivery-riders-prod
  python3 scripts/backfill_rider_gsi3_2char.py --table food-delivery-riders-prod --dry-run
"""

import argparse
import os
import sys

import boto3

# Allow running from repo root or scripts/ dir
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.geohash import encode as geohash_encode  # noqa: E402


def derive_prefix(item: dict) -> str:
    """Derive the desired 2-char GSI3PK from a rider item."""
    geohash_attr = item.get("geohash") or {}
    if "S" in geohash_attr and geohash_attr["S"]:
        return geohash_attr["S"][:2]

    lat_attr = item.get("lat") or {}
    lng_attr = item.get("lng") or {}
    if "N" in lat_attr and "N" in lng_attr:
        try:
            lat = float(lat_attr["N"])
            lng = float(lng_attr["N"])
            return geohash_encode(lat, lng, precision=7)[:2]
        except (TypeError, ValueError):
            return ""
    return ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="Riders table name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only log changes; do not write to DynamoDB",
    )
    args = parser.parse_args()

    dynamodb = boto3.client("dynamodb")
    table_name = args.table

    scanned = 0
    updated = 0
    skipped = 0
    missing_geohash = 0
    failed = 0
    last_evaluated_key = None

    while True:
        scan_kwargs = {
            "TableName": table_name,
            "ProjectionExpression": "riderId, geohash, GSI3PK, GSI3SK, lat, lng",
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = dynamodb.scan(**scan_kwargs)

        for item in response.get("Items", []):
            scanned += 1

            rider_id_attr = item.get("riderId") or {}
            rider_id = rider_id_attr.get("S")
            if not rider_id:
                skipped += 1
                continue

            current_prefix_attr = item.get("GSI3PK") or {}
            current_prefix = current_prefix_attr.get("S", "")

            new_prefix = derive_prefix(item)
            if not new_prefix or len(new_prefix) < 2:
                missing_geohash += 1
                continue

            new_prefix = new_prefix[:2]

            if current_prefix == new_prefix:
                skipped += 1
                continue

            if args.dry_run:
                print(
                    f"[DRY RUN] riderId={rider_id} GSI3PK '{current_prefix}' -> '{new_prefix}'"
                )
                updated += 1
                continue

            try:
                dynamodb.update_item(
                    TableName=table_name,
                    Key={"riderId": {"S": rider_id}},
                    UpdateExpression="SET GSI3PK = :pk",
                    ExpressionAttributeValues={":pk": {"S": new_prefix}},
                )
                updated += 1
                if updated % 100 == 0:
                    print(f"Updated {updated} riders so far...")
            except Exception as e:
                failed += 1
                print(f"FAILED riderId={rider_id}: {e}")

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    print("---")
    print(f"Scanned:         {scanned}")
    print(f"Updated:         {updated}{' (dry-run)' if args.dry_run else ''}")
    print(f"Skipped:         {skipped}")
    print(f"No geohash/loc:  {missing_geohash}")
    print(f"Failed:          {failed}")


if __name__ == "__main__":
    main()
