#!/usr/bin/env python3
"""
Backfill ``entryType = ORDER_EARNING`` for every rider-earnings row that
represents a regular delivery payout but has no ``entryType`` attribute yet.

Rules (idempotent — safe to re-run):
  * Skip rows that already have ``entryType`` set (any value).
  * Skip rows whose ``date`` sort-key contains ``#COD#``     → COD rows.
  * Skip rows whose ``date`` sort-key contains ``#BONUS#``   → milestone-bonus rows.
  * For every other row, ``SET entryType = 'ORDER_EARNING'`` guarded by
    ``attribute_not_exists(entryType)``.

Earnings sort-key formats (for reference):
  ``YYYY-MM-DD#<orderId>``                      → ORDER_EARNING        (this script targets these)
  ``YYYY-MM-DD#BONUS#<startDate>#<stops>``      → MILESTONE_BONUS      (already stamped on write)
  ``YYYY-MM-DD#COD#<orderId>``                  → COD_AMOUNT_COLLECTED (backfilled by sibling script)

Usage:
  python3 scripts/backfill_order_earning_entry_type.py --env dev --dry-run
  python3 scripts/backfill_order_earning_entry_type.py --env dev
  python3 scripts/backfill_order_earning_entry_type.py --env prod
  python3 scripts/backfill_order_earning_entry_type.py --rider RID-123 --dry-run
  python3 scripts/backfill_order_earning_entry_type.py --table food-delivery-rider-earnings-dev
"""

from __future__ import annotations

import argparse
import sys

import boto3
from botocore.exceptions import ClientError


COD_MARKER = "#COD#"
BONUS_MARKER = "#BONUS#"
ENTRY_TYPE_VALUE = "ORDER_EARNING"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill entryType=ORDER_EARNING on delivery earnings rows")
    parser.add_argument("--env", default="dev", help="Environment suffix used in default table name")
    parser.add_argument("--table", help="Full DynamoDB table name (overrides --env)")
    parser.add_argument("--rider", help="Restrict scan to a single rider (uses Query instead of Scan)")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing to DB")
    parser.add_argument("--page-size", type=int, default=200, help="DynamoDB page size (default: 200)")
    return parser.parse_args()


def iter_rows(client, table_name: str, rider_id: str | None, page_size: int):
    """Yield earnings items missing ``entryType``.

    Server-side filter narrows to rows that don't already have ``entryType``,
    and excludes COD / BONUS rows via SK ``contains()`` filters so we don't
    accidentally tag them.
    """
    base_attr_names = {"#d": "date", "#et": "entryType"}
    base_attr_values = {
        ":cod": {"S": COD_MARKER},
        ":bonus": {"S": BONUS_MARKER},
    }
    filter_expr = (
        "attribute_not_exists(#et) "
        "AND NOT contains(#d, :cod) "
        "AND NOT contains(#d, :bonus)"
    )

    last_key = None
    while True:
        if rider_id:
            kwargs = {
                "TableName": table_name,
                "KeyConditionExpression": "riderId = :rid",
                "FilterExpression": filter_expr,
                "ExpressionAttributeNames": base_attr_names,
                "ExpressionAttributeValues": {**base_attr_values, ":rid": {"S": rider_id}},
                "Limit": page_size,
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = client.query(**kwargs)
        else:
            kwargs = {
                "TableName": table_name,
                "FilterExpression": filter_expr,
                "ExpressionAttributeNames": base_attr_names,
                "ExpressionAttributeValues": base_attr_values,
                "Limit": page_size,
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = client.scan(**kwargs)

        for item in resp.get("Items", []):
            yield item

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            return


def main() -> int:
    args = parse_args()
    table_name = args.table or f"food-delivery-rider-earnings-{args.env}"
    client = boto3.client("dynamodb")

    print(f"Table   : {table_name}")
    print(f"Rider   : {args.rider or '<all>'}")
    print(f"Dry-run : {args.dry_run}")
    print(f"Setting : entryType = '{ENTRY_TYPE_VALUE}'")
    print(f"Skip if : entryType present OR date contains '{COD_MARKER}' OR '{BONUS_MARKER}'\n")

    matched = updated = skipped = failed = 0

    for item in iter_rows(client, table_name, args.rider, args.page_size):
        rider_id = item.get("riderId", {}).get("S")
        date_sk = item.get("date", {}).get("S")

        if not rider_id or not date_sk:
            continue

        # Belt-and-suspenders client-side guards (server filter already excludes these)
        if COD_MARKER in date_sk or BONUS_MARKER in date_sk:
            skipped += 1
            continue
        if item.get("entryType", {}).get("S", "").strip():
            skipped += 1
            continue

        matched += 1

        if args.dry_run:
            updated += 1
            print(f"  [DRY-RUN] {rider_id} | {date_sk}  →  entryType={ENTRY_TYPE_VALUE}")
            continue

        try:
            client.update_item(
                TableName=table_name,
                Key={"riderId": {"S": rider_id}, "date": {"S": date_sk}},
                UpdateExpression="SET entryType = :et",
                ConditionExpression="attribute_not_exists(entryType)",
                ExpressionAttributeValues={":et": {"S": ENTRY_TYPE_VALUE}},
            )
            updated += 1
            print(f"  ✓ {rider_id} | {date_sk}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "ConditionalCheckFailedException":
                # Another writer beat us to it — treat as skipped.
                skipped += 1
                continue
            failed += 1
            print(f"  ✗ {rider_id} | {date_sk}: {e}", file=sys.stderr)

    print(
        f"\nDone | matched={matched} updated={updated} "
        f"skipped={skipped} failed={failed}"
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
