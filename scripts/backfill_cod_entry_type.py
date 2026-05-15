#!/usr/bin/env python3
"""
Backfill ``entryType = COD_AMOUNT_COLLECTED`` for every rider-earnings row whose
sort-key matches the COD pattern ``<YYYY-MM-DD>#COD#<orderId>``.

Why: ``EarningsService.record_cash_collected`` did not stamp an ``entryType`` on
COD rows historically, so older rows are missing the field.  Now that
``ENTRY_TYPE_COD_AMOUNT_COLLECTED`` exists, this script brings legacy rows in
line with new writes.

Rules:
- Skip rows whose ``date`` sort-key does NOT contain ``#COD#``.
- Skip rows that already have ``entryType`` set (idempotent).
- For matching rows: ``SET entryType = 'COD_AMOUNT_COLLECTED'``.

Usage:
  python3 scripts/backfill_cod_entry_type.py --env dev
  python3 scripts/backfill_cod_entry_type.py --table food-delivery-rider-earnings-dev --dry-run
  python3 scripts/backfill_cod_entry_type.py --env prod                       # apply against prod
  python3 scripts/backfill_cod_entry_type.py --rider RID-123 --dry-run        # restrict to one rider
"""

from __future__ import annotations

import argparse
import sys

import boto3
from botocore.exceptions import ClientError


COD_MARKER = "#COD#"
ENTRY_TYPE_VALUE = "COD_AMOUNT_COLLECTED"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill entryType on COD earnings rows")
    parser.add_argument("--env", default="dev", help="Environment suffix used in default table name")
    parser.add_argument("--table", help="Full DynamoDB table name (overrides --env)")
    parser.add_argument("--rider", help="Restrict scan to a single rider (uses Query instead of Scan)")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing to DB")
    parser.add_argument("--page-size", type=int, default=200, help="DynamoDB page size (default: 200)")
    return parser.parse_args()


def iter_rows(client, table_name: str, rider_id: str | None, page_size: int):
    """Yield earnings items, either via Query (single rider) or Scan (full table)."""
    last_key = None
    while True:
        if rider_id:
            # `contains` is not allowed in a KeyConditionExpression sort-key
            # comparator, so we Query on the partition key and filter the SK.
            kwargs = {
                "TableName": table_name,
                "KeyConditionExpression": "riderId = :rid",
                "FilterExpression": "contains(#d, :cod)",
                "ExpressionAttributeNames": {"#d": "date"},
                "ExpressionAttributeValues": {
                    ":rid": {"S": rider_id},
                    ":cod": {"S": COD_MARKER},
                },
                "Limit": page_size,
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = client.query(**kwargs)
        else:
            kwargs = {
                "TableName": table_name,
                "FilterExpression": "contains(#d, :cod)",
                "ExpressionAttributeNames": {"#d": "date"},
                "ExpressionAttributeValues": {":cod": {"S": COD_MARKER}},
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
    print(f"Marker  : '{COD_MARKER}'  →  entryType='{ENTRY_TYPE_VALUE}'\n")

    matched = updated = skipped = failed = 0

    for item in iter_rows(client, table_name, args.rider, args.page_size):
        rider_id = item.get("riderId", {}).get("S")
        date_sk = item.get("date", {}).get("S")

        if not rider_id or not date_sk or COD_MARKER not in date_sk:
            continue

        matched += 1
        existing = item.get("entryType", {}).get("S", "").strip()
        if existing:
            skipped += 1
            if existing != ENTRY_TYPE_VALUE:
                print(f"  - {rider_id} | {date_sk} | already entryType={existing!r}, skipping")
            continue

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
                # Lost a race — another writer filled it in. Treat as skipped.
                skipped += 1
                continue
            failed += 1
            print(f"  ✗ {rider_id} | {date_sk}: {e}", file=sys.stderr)

    print(
        f"\nDone | matched={matched} updated={updated} "
        f"skipped(already set)={skipped} failed={failed}"
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
