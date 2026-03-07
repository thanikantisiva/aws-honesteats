#!/usr/bin/env python3
"""
Backfill subCategory for all menu items.

Rules:
- If subCategory already exists: skip
- Else if category exists: set subCategory = category
- Else: set subCategory = <default-subcategory>

Usage:
  python3 scripts/backfill_menu_sub_category.py --env dev
  python3 scripts/backfill_menu_sub_category.py --table food-delivery-menu-items-dev --dry-run
"""

import argparse
import boto3


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill subCategory in menu items table")
    parser.add_argument("--env", default="dev", help="Environment suffix (default: dev)")
    parser.add_argument("--table", help="Full DynamoDB table name (overrides --env)")
    parser.add_argument(
        "--default-subcategory",
        default="GENERAL",
        help="Fallback subCategory when category is empty (default: GENERAL)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing to DB")
    return parser.parse_args()


def resolve_sub_category(item, default_sub_category: str) -> str:
    category = item.get("category", {}).get("S", "").strip()
    if category:
        return category
    return default_sub_category


def main():
    args = parse_args()
    table_name = args.table or f"food-delivery-menu-items-{args.env}"
    default_sub_category = args.default_subcategory.strip() or "GENERAL"

    dynamodb = boto3.client("dynamodb")

    scanned = 0
    updated = 0
    skipped = 0
    failed = 0
    last_evaluated_key = None

    print(f"Table: {table_name}")
    print(f"Default subCategory: {default_sub_category}")
    print(f"Dry run: {args.dry_run}")

    while True:
        scan_kwargs = {
            "TableName": table_name,
            "ProjectionExpression": "PK, SK, itemId, category, subCategory",
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = dynamodb.scan(**scan_kwargs)
        items = response.get("Items", [])
        scanned += len(items)

        for item in items:
            pk = item.get("PK", {}).get("S")
            sk = item.get("SK", {}).get("S")
            item_id = item.get("itemId", {}).get("S", "")

            if not pk or not sk:
                skipped += 1
                continue

            if item.get("subCategory", {}).get("S", "").strip():
                skipped += 1
                continue

            new_sub_category = resolve_sub_category(item, default_sub_category)

            if args.dry_run:
                print(f"[DRY-RUN] itemId={item_id} PK={pk} SK={sk} -> subCategory={new_sub_category}")
                updated += 1
                continue

            try:
                dynamodb.update_item(
                    TableName=table_name,
                    Key={"PK": {"S": pk}, "SK": {"S": sk}},
                    UpdateExpression="SET subCategory = :subCategory",
                    ExpressionAttributeValues={":subCategory": {"S": new_sub_category}},
                )
                updated += 1
            except Exception as e:
                failed += 1
                print(f"Failed itemId={item_id} PK={pk} SK={sk}: {str(e)}")

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    print(
        f"Backfill complete | scanned={scanned} updated={updated} skipped={skipped} failed={failed}"
    )


if __name__ == "__main__":
    main()
