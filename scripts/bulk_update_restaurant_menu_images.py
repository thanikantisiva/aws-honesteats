#!/usr/bin/env python3
"""
Set the same `image` (DynamoDB list of one URL) on every menu item for a restaurant.

Queries MENU_ITEMS by PK = RESTAURANT#{restaurantId}, updates rows where SK begins with ITEM#
(skips markers like SK=CHANGED).

Requires AWS credentials with dynamodb:Query + dynamodb:UpdateItem on the target table.

Usage (prod table name matches SAM: food-delivery-menu-items-prod):
  export AWS_PROFILE=your-prod-profile   # or default creds for prod account
  python3 scripts/bulk_update_restaurant_menu_images.py --dry-run
  python3 scripts/bulk_update_restaurant_menu_images.py --apply

Env:
  MENU_ITEMS_TABLE_NAME — override table (default: food-delivery-menu-items-{ENVIRONMENT})
  ENVIRONMENT           — default prod for this script's table suffix
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Any, Iterator

import boto3
from botocore.exceptions import ClientError

DEFAULT_RESTAURANT_ID = "RES-1777091893663-3064"
DEFAULT_IMAGE_URL = (
    "https://d1ndj8fhsl2av0.cloudfront.net/restaurant-images/subcategory/20260430-070511-1.jpg"
)


def _table_name(args: argparse.Namespace) -> str:
    if args.table_name:
        return args.table_name
    env = args.environment.strip() or "prod"
    return os.environ.get("MENU_ITEMS_TABLE_NAME") or f"food-delivery-menu-items-{env}"


def iter_menu_item_rows(
    client: Any,
    table: str,
    restaurant_id: str,
) -> Iterator[dict]:
    pk = f"RESTAURANT#{restaurant_id}"
    eks: dict | None = None
    while True:
        kwargs: dict = {
            "TableName": table,
            "KeyConditionExpression": "PK = :pk",
            "ExpressionAttributeValues": {":pk": {"S": pk}},
        }
        if eks:
            kwargs["ExclusiveStartKey"] = eks
        resp = client.query(**kwargs)
        for item in resp.get("Items", []):
            sk = item.get("SK", {}).get("S", "")
            if sk.startswith("ITEM#"):
                yield item
        eks = resp.get("LastEvaluatedKey")
        if not eks:
            break


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--restaurant-id",
        default=DEFAULT_RESTAURANT_ID,
        help="Restaurant id (PK suffix)",
    )
    p.add_argument(
        "--image-url",
        default=DEFAULT_IMAGE_URL,
        help="Image URL to set on every item (stored as image = [url])",
    )
    p.add_argument(
        "--environment",
        default="prod",
        help="Table suffix when --table-name omitted (default: prod)",
    )
    p.add_argument(
        "--table-name",
        default="",
        help="Full DynamoDB table name (overrides MENU_ITEMS_TABLE_NAME and --environment)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="List items only, no updates (default)",
    )
    p.add_argument("--apply", action="store_true", help="Perform UpdateItem for each menu row")
    p.add_argument(
        "--delay-sec",
        type=float,
        default=0.0,
        help="Sleep between updates (throttle)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.apply:
        args.dry_run = False

    table = _table_name(args)
    restaurant_id = args.restaurant_id.strip()
    image_url = args.image_url.strip()

    if not restaurant_id or not image_url:
        print("restaurant-id and image-url must be non-empty", file=sys.stderr)
        return 1

    client = boto3.client("dynamodb")
    print(f"Table:       {table}")
    print(f"Restaurant:  {restaurant_id}")
    print(f"Image URL:   {image_url[:72]}…" if len(image_url) > 72 else f"Image URL:   {image_url}")
    print(f"Mode:        {'DRY-RUN' if args.dry_run else 'APPLY'}")
    print()

    rows = list(iter_menu_item_rows(client, table, restaurant_id))
    if not rows:
        print("No ITEM# rows found for this restaurant.", file=sys.stderr)
        return 1

    print(f"Found {len(rows)} menu item(s).")

    import time

    for i, item in enumerate(rows[:5], 1):
        name = item.get("itemName", {}).get("S", "?")
        sk = item.get("SK", {}).get("S", "")
        print(f"  {i}. {sk} — {name[:60]}")
    if len(rows) > 5:
        print(f"  ... +{len(rows) - 5} more")

    if args.dry_run:
        print("\nDry run — no writes. Pass --apply to update DynamoDB.")
        return 0

    ok = err = 0
    for item in rows:
        pk = item["PK"]
        sk = item["SK"]
        try:
            client.update_item(
                TableName=table,
                Key={"PK": pk, "SK": sk},
                UpdateExpression="SET #img = :img",
                ExpressionAttributeNames={"#img": "image"},
                ExpressionAttributeValues={":img": {"L": [{"S": image_url}]}},
            )
            ok += 1
            if ok % 25 == 0:
                print(f"  ... {ok}/{len(rows)} updated")
        except ClientError as e:
            err += 1
            sk_s = sk.get("S", "?") if isinstance(sk, dict) else str(sk)
            print(f"ERROR {sk_s}: {e}", file=sys.stderr)
        if args.delay_sec > 0:
            time.sleep(args.delay_sec)

    print(f"\nDone: {ok} updated, {err} failed.")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
