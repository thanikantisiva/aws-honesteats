#!/usr/bin/env python3
"""
Bulk add or wipe the `image` URL on EVERY menu item across ALL restaurants.

The menu-items table (food-delivery-menu-items-{env}) stores each item's image under
the `image` attribute as a DynamoDB List of String (a list of URLs). This script scans
the whole table and, depending on --action:

  add_image     SET image = [<--image-url>] on every menu item row
  delete_image  REMOVE the image attribute from every menu item row (wipe it out)

The table is single-table-style: the same partitions also hold non-item rows
(e.g. SK="COUNTER#PICKUP#YYYYMMDD"), so only rows whose SK begins with "ITEM#" are
ever touched.

Safety: dry-run by DEFAULT. Nothing is written until you pass --apply.

Credentials (same as AWS CLI / boto3):
  - Environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, optional AWS_SESSION_TOKEN,
    AWS_DEFAULT_REGION / AWS_REGION
  - Or: --profile NAME
  - Or: --access-key-id and --secret-access-key (optional --session-token)
  - Region defaults to ap-south-1 unless overridden by --region / env.

Usage:
  # Preview adding an image to every dev item (no writes):
  python3.11 scripts/bulk_menu_item_images.py --env dev --action add_image \
      --image-url "https://cdn.example.com/x.jpg"

  # Apply it:
  python3.11 scripts/bulk_menu_item_images.py --env dev --action add_image \
      --image-url "https://cdn.example.com/x.jpg" --apply

  # Wipe images off every dev item:
  python3.11 scripts/bulk_menu_item_images.py --env dev --action delete_image --apply

  # Target prod (use the prod account creds/profile):
  AWS_PROFILE=awsprod python3.11 scripts/bulk_menu_item_images.py \
      --env prod --action delete_image --apply

NOTE: run with python3.11/python3.12 (the system python3 3.8 has a broken boto3).
NOTE: this fires the menu-items DynamoDB stream handler for every row updated.
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Any

import boto3

ADD = "add_image"
DELETE = "delete_image"
ITEM_PREFIX = "ITEM#"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bulk add/delete the menu item `image` URL across all restaurants",
    )
    p.add_argument(
        "--action",
        required=True,
        choices=[ADD, DELETE],
        help=f"{ADD}: set image=[--image-url] on all items; {DELETE}: remove image from all items",
    )
    p.add_argument("--env", default="dev", help="Table suffix when --table omitted (default: dev)")
    p.add_argument("--table", help="Full DynamoDB table name (overrides --env)")
    p.add_argument(
        "--image-url",
        dest="image_url",
        help="Image URL to set on every item (required for add_image; stored as image = [url])",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Preview only, no writes (default)",
    )
    p.add_argument("--apply", action="store_true", help="Actually write the changes to DynamoDB")
    p.add_argument("--page-size", type=int, default=200, help="Scan page size (default: 200)")
    p.add_argument("--region", help="AWS region (default: env or ap-south-1)")
    p.add_argument("--profile", help="Named AWS profile (~/.aws/credentials)")
    p.add_argument("--access-key-id", dest="access_key_id", help="AWS access key (explicit creds)")
    p.add_argument("--secret-access-key", dest="secret_access_key", help="AWS secret key")
    p.add_argument("--session-token", dest="session_token", help="AWS session token (temp creds)")
    p.add_argument(
        "--sleep",
        type=float,
        default=0,
        help="Seconds to sleep after each successful update (throttle; default: 0)",
    )
    return p.parse_args()


def _boto_session(args: argparse.Namespace) -> boto3.session.Session:
    sk: dict[str, Any] = {
        "region_name": args.region
        or os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION")
        or "ap-south-1",
    }
    if args.access_key_id and args.secret_access_key:
        sk["aws_access_key_id"] = args.access_key_id
        sk["aws_secret_access_key"] = args.secret_access_key
        if args.session_token:
            sk["aws_session_token"] = args.session_token
    elif args.profile:
        sk["profile_name"] = args.profile
    return boto3.Session(**sk)


def _dynamo_image_strings(image_attr: dict[str, Any] | None) -> list[str] | None:
    """Return the URLs in an `image` attribute, tolerating legacy single-string shape.

    None  -> attribute absent / unrecognized shape
    []    -> present but empty
    [...] -> list of URL strings
    """
    if not image_attr:
        return None
    if "S" in image_attr:
        s = image_attr["S"]
        return [s] if isinstance(s, str) and s.strip() else []
    if "L" not in image_attr:
        return None
    out: list[str] = []
    for el in image_attr["L"]:
        if isinstance(el, dict) and "S" in el and isinstance(el["S"], str):
            out.append(el["S"])
    return out


def main() -> int:
    args = parse_args()
    if args.apply:
        args.dry_run = False

    if args.action == ADD:
        if not args.image_url or not args.image_url.strip():
            raise SystemExit("--image-url is required for --action add_image")
    image_url = (args.image_url or "").strip()

    if (args.access_key_id or args.secret_access_key) and not (
        args.access_key_id and args.secret_access_key
    ):
        raise SystemExit("Both --access-key-id and --secret-access-key are required for explicit keys")

    table_name = args.table or f"food-delivery-menu-items-{args.env}"
    session = _boto_session(args)
    dynamodb = session.client("dynamodb")

    print(f"Table:    {table_name}")
    print(f"Action:   {args.action}")
    if args.action == ADD:
        shown = image_url if len(image_url) <= 80 else image_url[:79] + "…"
        print(f"ImageURL: {shown}")
    print(f"Mode:     {'DRY-RUN (no writes)' if args.dry_run else 'APPLY'}")
    print(f"Region:   {session.region_name}")
    print()

    # add_image: every ITEM# row. delete_image: only ITEM# rows that currently have an image.
    if args.action == DELETE:
        filter_expr = "begins_with(SK, :p) AND attribute_exists(image)"
    else:
        filter_expr = "begins_with(SK, :p)"

    scanned_items = 0
    matched = 0
    updated = 0
    skipped_unchanged = 0
    failed = 0
    last_evaluated_key = None

    while True:
        scan_kwargs: dict[str, Any] = {
            "TableName": table_name,
            "ProjectionExpression": "PK, SK, itemId, itemName, image",
            "FilterExpression": filter_expr,
            "ExpressionAttributeValues": {":p": {"S": ITEM_PREFIX}},
            "Limit": args.page_size,
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = dynamodb.scan(**scan_kwargs)

        for item in response.get("Items", []):
            pk = item.get("PK", {}).get("S")
            sk = item.get("SK", {}).get("S")
            # Belt-and-suspenders: never touch non-item rows (COUNTER#, etc.)
            if not pk or not sk or not sk.startswith(ITEM_PREFIX):
                continue
            scanned_items += 1
            item_id = item.get("itemId", {}).get("S", "")
            name = item.get("itemName", {}).get("S", "")

            if args.action == ADD:
                current = _dynamo_image_strings(item.get("image"))
                if current == [image_url]:
                    skipped_unchanged += 1
                    continue
                update_kwargs = {
                    "UpdateExpression": "SET #img = :img",
                    "ExpressionAttributeNames": {"#img": "image"},
                    "ExpressionAttributeValues": {":img": {"L": [{"S": image_url}]}},
                }
            else:  # DELETE
                update_kwargs = {
                    "UpdateExpression": "REMOVE #img",
                    "ExpressionAttributeNames": {"#img": "image"},
                }

            matched += 1

            if args.dry_run:
                verb = "SET" if args.action == ADD else "REMOVE"
                print(f"[DRY-RUN] {verb} image | itemId={item_id} SK={sk} {name[:48]}")
                updated += 1
                continue

            try:
                dynamodb.update_item(
                    TableName=table_name,
                    Key={"PK": {"S": pk}, "SK": {"S": sk}},
                    **update_kwargs,
                )
                updated += 1
                if updated % 25 == 0:
                    print(f"  ... {updated} updated")
                if args.sleep > 0:
                    time.sleep(args.sleep)
            except Exception as e:  # noqa: BLE001 - log and continue
                failed += 1
                print(f"FAILED itemId={item_id} PK={pk} SK={sk}: {e}")

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    print(
        "\nDone | "
        f"item_rows={scanned_items} matched={matched} "
        f"{'would_update' if args.dry_run else 'updated'}={updated} "
        f"skipped_unchanged={skipped_unchanged} failed={failed}"
    )
    if args.dry_run:
        print("Dry run — no writes. Re-run with --apply to commit.")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
