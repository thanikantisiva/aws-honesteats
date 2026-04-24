#!/usr/bin/env python3
"""
Rewrite menu item `image` URL query strings to a single lightweight param: ?w=400.

Unsplash and similar URLs often use long query strings (e.g. ?auto=format&fit=crop&w=800&q=80)
which can slow loading. This script replaces any non-trivial query on each image URL with ?w=400
only (default width overridable via --width).

Credentials (same as AWS CLI / boto3):
  - Environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, optional AWS_SESSION_TOKEN,
    AWS_DEFAULT_REGION or AWS_REGION
  - Or: --profile NAME
  - Or: --access-key-id and --secret-access-key (optional --session-token)
  - Or: --region only, using the default credential chain

Usage:
  export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_DEFAULT_REGION=ap-south-1
  python3 scripts/simplify_menu_item_image_urls.py --env prod --dry-run

  python3 scripts/simplify_menu_item_image_urls.py --table food-delivery-menu-items-prod

  python3 scripts/simplify_menu_item_image_urls.py --env dev --profile myprofile --dry-run
"""

from __future__ import annotations

import argparse
import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import boto3


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Normalize menu item image URLs to ?w=<width> only")
    p.add_argument("--env", default="dev", help="Table suffix when --table omitted (default: dev)")
    p.add_argument("--table", help="Full DynamoDB table name (overrides --env)")
    p.add_argument("--width", type=int, default=400, help="Query value for w= (default: 400)")
    p.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    p.add_argument("--region", help="AWS region (else env / profile default)")
    p.add_argument("--profile", help="Named AWS profile (~/.aws/credentials)")
    p.add_argument("--access-key-id", dest="access_key_id", help="AWS access key (optional explicit creds)")
    p.add_argument("--secret-access-key", dest="secret_access_key", help="AWS secret key")
    p.add_argument(
        "--session-token",
        dest="session_token",
        help="AWS session token (temporary credentials)",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0,
        help="Seconds to sleep after each successful update (throttle; default: 0)",
    )
    return p.parse_args()


def _boto_session(args: argparse.Namespace) -> boto3.session.Session:
    sk: dict[str, Any] = {}
    if args.region:
        sk["region_name"] = args.region
    if args.access_key_id and args.secret_access_key:
        sk["aws_access_key_id"] = args.access_key_id
        sk["aws_secret_access_key"] = args.secret_access_key
        if args.session_token:
            sk["aws_session_token"] = args.session_token
    elif args.profile:
        sk["profile_name"] = args.profile
    return boto3.Session(**sk)


def _query_is_only_w(url: str, width: int) -> bool:
    parsed = urlparse(url)
    if not parsed.query:
        return False
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    return pairs == [("w", str(width))]


def simplify_image_url(url: str, width: int) -> str:
    """Strip existing query/fragment; set query to w=<width> only."""
    if not url or not isinstance(url, str):
        return url
    u = url.strip()
    if not u:
        return url
    if _query_is_only_w(u, width):
        return u
    parsed = urlparse(u)
    new_query = urlencode([("w", str(width))])
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", new_query, ""))


def _dynamo_image_strings(image_attr: dict[str, Any] | None) -> list[str] | None:
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


def _build_image_attr(urls: list[str]) -> dict[str, Any]:
    return {"L": [{"S": u} for u in urls]}


def main() -> None:
    args = parse_args()
    table_name = args.table or f"food-delivery-menu-items-{args.env}"
    width = int(args.width)
    if width <= 0:
        raise SystemExit("--width must be positive")
    if (args.access_key_id or args.secret_access_key) and not (
        args.access_key_id and args.secret_access_key
    ):
        raise SystemExit("Both --access-key-id and --secret-access-key are required for explicit keys")

    session = _boto_session(args)
    dynamodb = session.client("dynamodb")

    scanned = 0
    updated = 0
    skipped_no_image = 0
    skipped_unchanged = 0
    failed = 0
    last_evaluated_key = None

    print(f"Table:    {table_name}")
    print(f"Target:   ?w={width} only")
    print(f"Dry run:  {args.dry_run}")
    print(f"Region:   {session.region_name or '(default chain)'}")

    while True:
        scan_kwargs: dict[str, Any] = {
            "TableName": table_name,
            "ProjectionExpression": "PK, SK, itemId, image",
            "FilterExpression": "attribute_exists(image)",
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
            image_attr = item.get("image")

            if not pk or not sk:
                skipped_no_image += 1
                continue

            old_urls = _dynamo_image_strings(image_attr)
            if old_urls is None:
                skipped_no_image += 1
                continue

            new_urls = [simplify_image_url(u, width) for u in old_urls]
            if new_urls == old_urls:
                skipped_unchanged += 1
                continue

            if args.dry_run:
                print(f"[DRY-RUN] itemId={item_id} PK={pk} SK={sk}")
                for a, b in zip(old_urls, new_urls):
                    if a != b:
                        print(f"  - {a}")
                        print(f"  + {b}")
                updated += 1
                continue

            try:
                dynamodb.update_item(
                    TableName=table_name,
                    Key={"PK": {"S": pk}, "SK": {"S": sk}},
                    UpdateExpression="SET #img = :img",
                    ExpressionAttributeNames={"#img": "image"},
                    ExpressionAttributeValues={":img": _build_image_attr(new_urls)},
                )
                updated += 1
                if args.sleep > 0:
                    time.sleep(args.sleep)
            except Exception as e:
                failed += 1
                print(f"Failed itemId={item_id} PK={pk} SK={sk}: {e}")

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    print(
        "Done | "
        f"scanned={scanned} updated={updated} "
        f"skipped_unchanged={skipped_unchanged} skipped_no_usable_image={skipped_no_image} "
        f"failed={failed}"
    )


if __name__ == "__main__":
    main()
