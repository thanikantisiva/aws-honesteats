#!/usr/bin/env python3
"""
POST menu items from a JSON file (e.g. makers_menu_bodies_RES-*.json) to the API.

Expected JSON shapes:
  - { "restaurantId": "RES-...", "items": [ { ... POST body ... }, ... ] }
  - [ { ... }, ... ]  (requires --restaurant-id)

Each object in `items` is sent as the JSON body to:
  POST {base}/api/v1/restaurants/{restaurantId}/menu

Auth (same as scripts/import_menu_from_xlsx.py):
  - Default: x-retool-header from HONESTEATS_RETOOL_BYPASS (prod: set real secret)
  - Or: Authorization: Bearer <JWT> (--bearer-token / HONESTEATS_BEARER_TOKEN)

Default is DRY-RUN (no HTTP). Pass --apply to insert.

Usage (prod):
  export HONESTEATS_RETOOL_BYPASS='<prod-secret>'   # or use --bearer-token
  python3 scripts/post_menu_from_json.py \\
    --json ~/Downloads/makers_menu_bodies_RES-1776965639587-4589.json \\
    --api-url https://api.yumdude.com \\
    --apply

  # Smoke test first 2 items only:
  python3 scripts/post_menu_from_json.py --json ... --apply --limit 2

Env:
  HONESTEATS_API_URL, HONESTEATS_RETOOL_BYPASS, HONESTEATS_BEARER_TOKEN, HONESTEATS_REQUEST_DELAY_SEC
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

RETOOL_BYPASS_HEADER = "x-retool-header"
DEFAULT_RETOOL_BYPASS = os.environ.get(
    "HONESTEATS_RETOOL_BYPASS",
    "9f2b7c4a6d1e8f30b5a9c2e7d4f1a6bc",
)
DEFAULT_API_URL = os.environ.get("HONESTEATS_API_URL", "https://api.yumdude.com")


def _normalize_api_base(url: str) -> str:
    u = url.rstrip("/")
    if u.endswith("/api/v1"):
        u = u[: -len("/api/v1")]
    return u


def build_session(args: argparse.Namespace) -> requests.Session:
    s = requests.Session()
    s.headers["Content-Type"] = "application/json"
    if args.no_retool_bypass:
        bypass = None
    elif (args.retool_bypass or "").strip():
        bypass = args.retool_bypass.strip()
    else:
        bypass = DEFAULT_RETOOL_BYPASS.strip() or None
    if bypass:
        s.headers[RETOOL_BYPASS_HEADER] = bypass
    token = (args.bearer_token or "").strip()
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
    return s


def load_items(path: Path) -> tuple[str | None, list[dict[str, Any]]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "items" in raw:
        rid = raw.get("restaurantId")
        items = raw["items"]
        if not isinstance(items, list):
            raise ValueError('"items" must be a JSON array')
        return (str(rid) if rid else None, [dict(x) for x in items if isinstance(x, dict)])
    if isinstance(raw, list):
        return (None, [dict(x) for x in raw if isinstance(x, dict)])
    raise ValueError("JSON must be {restaurantId, items} or a top-level array of objects")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="POST menu bodies from JSON to YumDude API")
    p.add_argument(
        "--json",
        type=Path,
        default=Path.home() / "Downloads" / "makers_menu_bodies_RES-1776965639587-4589.json",
        help="Path to JSON file",
    )
    p.add_argument(
        "--restaurant-id",
        default="",
        help="Override restaurant id (required if JSON is a bare array without restaurantId)",
    )
    p.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help="API base URL (default prod: https://api.yumdude.com)",
    )
    p.add_argument("--no-retool-bypass", action="store_true")
    p.add_argument("--retool-bypass", default="")
    p.add_argument(
        "--bearer-token",
        default=os.environ.get("HONESTEATS_BEARER_TOKEN", ""),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Parse and print summary only (default)",
    )
    p.add_argument("--apply", action="store_true", help="POST each item to the API")
    p.add_argument(
        "--delay-sec",
        type=float,
        default=float(os.environ.get("HONESTEATS_REQUEST_DELAY_SEC", "0.05")),
    )
    p.add_argument("--start", type=int, default=0, help="0-based index to start from")
    p.add_argument("--limit", type=int, default=0, help="Max items to POST (0 = all)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.apply:
        args.dry_run = False

    if not args.json.is_file():
        print(f"File not found: {args.json}", file=sys.stderr)
        return 1

    try:
        file_rid, items = load_items(args.json)
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Invalid JSON: {e}", file=sys.stderr)
        return 1

    restaurant_id = (args.restaurant_id or "").strip() or file_rid
    if not restaurant_id:
        print("restaurantId missing: set in JSON or pass --restaurant-id", file=sys.stderr)
        return 1

    start = max(0, args.start)
    end = len(items)
    if args.limit > 0:
        end = min(end, start + args.limit)
    slice_items = items[start:end]

    base = _normalize_api_base(args.api_url)
    menu_url = f"{base}/api/v1/restaurants/{restaurant_id}/menu"

    print(f"API:        POST {menu_url}")
    print(f"Restaurant: {restaurant_id}")
    print(f"Items:      {len(slice_items)} (from file total {len(items)}, start={start})")
    print(f"Mode:       {'DRY-RUN' if args.dry_run else 'APPLY'}")

    for i, body in enumerate(slice_items[:5], 1):
        name = body.get("name", "?")
        print(f"  {i}. {name!r}")
    if len(slice_items) > 5:
        print(f"  ... +{len(slice_items) - 5} more")

    if args.dry_run:
        print("\nNo HTTP calls. Pass --apply to POST to prod.")
        return 0

    session = build_session(args)
    has_bypass = RETOOL_BYPASS_HEADER in session.headers
    has_bearer = "Authorization" in session.headers
    if not has_bypass and not has_bearer:
        print(
            "Error: no auth. Set HONESTEATS_RETOOL_BYPASS, --retool-bypass, or --bearer-token.",
            file=sys.stderr,
        )
        return 1

    ok = err = 0
    for body in slice_items:
        name = body.get("name", "?")
        payload = dict(body)
        if args.delay_sec > 0:
            time.sleep(args.delay_sec)
        try:
            r = session.post(menu_url, json=payload, timeout=60)
        except requests.RequestException as ex:
            err += 1
            print(f"ERROR {name!r}: {ex}", file=sys.stderr)
            continue
        if r.status_code == 201:
            ok += 1
            if ok % 25 == 0:
                print(f"  ... {ok}/{len(slice_items)} created")
        else:
            err += 1
            print(f"ERROR {name!r}: HTTP {r.status_code} {r.text[:400]}", file=sys.stderr)
            if r.status_code == 401:
                print("\nUnauthorized — check prod bypass token or JWT.", file=sys.stderr)
                return 1

    print(f"\nDone: {ok} created, {err} failed.")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
