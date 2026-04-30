#!/usr/bin/env python3
"""
Insert Vinayaka menu items from enriched JSON into production (or any API base).

POST {base}/api/v1/restaurants/{restaurantId}/menu

Default JSON: ~/Downloads/Vinayaka_menu_payloads.json (array of bodies from enrich_vinayaka_menu.py)

Usage:
  python3 scripts/insert_vinayaka_menu.py              # dry-run
  python3 scripts/insert_vinayaka_menu.py --apply    # POST for real

Env:
  HONESTEATS_API_URL          — default https://api.yumdude.com
  HONESTEATS_RETOOL_BYPASS    — x-retool-header
  HONESTEATS_BEARER_TOKEN     — optional JWT
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))
from menu_item_images import resolve_menu_item_image

RESTAURANT_ID = "RES-1777199457630-7702"
RETOOL_BYPASS_HEADER = "x-retool-header"
DEFAULT_RETOOL_BYPASS = os.environ.get(
    "HONESTEATS_RETOOL_BYPASS",
    "9f2b7c4a6d1e8f30b5a9c2e7d4f1a6bc",
)
DEFAULT_API_URL = os.environ.get("HONESTEATS_API_URL", "https://api.yumdude.com")
JSON_PATH = Path.home() / "Downloads" / "Vinayaka_menu_payloads.json"


def _normalize_api_base(url: str) -> str:
    u = url.rstrip("/")
    if u.endswith("/api/v1"):
        u = u[: -len("/api/v1")]
    return u


def load_items(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "items" in data:
        return data["items"]
    raise ValueError(f"Expected JSON array or {{\"items\": [...]}}, got {type(data)}")


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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--json",
        type=Path,
        default=JSON_PATH,
        help="Path to Vinayaka_menu_payloads.json",
    )
    p.add_argument(
        "--restaurant-id",
        default=RESTAURANT_ID,
        help="Restaurant ID in URL (default Vinayaka RES id)",
    )
    p.add_argument("--api-url", default=DEFAULT_API_URL, help="API base URL")
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
        help="Parse and print only (default)",
    )
    p.add_argument("--apply", action="store_true", help="POST each item to the API")
    p.add_argument(
        "--delay-sec",
        type=float,
        default=float(os.environ.get("HONESTEATS_REQUEST_DELAY_SEC", "0.05")),
    )
    p.add_argument("--no-images", action="store_true", help="Skip stock image resolution")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.apply:
        args.dry_run = False

    if not args.json.is_file():
        print(f"JSON not found: {args.json}", file=sys.stderr)
        return 1

    items = load_items(args.json)
    print(f"Loaded {len(items)} items from {args.json}")

    base = _normalize_api_base(args.api_url)
    menu_url = f"{base}/api/v1/restaurants/{args.restaurant_id}/menu"

    print(f"API:        POST {menu_url}")
    print(f"Restaurant: {args.restaurant_id}")
    print(f"Items:      {len(items)}")
    print(f"Mode:       {'DRY-RUN' if args.dry_run else 'APPLY'}")
    print()

    veg = sum(1 for it in items if it.get("isVeg") is True)
    nv = sum(1 for it in items if it.get("isVeg") is False)
    print(f"Veg: {veg}  |  Non-Veg: {nv}  |  Unknown: {len(items) - veg - nv}")

    for i, it in enumerate(items[:5], 1):
        img = ""
        if not args.no_images:
            resolved = resolve_menu_item_image(
                it["name"], it["category"], it.get("subCategory", "") or "", None
            )
            img = f" | img={resolved[:56]}…"
        print(
            f"  {i}. {it['name']} | {it['category']} / {it.get('subCategory', '')} "
            f"| ₹{it['restaurantPrice']} +{it['hikePercentage']}% "
            f"| {'V' if it.get('isVeg') else 'NV'}{img}"
        )
    if len(items) > 5:
        print(f"  ... +{len(items) - 5} more")

    if args.dry_run:
        print("\nDry run — no HTTP calls. Pass --apply to insert via API.")
        return 0

    session = build_session(args)
    has_bypass = RETOOL_BYPASS_HEADER in session.headers
    has_bearer = "Authorization" in session.headers
    if not has_bypass and not has_bearer:
        print(
            "Error: no auth configured. Set HONESTEATS_RETOOL_BYPASS or pass --bearer-token.",
            file=sys.stderr,
        )
        return 1

    ok = err = 0
    for it in items:
        payload = {
            "name": it["name"],
            "restaurantPrice": it["restaurantPrice"],
            "hikePercentage": it["hikePercentage"],
            "category": it["category"],
            "subCategory": it.get("subCategory") or None,
            "isVeg": it.get("isVeg"),
            "isAvailable": it.get("isAvailable", True),
            "description": it.get("description") or None,
        }
        if not args.no_images:
            payload["image"] = resolve_menu_item_image(
                it["name"], it["category"], it.get("subCategory", "") or "", None
            )

        if args.delay_sec > 0:
            time.sleep(args.delay_sec)

        try:
            r = session.post(menu_url, json=payload, timeout=60)
        except requests.RequestException as ex:
            err += 1
            print(f"ERROR {it['name']!r}: {ex}", file=sys.stderr)
            continue

        if r.status_code == 201:
            ok += 1
            if ok % 25 == 0:
                print(f"  ... {ok}/{len(items)} created")
        else:
            err += 1
            print(f"ERROR {it['name']!r}: HTTP {r.status_code} {r.text[:300]}", file=sys.stderr)
            if r.status_code == 401:
                print("\nUnauthorized — check auth config.", file=sys.stderr)
                return 1

    print(f"\nDone: {ok} created, {err} failed.")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
