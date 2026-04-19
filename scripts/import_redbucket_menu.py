#!/usr/bin/env python3
"""
Import Red Bucket menu items from Excel via HTTP API.

POST {base}/api/v1/restaurants/{restaurantId}/menu

Reads: /Users/user/Downloads/RedBucket_Price_Comparison calculations.xlsx
  Col B: Item name
  Col C: Dine-In (₹) → restaurantPrice
  Col D: Our Hike on Dine-in % → hikePercentage
  Col E: Description
  Col F: Category
  Col G: Subcategory
  Col H: Veg or Non-Veg → isVeg
  Col I: Image URL

Usage:
  # Dry run (just prints payloads, no HTTP calls):
  python3 scripts/import_redbucket_menu.py

  # Actually insert:
  python3 scripts/import_redbucket_menu.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import openpyxl
import requests

RESTAURANT_ID = "RES-1776340103345-1527"
XLSX_PATH = Path("/Users/user/Downloads/RedBucket_Price_Comparison calculations.xlsx")
SHEET_NAME = "Price Comparison"

RETOOL_BYPASS_HEADER = "x-retool-header"
DEFAULT_RETOOL_BYPASS = os.environ.get(
    "HONESTEATS_RETOOL_BYPASS",
    "9f2b7c4a6d1e8f30b5a9c2e7d4f1a6bc",
)
DEFAULT_API_URL = os.environ.get("HONESTEATS_API_URL", "https://api.yumdude.com")


def parse_veg(value) -> bool | None:
    if value is None:
        return None
    v = str(value).strip().lower()
    if v in ("veg", "yes", "true", "1"):
        return True
    if v in ("non-veg", "nonveg", "non veg", "no", "false", "0"):
        return False
    return None


def build_payloads() -> list[dict]:
    wb = openpyxl.load_workbook(XLSX_PATH, read_only=True, data_only=True)
    ws = wb[SHEET_NAME]

    payloads: list[dict] = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        name = row[1]  # B
        price = row[2]  # C
        hike = row[3]  # D
        desc = row[4]  # E
        category = row[5]  # F
        subcategory = row[6]  # G
        veg_raw = row[7]  # H
        image_url = row[8]  # I

        if not name or price is None:
            continue

        payload: dict = {
            "name": str(name).strip(),
            "restaurantPrice": float(price),
            "hikePercentage": float(hike) if hike is not None else 0,
            "category": str(category).strip() if category else None,
            "subCategory": str(subcategory).strip() if subcategory else None,
            "isVeg": parse_veg(veg_raw),
            "isAvailable": True,
            "description": str(desc).strip() if desc else None,
            "image": [str(image_url).strip()] if image_url else [],
        }
        payloads.append(payload)

    wb.close()
    return payloads


def build_session(api_url: str, bearer_token: str | None = None) -> requests.Session:
    s = requests.Session()
    s.headers["Content-Type"] = "application/json"
    if bearer_token:
        s.headers["Authorization"] = f"Bearer {bearer_token}"
    else:
        s.headers[RETOOL_BYPASS_HEADER] = DEFAULT_RETOOL_BYPASS
    return s


def main() -> int:
    parser = argparse.ArgumentParser(description="Import Red Bucket menu from Excel")
    parser.add_argument("--apply", action="store_true", help="Actually POST to API (default is dry run)")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="API base URL")
    parser.add_argument("--bearer-token", default=os.environ.get("HONESTEATS_BEARER_TOKEN"), help="JWT token")
    parser.add_argument("--delay", type=float, default=0.3, help="Delay between requests (seconds)")
    args = parser.parse_args()

    payloads = build_payloads()
    print(f"Loaded {len(payloads)} menu items from {XLSX_PATH.name}")
    print(f"Restaurant ID: {RESTAURANT_ID}\n")

    menu_url = f"{args.api_url.rstrip('/')}/api/v1/restaurants/{RESTAURANT_ID}/menu"

    # Always print all payloads
    for i, p in enumerate(payloads, 1):
        print(f"[{i:3d}] {p['name']}")
        print(f"      ₹{p['restaurantPrice']} + {p['hikePercentage']}% hike | {p['category']} > {p['subCategory']}")
        print(f"      veg={p['isVeg']} | img={'yes' if p['image'] else 'no'}")
        if not args.apply:
            print(f"      payload: {json.dumps(p, indent=None, ensure_ascii=False)}")
        print()

    if not args.apply:
        print("=" * 60)
        print("DRY RUN — no HTTP calls made.")
        print(f"To insert, run:  python3 {sys.argv[0]} --apply")
        print("=" * 60)
        return 0

    # Insert via API
    session = build_session(args.api_url, args.bearer_token)
    ok = err = 0

    for i, p in enumerate(payloads, 1):
        if args.delay > 0:
            time.sleep(args.delay)
        try:
            r = session.post(menu_url, json=p, timeout=60)
        except requests.RequestException as ex:
            err += 1
            print(f"ERROR [{i}] {p['name']}: {ex}", file=sys.stderr)
            continue

        if r.status_code == 201:
            ok += 1
            print(f"  ✓ [{i}] {p['name']} created")
        else:
            err += 1
            print(f"  ✗ [{i}] {p['name']}: HTTP {r.status_code} {r.text[:200]}", file=sys.stderr)
            if r.status_code == 401:
                print("\nUnauthorized. Check retool bypass or bearer token.", file=sys.stderr)
                return 1

    print(f"\nDone: {ok} created, {err} failed.")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
