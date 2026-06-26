#!/usr/bin/env python3
"""
Migrate the per-restaurant coupon blocklist (blockedCouponsByRestaurant) out of the
global config row (CONFIG#GLOBAL) into the dedicated coupon row (CONFIG#COUPONS).

Uses the HTTP API (no AWS creds needed):
  GET  /api/v1/globalconfig          -> read the blocklist (incl. legacy key variants)
  POST /api/v1/coupon-blocks         -> write it to CONFIG#COUPONS
  POST /api/v1/globalconfig (--strip)-> rewrite global config without the blocklist keys

Target environment: --env dev|prod (default prod), or --api-url <host>, or
$HONESTEATS_API_URL. Precedence: --api-url > --env > env var > prod.

Usage:
  python3 scripts/migrate_coupon_blocks_config.py --env dev                 # dry run (dev)
  python3 scripts/migrate_coupon_blocks_config.py --env dev --apply         # copy into CONFIG#COUPONS
  python3 scripts/migrate_coupon_blocks_config.py --env prod --apply --strip # also remove from global
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import requests

# CouponService historically accepted these variants on the global config.
BLOCKED_KEYS = ("blockedCouponsByRestaurant", "couponBlocklistByRestaurant", "restaurantCouponBlocklist")

RETOOL_BYPASS_HEADER = "x-retool-header"
DEFAULT_RETOOL_BYPASS = os.environ.get("HONESTEATS_RETOOL_BYPASS", "9f2b7c4a6d1e8f30b5a9c2e7d4f1a6bc")
DEFAULT_API_URL = os.environ.get("HONESTEATS_API_URL", "https://api.yumdude.com")

ENV_API_URLS = {
    "prod": "https://api.yumdude.com",
    "dev": "https://api.dev.yumdude.com",
}


def _session(bearer: str | None) -> requests.Session:
    s = requests.Session()
    s.headers["Content-Type"] = "application/json"
    if bearer:
        s.headers["Authorization"] = f"Bearer {bearer}"
    else:
        s.headers[RETOOL_BYPASS_HEADER] = DEFAULT_RETOOL_BYPASS
    return s


def _extract_blocklist(global_config: dict) -> dict:
    """Return the {restaurantId: [codes]} map from whichever variant key exists."""
    for key in BLOCKED_KEYS:
        value = global_config.get(key)
        if isinstance(value, dict) and value:
            return value
    return {}


def main() -> int:
    ap = argparse.ArgumentParser(description="Migrate coupon blocklist to CONFIG#COUPONS")
    ap.add_argument("--apply", action="store_true", help="perform writes (default: dry run)")
    ap.add_argument("--strip", action="store_true", help="also remove the blocklist keys from CONFIG#GLOBAL")
    ap.add_argument("--env", choices=sorted(ENV_API_URLS),
                    help="target environment (prod/dev); shortcut for --api-url")
    ap.add_argument("--api-url", default=None, help="explicit API base URL (overrides --env)")
    ap.add_argument("--bearer-token", default=os.environ.get("HONESTEATS_BEARER_TOKEN"))
    args = ap.parse_args()

    api_url = args.api_url or (ENV_API_URLS[args.env] if args.env else DEFAULT_API_URL)
    base = api_url.rstrip("/")
    print(f"Target: {base}\n")
    s = _session(args.bearer_token)

    r = s.get(f"{base}/api/v1/globalconfig", timeout=60)
    if r.status_code != 200:
        print(f"✗ GET /globalconfig failed: HTTP {r.status_code} {r.text[:200]}", file=sys.stderr)
        return 1
    global_config = (r.json() or {}).get("config") or {}

    blocklist = _extract_blocklist(global_config)
    if not blocklist:
        print("Nothing to migrate — no blockedCouponsByRestaurant in global config.")
        return 0

    print("Coupon blocklist found in global row:")
    print(json.dumps(blocklist, indent=2, ensure_ascii=False))
    print(f"\nPlan:\n  → POST {base}/api/v1/coupon-blocks    (write the blocklist)")
    if args.strip:
        print(f"  → POST {base}/api/v1/globalconfig     (rewrite global WITHOUT the blocklist keys)")

    if not args.apply:
        print("\nDRY RUN — no writes. Re-run with --apply (add --strip to also clean global).")
        return 0

    w = s.post(f"{base}/api/v1/coupon-blocks", json={"blockedCouponsByRestaurant": blocklist}, timeout=60)
    if w.status_code != 200:
        print(f"✗ POST /coupon-blocks failed: HTTP {w.status_code} {w.text[:200]}", file=sys.stderr)
        return 1
    print("✓ Wrote CONFIG#COUPONS (blockedCouponsByRestaurant)")

    if args.strip:
        cleaned = {k: v for k, v in global_config.items() if k not in BLOCKED_KEYS}
        g = s.post(f"{base}/api/v1/globalconfig", json=cleaned, timeout=60)
        if g.status_code not in (200, 201):
            print(f"✗ POST /globalconfig (strip) failed: HTTP {g.status_code} {g.text[:200]}", file=sys.stderr)
            return 1
        print("✓ Removed the blocklist keys from CONFIG#GLOBAL")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
