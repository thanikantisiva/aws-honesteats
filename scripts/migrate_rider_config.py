#!/usr/bin/env python3
"""
Migrate rider config (riderSlots / riderBonusConfig / riderSlotsSettings) out of
the global config row (CONFIG#GLOBAL) into the dedicated row (CONFIG#RIDER).

Uses the HTTP API (no AWS creds needed):
  GET  /api/v1/globalconfig          -> read the 3 rider keys
  POST /api/v1/rider-config          -> write them to the dedicated row
  POST /api/v1/globalconfig (--strip)-> rewrite global config without the 3 keys

Target environment: --env dev|prod (default prod), or --api-url <host>, or
$HONESTEATS_API_URL. Precedence: --api-url > --env > env var > prod.

Usage:
  python3 scripts/migrate_rider_config.py --env dev                 # dry run (dev)
  python3 scripts/migrate_rider_config.py --env dev --apply         # copy into CONFIG#RIDER
  python3 scripts/migrate_rider_config.py --env prod --apply --strip # also remove from global
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import requests

KEYS = ("riderSlots", "riderBonusConfig", "riderSlotsSettings")

RETOOL_BYPASS_HEADER = "x-retool-header"
DEFAULT_RETOOL_BYPASS = os.environ.get("HONESTEATS_RETOOL_BYPASS", "9f2b7c4a6d1e8f30b5a9c2e7d4f1a6bc")
DEFAULT_API_URL = os.environ.get("HONESTEATS_API_URL", "https://api.yumdude.com")

# Known environments (host only — the script appends /api/v1/... paths).
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


def main() -> int:
    ap = argparse.ArgumentParser(description="Migrate rider config to its own row")
    ap.add_argument("--apply", action="store_true", help="perform writes (default: dry run)")
    ap.add_argument("--strip", action="store_true", help="also remove the keys from CONFIG#GLOBAL")
    ap.add_argument("--env", choices=sorted(ENV_API_URLS),
                    help="target environment (prod/dev); shortcut for --api-url")
    ap.add_argument("--api-url", default=None, help="explicit API base URL (overrides --env)")
    ap.add_argument("--bearer-token", default=os.environ.get("HONESTEATS_BEARER_TOKEN"))
    args = ap.parse_args()

    # Precedence: --api-url > --env > $HONESTEATS_API_URL > prod default.
    api_url = args.api_url or (ENV_API_URLS[args.env] if args.env else DEFAULT_API_URL)
    base = api_url.rstrip("/")
    print(f"Target: {base}\n")
    s = _session(args.bearer_token)

    # 1) Read the global config row.
    r = s.get(f"{base}/api/v1/globalconfig", timeout=60)
    if r.status_code != 200:
        print(f"✗ GET /globalconfig failed: HTTP {r.status_code} {r.text[:200]}", file=sys.stderr)
        return 1
    global_config = (r.json() or {}).get("config") or {}

    rider_config = {k: global_config[k] for k in KEYS if k in global_config}
    if not rider_config:
        print("Nothing to migrate — no riderSlots/riderBonusConfig/riderSlotsSettings in global config.")
        return 0

    print("Rider config found in global row:")
    print(json.dumps(rider_config, indent=2, ensure_ascii=False))
    print(f"\nPlan:\n  → POST {base}/api/v1/rider-config     (write the rider keys)")
    if args.strip:
        print(f"  → POST {base}/api/v1/globalconfig     (rewrite global WITHOUT the rider keys)")

    if not args.apply:
        print("\nDRY RUN — no writes. Re-run with --apply (add --strip to also clean global).")
        return 0

    # 2) Write the dedicated row.
    w = s.post(f"{base}/api/v1/rider-config", json=rider_config, timeout=60)
    if w.status_code != 200:
        print(f"✗ POST /rider-config failed: HTTP {w.status_code} {w.text[:200]}", file=sys.stderr)
        return 1
    print("✓ Wrote CONFIG#RIDER")

    # 3) Optionally strip the keys from the global row.
    if args.strip:
        cleaned = {k: v for k, v in global_config.items() if k not in KEYS}
        g = s.post(f"{base}/api/v1/globalconfig", json=cleaned, timeout=60)
        if g.status_code not in (200, 201):
            print(f"✗ POST /globalconfig (strip) failed: HTTP {g.status_code} {g.text[:200]}", file=sys.stderr)
            return 1
        print("✓ Removed the rider keys from CONFIG#GLOBAL")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
