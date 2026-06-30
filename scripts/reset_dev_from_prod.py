#!/usr/bin/env python3
"""
Reset DEV restaurants / menu-items tables from PROD: WIPE the dev table(s) then
COPY every row from the matching prod table(s).

Tables handled (DynamoDB, region ap-south-1):
  restaurants  -> food-delivery-restaurants-{env}   (PK + SK composite, 4 GSIs auto-populate on write)
  menu-items   -> food-delivery-menu-items-{env}     (PK + SK composite, no GSIs, has a stream + ttl)

Items are copied VERBATIM (full attribute maps), so GSI key attributes
(restaurantId, GSI1PK/SK, GSI2PK/SK, GSI3PK/SK on restaurants) are preserved and
the indexes stay queryable. For menu-items this also copies COUNTER#... rows
(pickup-token counters) — the full table is mirrored. Writing to dev menu-items
fires the dev menu-items DynamoDB stream handler.

dev and prod may be the same AWS account (different table suffixes) OR separate
accounts. This script supports both: pass --src-profile / --dst-profile, or leave
them off to use the default credential chain for both.

SAFETY:
  * Dry-run by DEFAULT. Nothing is written until you pass --apply.
  * Refuses to use a DESTINATION table whose name contains "prod" (guards against
    wiping prod). Override only with --allow-nondev-dest if you really mean it.
  * Refuses when a source and destination table resolve to the same name+creds.
  * On --apply (without --yes) it asks you to type the destination env to confirm.

Requires: dynamodb:DescribeTable, dynamodb:Scan on source+dest, and
dynamodb:BatchWriteItem on dest.

Run with python3.11 / python3.12 (system python3 3.8 has a broken boto3).

Examples:
  # Preview (no writes): clear dev, copy prod -> dev, both tables, one account
  python3.11 scripts/reset_dev_from_prod.py

  # Apply, same account (default creds read prod and write dev):
  python3.11 scripts/reset_dev_from_prod.py --apply

  # Separate accounts (prod creds read, dev creds write):
  python3.11 scripts/reset_dev_from_prod.py \
      --src-profile awsprod --dst-profile awsdev --apply

  # Only menu-items, skip the confirmation prompt:
  python3.11 scripts/reset_dev_from_prod.py --tables menu-items --apply --yes
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any, Iterator

import boto3

# logical table -> base name (env suffix appended)
TABLE_BASES = {
    "restaurants": "food-delivery-restaurants",
    "menu-items": "food-delivery-menu-items",
}
DEFAULT_REGION = "ap-south-1"
BATCH_MAX = 25  # DynamoDB BatchWriteItem hard limit


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Wipe dev restaurants/menu-items table(s) and copy them from prod",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--tables",
        default="restaurants,menu-items",
        help="Comma list of tables: restaurants, menu-items, or all (default: both)",
    )
    p.add_argument("--src-env", default="prod", help="Source env / table suffix (default: prod)")
    p.add_argument("--dst-env", default="dev", help="Destination env / table suffix (default: dev)")
    p.add_argument("--src-profile", help="AWS profile for the SOURCE (prod) account (default chain if omitted)")
    p.add_argument("--dst-profile", help="AWS profile for the DESTINATION (dev) account (default chain if omitted)")
    p.add_argument("--src-region", default=DEFAULT_REGION, help=f"Source region (default: {DEFAULT_REGION})")
    p.add_argument("--dst-region", default=DEFAULT_REGION, help=f"Destination region (default: {DEFAULT_REGION})")
    p.add_argument(
        "--mode",
        choices=["reset", "clear-only", "import-only"],
        default="reset",
        help="reset = clear dst then copy from src (default); clear-only = just wipe dst; import-only = just copy",
    )
    p.add_argument("--dry-run", action="store_true", default=True, help="Preview counts only, no writes (default)")
    p.add_argument("--apply", action="store_true", help="Actually wipe and write")
    p.add_argument("--yes", action="store_true", help="Skip the interactive confirmation on --apply")
    p.add_argument(
        "--allow-nondev-dest",
        action="store_true",
        help="Permit a destination table whose name contains 'prod' (DANGEROUS; off by default)",
    )
    p.add_argument("--throttle", type=float, default=0.0, help="Seconds to sleep between batches (default: 0)")
    return p.parse_args()


def _session(profile: str | None, region: str) -> boto3.session.Session:
    kw: dict[str, Any] = {"region_name": region}
    if profile:
        kw["profile_name"] = profile
    return boto3.Session(**kw)


def _table_name(logical: str, env: str) -> str:
    return f"{TABLE_BASES[logical]}-{env}"


def _key_attr_names(client: Any, table: str) -> list[str]:
    """Read the table's key schema (HASH/RANGE attribute names) via DescribeTable."""
    desc = client.describe_table(TableName=table)["Table"]
    return [k["AttributeName"] for k in desc["KeySchema"]]


def _count_rows(client: Any, table: str) -> int:
    total = 0
    eks = None
    while True:
        kw: dict[str, Any] = {"TableName": table, "Select": "COUNT"}
        if eks:
            kw["ExclusiveStartKey"] = eks
        resp = client.scan(**kw)
        total += resp.get("Count", 0)
        eks = resp.get("LastEvaluatedKey")
        if not eks:
            break
    return total


def _scan_items(client: Any, table: str, key_attrs: list[str] | None = None) -> Iterator[dict]:
    """Yield rows. If key_attrs given, project only those (for delete); else full items."""
    kw: dict[str, Any] = {"TableName": table}
    if key_attrs:
        names = {f"#k{i}": a for i, a in enumerate(key_attrs)}
        kw["ProjectionExpression"] = ", ".join(names.keys())
        kw["ExpressionAttributeNames"] = names
    eks = None
    while True:
        if eks:
            kw["ExclusiveStartKey"] = eks
        resp = client.scan(**kw)
        for item in resp.get("Items", []):
            yield item
        eks = resp.get("LastEvaluatedKey")
        if not eks:
            break


class BatchWriter:
    """Buffered BatchWriteItem with UnprocessedItems retry. Handles Put and Delete."""

    def __init__(self, client: Any, table: str, throttle: float = 0.0):
        self.client = client
        self.table = table
        self.throttle = throttle
        self.buf: list[dict] = []
        self.written = 0

    def put(self, item: dict) -> None:
        self._add({"PutRequest": {"Item": item}})

    def delete(self, key: dict) -> None:
        self._add({"DeleteRequest": {"Key": key}})

    def _add(self, request: dict) -> None:
        self.buf.append(request)
        if len(self.buf) >= BATCH_MAX:
            self._flush()

    def _flush(self) -> None:
        if not self.buf:
            return
        self._send(self.buf)
        self.written += len(self.buf)
        self.buf = []
        if self.throttle:
            time.sleep(self.throttle)

    def _send(self, chunk: list[dict]) -> None:
        request_items = {self.table: chunk}
        attempt = 0
        while request_items:
            resp = self.client.batch_write_item(RequestItems=request_items)
            request_items = resp.get("UnprocessedItems") or {}
            if request_items:
                attempt += 1
                if attempt > 12:
                    raise RuntimeError(
                        f"BatchWriteItem still has unprocessed items after {attempt} retries on {self.table}"
                    )
                time.sleep(min(0.05 * (2 ** attempt), 5.0))

    def close(self) -> None:
        self._flush()


def clear_table(client: Any, table: str, throttle: float) -> int:
    key_attrs = _key_attr_names(client, table)
    writer = BatchWriter(client, table, throttle)
    for row in _scan_items(client, table, key_attrs=key_attrs):
        writer.delete({k: row[k] for k in key_attrs})
        if writer.written and writer.written % 500 == 0:
            print(f"    deleted ~{writer.written} ...")
    writer.close()
    return writer.written


def copy_table(src_client: Any, dst_client: Any, src_table: str, dst_table: str, throttle: float) -> int:
    writer = BatchWriter(dst_client, dst_table, throttle)
    for item in _scan_items(src_client, src_table):
        writer.put(item)
        if writer.written and writer.written % 500 == 0:
            print(f"    copied ~{writer.written} ...")
    writer.close()
    return writer.written


def main() -> int:
    args = parse_args()
    if args.apply:
        args.dry_run = False

    logical = [t.strip() for t in args.tables.split(",") if t.strip()]
    if "all" in logical:
        logical = list(TABLE_BASES.keys())
    bad = [t for t in logical if t not in TABLE_BASES]
    if bad:
        print(f"Unknown table(s): {bad}. Valid: {list(TABLE_BASES.keys())} or 'all'", file=sys.stderr)
        return 2

    src_sess = _session(args.src_profile, args.src_region)
    dst_sess = _session(args.dst_profile, args.dst_region)
    src_client = src_sess.client("dynamodb")
    dst_client = dst_sess.client("dynamodb")

    plan = []
    for t in logical:
        src_table = _table_name(t, args.src_env)
        dst_table = _table_name(t, args.dst_env)

        # ---- safety guards ----
        if "prod" in dst_table.lower() and not args.allow_nondev_dest:
            print(
                f"REFUSING: destination '{dst_table}' looks like prod. "
                f"This script wipes the destination. Use --allow-nondev-dest to override.",
                file=sys.stderr,
            )
            return 2
        same_creds = (args.src_profile or "") == (args.dst_profile or "") and args.src_region == args.dst_region
        if src_table == dst_table and same_creds:
            print(
                f"REFUSING: source and destination are the same table+account ({src_table}). "
                f"Check --src-env/--dst-env.",
                file=sys.stderr,
            )
            return 2
        plan.append((t, src_table, dst_table))

    # ---- summary ----
    print("=" * 64)
    print(f"Mode:        {args.mode}")
    print(f"Write mode:  {'DRY-RUN (no writes)' if args.dry_run else 'APPLY'}")
    print(f"Source:      env={args.src_env} profile={args.src_profile or '(default)'} region={args.src_region}")
    print(f"Destination: env={args.dst_env} profile={args.dst_profile or '(default)'} region={args.dst_region}")
    for t, s, d in plan:
        if args.mode == "clear-only":
            print(f"  - {t}: WIPE {d}")
        elif args.mode == "import-only":
            print(f"  - {t}: COPY {s}  ->  {d}")
        else:
            print(f"  - {t}: WIPE {d}, then COPY {s}  ->  {d}")
    print("=" * 64)

    # ---- dry run: just count ----
    if args.dry_run:
        for t, s, d in plan:
            try:
                if args.mode != "import-only":
                    n_del = _count_rows(dst_client, d)
                    print(f"[{t}] would DELETE {n_del} row(s) from {d}")
                if args.mode != "clear-only":
                    n_src = _count_rows(src_client, s)
                    print(f"[{t}] would COPY   {n_src} row(s) from {s} -> {d}")
            except Exception as e:  # noqa: BLE001
                print(f"[{t}] count failed: {e}", file=sys.stderr)
        print("\nDry run — no writes. Re-run with --apply to execute.")
        return 0

    # ---- confirmation ----
    if not args.yes:
        print(
            f"\nThis will IRREVERSIBLY modify destination env '{args.dst_env}'."
            f"\nType the destination env name to proceed: ",
            end="",
        )
        try:
            answer = input().strip()
        except EOFError:
            answer = ""
        if answer != args.dst_env:
            print("Aborted (confirmation did not match).", file=sys.stderr)
            return 1

    # ---- apply ----
    failures = 0
    for t, s, d in plan:
        print(f"\n[{t}] {s} -> {d}")
        try:
            if args.mode != "import-only":
                print(f"  clearing {d} ...")
                deleted = clear_table(dst_client, d, args.throttle)
                print(f"  deleted {deleted} row(s)")
            if args.mode != "clear-only":
                print(f"  copying {s} -> {d} ...")
                copied = copy_table(src_client, dst_client, s, d, args.throttle)
                print(f"  copied {copied} row(s)")
        except Exception as e:  # noqa: BLE001
            failures += 1
            print(f"  FAILED [{t}]: {e}", file=sys.stderr)

    print(f"\nDone. {len(plan) - failures}/{len(plan)} table(s) ok, {failures} failed.")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
