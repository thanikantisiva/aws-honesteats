#!/usr/bin/env python3
"""
Read Vinayaka.xlsx (columns A–E: Category, Subcategory, Item, Price, Hike),
enrich category / subcategory / item text (trim, collapse spaces, title-style caps),
emit JSON bodies for menu items. Does NOT call any API.

Default paths can be overridden with --input / --output.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

RESTAURANT_ID_DEFAULT = "RES-1777199457630-7702"


def _num(v: Any) -> float | int:
    """Parse numeric cell; use int when value is mathematically whole."""
    if v is None or (isinstance(v, str) and not str(v).strip()):
        return 0
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        if isinstance(v, float) and v == int(v):
            return int(v)
        return int(v) if isinstance(v, int) else float(v)
    s = str(v).strip().replace(",", "")
    try:
        x = float(s)
        return int(x) if x == int(x) else x
    except ValueError:
        return 0


def enrich_label(value: Any) -> str:
    """Trim, normalize whitespace, then capitalize each word start (after space, /, -, or opening bracket)."""
    if value is None:
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        s = str(value).strip()
        if s.endswith(".0") and s[:-2].replace("-", "").isdigit():
            s = s[:-2]
        return s
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    s = " ".join(s.split()).lower()
    # Letters that begin a "word" (start, or after whitespace / ( [ - /)
    return re.sub(r"(^|[\s(/\-])([a-zà-ÿ])", lambda m: m.group(1) + m.group(2).upper(), s)


def row_to_body(
    row: tuple[Any, ...],
    restaurant_id: str,
) -> dict[str, Any]:
    cat, sub, name, price, hike = (row + (None,) * 5)[:5]
    return {
        "restaurantId": restaurant_id,
        "name": enrich_label(name),
        "restaurantPrice": _num(price),
        "hikePercentage": _num(hike),
        "category": enrich_label(cat),
        "subCategory": enrich_label(sub),
        "isVeg": True,
        "isAvailable": True,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--input",
        type=Path,
        default=Path("/Users/user/Downloads/Vinayaka.xlsx"),
        help="Path to Vinayaka.xlsx",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=Path("/Users/user/Downloads/Vinayaka_menu_payloads.json"),
        help="Where to write JSON array of bodies",
    )
    p.add_argument(
        "--restaurant-id",
        default=RESTAURANT_ID_DEFAULT,
        help="Restaurant id on each payload",
    )
    args = p.parse_args()

    try:
        import openpyxl
    except ImportError:
        print("Install openpyxl: pip install openpyxl", file=sys.stderr)
        return 1

    if not args.input.is_file():
        print(f"Input not found: {args.input}", file=sys.stderr)
        return 1

    wb = openpyxl.load_workbook(args.input, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(min_row=2, values_only=True))
    wb.close()

    bodies: list[dict[str, Any]] = []
    for row in rows:
        if not row or all(x is None or str(x).strip() == "" for x in row[:3]):
            continue
        bodies.append(row_to_body(row, args.restaurant_id))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(bodies, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Wrote {len(bodies)} bodies to {args.output}")
    if bodies:
        print("Sample (first item):\n", json.dumps(bodies[0], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
