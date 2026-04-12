"""
Resolve a stock image URL for a menu item (Unsplash, stable photo IDs).

Used by import_menu_from_xlsx when the sheet has no Image URL column.
Override: put an https URL in Excel column I (optional).
"""
from __future__ import annotations

import hashlib
from typing import Iterable, Optional

_W = "w=800&q=80"


def _u(photo_id: str) -> str:
    return f"https://images.unsplash.com/{photo_id}?{_W}"


# Longer / more specific phrases first (substring match on item name, lowercased).
_PHRASE_IMAGES: list[tuple[str, str]] = [
    ("choco lava", _u("photo-1624353365646-e1a586ea6e52")),
    ("chocolate truffle", _u("photo-1578985545062-69928b1d9587")),
    ("red velvet", _u("photo-1586985289688-ca3cf47d3e4e")),
    ("black forest", _u("photo-1578985545062-69928b1d9587")),
    ("butterscotch", _u("photo-1621303837174-89787e7b8120")),
    ("pineapple", _u("photo-1464349095431-e9a21285b5f3")),
    ("vanilla cool", _u("photo-1464349095431-e9a21285b5f3")),
    ("belgium chocolate", _u("photo-1606313564200-e75d5e30476c")),
    ("oreo", _u("photo-1572490122747-3968b75cc699")),
    ("nutella", _u("photo-1572490122747-3968b75cc699")),
    ("kit-kat", _u("photo-1560008581-09826e1e69d5")),
    ("cold coffee", _u("photo-1461023058943-07fcbe16d735")),
    ("milkshake", _u("photo-1572490122747-3968b75cc699")),
    ("lassi", _u("photo-1556881283-f691687c92f6")),
    ("mocktail", _u("photo-1536935338788-846bb9981813")),
    ("blue curacao", _u("photo-1536935338788-846bb9981813")),
    ("strawberry love", _u("photo-1572490122747-3968b75cc699")),
    ("peri peri", _u("photo-1573080496219-bb080dd4f877")),
    ("french fries", _u("photo-1573080496219-bb080dd4f877")),
    ("cheese masala french", _u("photo-1573080496219-bb080dd4f877")),
    ("chicken popcorn", _u("photo-1598103442097-8b74394b95c6")),
    ("chicken nugget", _u("photo-1626082927389-6cd097cdc6ec")),
    ("chicken finger", _u("photo-1598103442097-8b74394b95c6")),
    ("chilli garlic potato", _u("photo-1573080496219-bb080dd4f877")),
    ("cheese pizza finger", _u("photo-1565299624946-b28f40a0ae38")),
    ("veg spl", _u("photo-1565299624946-b28f40a0ae38")),
    ("margarita pizza", _u("photo-1574071318508-1cdbab80d002")),
    ("double decker", _u("photo-1528735602780-2552fd46c7af")),
    ("pav bhaji", _u("photo-1606491956689-2ea866880c84")),
    ("vada pav", _u("photo-1606491956689-2ea866880c84")),
    ("pani poori", _u("photo-1626110939710-22109d141838")),
    ("dahi ", _u("photo-1601050690597-df0568f70950")),
    ("samosa ragada", _u("photo-1601050690597-df0568f70950")),
    ("bhel puri", _u("photo-1601050690597-df0568f70950")),
    ("sev puri", _u("photo-1601050690597-df0568f70950")),
    ("plum cake", _u("photo-1606890737304-57a1ca2a5b62")),
    ("osmania", _u("photo-1509440159596-0249088772ff")),
    ("milk bread", _u("photo-1509440159596-0249088772ff")),
    ("brown bread", _u("photo-1509440159596-0249088772ff")),
    ("fruit bread", _u("photo-1509440159596-0249088772ff")),
    ("cream roll", _u("photo-1557925923-cd4648e4fcd3")),
    ("pastry", _u("photo-1557925923-cd4648e4fcd3")),
    ("honey cake", _u("photo-1464349095431-e9a21285b5f3")),
    ("any veg pizza", _u("photo-1565299624946-b28f40a0ae38")),
    ("any non-veg pizza", _u("photo-1593560708920-61dd98c46a4e")),
    ("any veg burger", _u("photo-1568901346375-23c9450c58cd")),
    ("any non-veg burger", _u("photo-1568901346375-23c9450c58cd")),
    ("sharbath", _u("photo-1544145945-f90425340bce")),
    ("brownie", _u("photo-1606313564200-e75d5e30476c")),
    ("egg puff", _u("photo-1567620905732-2d1ec7ab7445")),
    ("chicken puff", _u("photo-1598103442097-8b74394b95c6")),
    ("veg puff", _u("photo-1567620905732-2d1ec7ab7445")),
    ("paneer puff", _u("photo-1631452180519-c014fe946bc7")),
    ("samosa", _u("photo-1601050690597-df0568f70950")),
    ("pizza", _u("photo-1565299624946-b28f40a0ae38")),
    ("burger", _u("photo-1568901346375-23c9450c58cd")),
    ("sandwich", _u("photo-1528735602780-2552fd46c7af")),
    ("nugget", _u("photo-1626082927389-6cd097cdc6ec")),
    ("finger", _u("photo-1573080496219-bb080dd4f877")),
    ("popcorn", _u("photo-1598103442097-8b74394b95c6")),
    ("chicken", _u("photo-1598103442097-8b74394b95c6")),
    ("paneer", _u("photo-1631452180519-c014fe946bc7")),
    ("mushroom", _u("photo-1540189549336-e13ebbc87588")),
    ("babycorn", _u("photo-1540189549336-e13ebbc87588")),
    ("sweet corn", _u("photo-1540189549336-e13ebbc87588")),
    ("cake", _u("photo-1578985545062-69928b1d9587")),
    ("coffee", _u("photo-1461023058943-07fcbe16d735")),
    ("shake", _u("photo-1572490122747-3968b75cc699")),
    ("biscuit", _u("photo-1499636136210-6f4ee915583e")),
    ("bread", _u("photo-1509440159596-0249088772ff")),
    ("cutlet", _u("photo-1585937421612-63592988bf9b")),
    ("tikki", _u("photo-1601050690597-df0568f70950")),
    ("chaat", _u("photo-1601050690597-df0568f70950")),
    ("combo", _u("photo-1414235077428-338989a2e8c0")),
]

_CATEGORY_POOLS: dict[str, list[str]] = {
    "Snacks": [
        _u("photo-1601050690597-df0568f70950"),
        _u("photo-1567620905732-2d1ec7ab7445"),
        _u("photo-1573080496219-bb080dd4f877"),
    ],
    "Chef Special": [
        _u("photo-1606313564200-e75d5e30476c"),
        _u("photo-1544145945-f90425340bce"),
    ],
    "Starters": [
        _u("photo-1573080496219-bb080dd4f877"),
        _u("photo-1626082927389-6cd097cdc6ec"),
        _u("photo-1598103442097-8b74394b95c6"),
    ],
    "Pizza": [
        _u("photo-1565299624946-b28f40a0ae38"),
        _u("photo-1513104890138-7c749659a591"),
        _u("photo-1593560708920-61dd98c46a4e"),
    ],
    "Burgers": [
        _u("photo-1568901346375-23c9450c58cd"),
        _u("photo-1550547660-d9450f859349"),
    ],
    "Sandwiches": [
        _u("photo-1528735602780-2552fd46c7af"),
        _u("photo-1553909489-cd47e0907980"),
    ],
    "Sides": [
        _u("photo-1573080496219-bb080dd4f877"),
        _u("photo-1550547660-d9450f859349"),
    ],
    "Combos": [
        _u("photo-1414235077428-338989a2e8c0"),
        _u("photo-1504674900247-0877df9cc836"),
    ],
    "Beverages": [
        _u("photo-1572490122747-3968b75cc699"),
        _u("photo-1536935338788-846bb9981813"),
        _u("photo-1556881283-f691687c92f6"),
        _u("photo-1461023058943-07fcbe16d735"),
    ],
    "Cakes": [
        _u("photo-1578985545062-69928b1d9587"),
        _u("photo-1464349095431-e9a21285b5f3"),
        _u("photo-1606890737304-57a1ca2a5b62"),
    ],
    "Bakery": [
        _u("photo-1509440159596-0249088772ff"),
        _u("photo-1557925923-cd4648e4fcd3"),
        _u("photo-1499636136210-6f4ee915583e"),
    ],
    "Chaat": [
        _u("photo-1626110939710-22109d141838"),
        _u("photo-1601050690597-df0568f70950"),
        _u("photo-1606491956689-2ea866880c84"),
    ],
}

_DEFAULT_POOL = [
    _u("photo-1504674900247-0877df9cc836"),
    _u("photo-1546069901-ba9599a7e63c"),
    _u("photo-1476224203421-9ac39bcb3327"),
]


def _pick_from_pool(pool: Iterable[str], name: str) -> str:
    p = list(pool)
    if not p:
        p = _DEFAULT_POOL
    h = hashlib.md5(name.encode("utf-8")).hexdigest()
    return p[int(h, 16) % len(p)]


def resolve_menu_item_image(
    name: str,
    category: str,
    subcategory: str,
    excel_image_url: Optional[str] = None,
) -> str:
    """
    Return one HTTPS image URL for the item.
    excel_image_url: optional column I from spreadsheet.
    """
    raw = (excel_image_url or "").strip()
    if raw.lower().startswith("http://") or raw.lower().startswith("https://"):
        return raw

    n = name.lower().strip()
    for phrase, url in _PHRASE_IMAGES:
        if phrase in n:
            return url

    cat = (category or "").strip()
    pool = _CATEGORY_POOLS.get(cat)
    if pool:
        return _pick_from_pool(pool, name)

    return _pick_from_pool(_DEFAULT_POOL, name + cat + (subcategory or ""))
