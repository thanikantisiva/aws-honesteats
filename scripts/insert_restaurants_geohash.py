#!/usr/bin/env python3
"""
Bulk-insert mock restaurants (geohash from lat/lng via API) with large menus.

Aligned with current API:
  POST /api/v1/restaurants  — restaurantImage: string or array of image URLs (2–4 in seed data)
  POST /api/v1/restaurants/{id}/menu — image: string or array (1–2 URLs per dish)

Geohash is computed server-side on the Restaurant model (precision 7).

By default, restaurants are spaced from 0.2 km up to 4 km from CENTER_LAT/CENTER_LNG
(use --radius-km / --min-distance-km to change).

Environment:
  HONESTEATS_API_URL — API base (do not include /api/v1). Trailing slash optional.
    Default: https://api.dev.yumdude.com (same as rork-honesteats lib/api-config DEV_BASE_URL)
  REQUEST_DELAY_SEC   — delay between HTTP calls (default 0.05)
  HONESTEATS_RETOOL_BYPASS — value for x-retool-header (matches app.py RETOOL_BYPASS_VALUE / Lambda env).
    Default: 9f2b7c4a6d1e8f30b5a9c2e7d4f1a6bc  Set empty to omit the header.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

# -----------------------------------------------------------------------------
# Paths / defaults
# -----------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

def _normalize_api_base(url: str) -> str:
    """Accept base ending with /default or /default/api/v1."""
    u = url.rstrip("/")
    if u.endswith("/api/v1"):
        u = u[: -len("/api/v1")]
    return u


DEFAULT_API_URL = _normalize_api_base(
    os.environ.get(
        "HONESTEATS_API_URL",
        "https://api.dev.yumdude.com",
    )
)

# Matches app.py AUTH_BYPASS_HEADER / get_secret("RETOOL_BYPASS_VALUE", ...)
RETOOL_BYPASS_HEADER = "x-retool-header"
DEFAULT_RETOOL_BYPASS = os.environ.get(
    "HONESTEATS_RETOOL_BYPASS",
    "9f2b7c4a6d1e8f30b5a9c2e7d4f1a6bc",
)

# Nandyala, Andhra Pradesh (same as legacy script)
CENTER_LAT = 15.4729283
CENTER_LNG = 78.4574246

DEFAULT_LOCATION_ID = "NANDYALA"

# All seeded restaurants are placed between MIN_SEED_DISTANCE_KM and RADIUS_KM from CENTER_*.
RADIUS_KM = 4.0
MIN_SEED_DISTANCE_KM = 0.2

# At least 40 restaurants, ≥200 items each, 30 distinct categories per restaurant
NUM_RESTAURANTS = 40
MIN_MENU_ITEMS = 200
NUM_CATEGORIES = 30
# 30 * 7 = 210 ≥ 200
ITEMS_PER_CATEGORY = 7

# -----------------------------------------------------------------------------
# 30 menu categories (distinct per restaurant; reused names across restaurants)
# -----------------------------------------------------------------------------
MENU_CATEGORIES: List[str] = [
    "Chef's Specials",
    "Starters — Veg",
    "Starters — Non-Veg",
    "Soups & Broths",
    "Salads & Bowls",
    "Tandoor & Grill",
    "Curries — Vegetarian",
    "Curries — Chicken",
    "Curries — Mutton",
    "Biryani & Pulao",
    "Rice & Noodles",
    "Indian Breads",
    "South Indian Classics",
    "Indo-Chinese Starters",
    "Indo-Chinese Mains",
    "Thai Favourites",
    "Continental",
    "Burgers",
    "Sandwiches & Wraps",
    "Pizza",
    "Pasta",
    "Breakfast",
    "Kids Menu",
    "Desserts",
    "Ice Cream & Falooda",
    "Hot Beverages",
    "Cold Beverages",
    "Shakes & Smoothies",
    "Snacks & Sides",
    "Meal Combos",
]

assert len(MENU_CATEGORIES) == NUM_CATEGORIES, "MENU_CATEGORIES must match NUM_CATEGORIES"

DISH_STYLES = (
    "Classic",
    "Royal",
    "Spicy",
    "Chef's",
    "House",
    "Golden",
    "Signature",
    "Grand",
    "Mild",
    "Tandoori",
    "Special",
    "Supreme",
)

FOOD_IMAGES: List[str] = [
    "https://images.unsplash.com/photo-1517248135467-4c7edcad34c4?w=400",
    "https://images.unsplash.com/photo-1555396273-367ea4eb4db5?w=400",
    "https://images.unsplash.com/photo-1579584425555-c3ce17fd4351?w=400",
    "https://images.unsplash.com/photo-1550547660-d9450f859349?w=400",
    "https://images.unsplash.com/photo-1559314809-0d155014e29e?w=400",
    "https://images.unsplash.com/photo-1589301760014-d929f3979dbc?w=400",
    "https://images.unsplash.com/photo-1565299624946-b28f40a0ae38?w=400",
    "https://images.unsplash.com/photo-1567620905732-2d1ec7ab7445?w=400",
    "https://images.unsplash.com/photo-1546069901-ba9599a7e63c?w=400",
    "https://images.unsplash.com/photo-1504674900247-0877df9cc836?w=400",
    "https://images.unsplash.com/photo-1476224203421-9ac39bcb3327?w=400",
    "https://images.unsplash.com/photo-1490645935967-10de6ba17061?w=400",
]


@dataclass(frozen=True)
class RestaurantTemplate:
    name: str
    cuisine: List[str]


RESTAURANT_ARCHETYPES: List[RestaurantTemplate] = [
    RestaurantTemplate("Spice Garden", ["Indian", "North Indian", "Biryani"]),
    RestaurantTemplate("Pizza Paradise", ["Italian", "Pizza", "Fast Food"]),
    RestaurantTemplate("Sushi Express", ["Japanese", "Sushi", "Asian"]),
    RestaurantTemplate("Burger Hub", ["American", "Burgers", "Fast Food"]),
    RestaurantTemplate("Thai Corner", ["Thai", "Asian", "Noodles"]),
    RestaurantTemplate("Dosa Point", ["South Indian", "Breakfast", "Vegetarian"]),
    RestaurantTemplate("Mughal Darbar", ["Mughlai", "Kebabs", "North Indian"]),
    RestaurantTemplate("Coastal Catch", ["Seafood", "Coastal", "Kerala"]),
]


def pick_restaurant_gallery(rng: random.Random) -> List[str]:
    """2–4 distinct images per restaurant (API: restaurantImage as array)."""
    k = rng.randint(2, min(4, len(FOOD_IMAGES)))
    return rng.sample(FOOD_IMAGES, k=k)


def pick_dish_images(rng: random.Random, restaurant_gallery: Sequence[str]) -> List[str]:
    """1 or 2 images per dish (API: image as string or array of URLs)."""
    want = rng.choice([1, 2])
    # Prefer mixing gallery + stock so carousel feels tied to the venue
    merged = list(dict.fromkeys(list(restaurant_gallery) + list(FOOD_IMAGES)))
    if not merged:
        return []
    want = min(want, len(merged))
    return rng.sample(merged, k=want)


def generate_coordinate_at_distance(
    center_lat: float,
    center_lng: float,
    distance_km: float,
    bearing_degrees: float,
) -> Tuple[float, float]:
    """Return (lat, lng) at distance_km and bearing from center."""
    r_earth = 6371.0
    bearing = math.radians(bearing_degrees)
    lat1 = math.radians(center_lat)
    lon1 = math.radians(center_lng)

    lat2 = math.asin(
        math.sin(lat1) * math.cos(distance_km / r_earth)
        + math.cos(lat1) * math.sin(distance_km / r_earth) * math.cos(bearing)
    )
    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(distance_km / r_earth) * math.cos(lat1),
        math.cos(distance_km / r_earth) - math.sin(lat1) * math.sin(lat2),
    )
    return math.degrees(lat2), math.degrees(lon2)


def _pick_veg_for_category(category: str) -> bool:
    c = category.lower()
    if "non-veg" in c or "chicken" in c or "mutton" in c or "seafood" in c:
        return False
    if "veg" in c and "non" not in c:
        return True
    return random.choice([True, True, False])


def build_menu_item_payload(
    *,
    category: str,
    item_index: int,
    restaurant_gallery: Sequence[str],
    rng: random.Random,
) -> Dict[str, Any]:
    """Single menu row for POST /restaurants/{id}/menu."""
    style = DISH_STYLES[item_index % len(DISH_STYLES)]
    short_cat = category.split("—")[0].strip()[:24]
    name = f"{style} {short_cat} #{item_index + 1}"
    restaurant_price = float(rng.randint(45, 520))
    hike = float(rng.choice([8, 10, 12, 15, 18]))
    is_veg = _pick_veg_for_category(category)
    sub: Optional[str] = None
    if rng.random() < 0.35:
        sub = rng.choice(["Regular", "Large", "Family", "Single", "Combo"])

    dish_images = pick_dish_images(rng, restaurant_gallery)
    image_field: Any = dish_images[0] if len(dish_images) == 1 else dish_images

    return {
        "name": name,
        "restaurantPrice": restaurant_price,
        "hikePercentage": hike,
        "category": category,
        **({"subCategory": sub} if sub else {}),
        "isVeg": is_veg,
        "isAvailable": True,
        "description": f"{name} — {category}. Freshly prepared.",
        "image": image_field,
    }


def build_all_menu_items(
    restaurant_gallery: Sequence[str],
    seed: int,
) -> List[Dict[str, Any]]:
    """≥ MIN_MENU_ITEMS across exactly NUM_CATEGORIES distinct categories."""
    rng = random.Random(seed)
    items: List[Dict[str, Any]] = []
    global_idx = 0
    for category in MENU_CATEGORIES:
        for _ in range(ITEMS_PER_CATEGORY):
            items.append(
                build_menu_item_payload(
                    category=category,
                    item_index=global_idx,
                    restaurant_gallery=restaurant_gallery,
                    rng=rng,
                )
            )
            global_idx += 1
    assert len(items) >= MIN_MENU_ITEMS
    assert len({i["category"] for i in items}) == NUM_CATEGORIES
    return items


def distance_schedule(
    n: int,
    *,
    max_radius_km: float = RADIUS_KM,
    min_distance_km: float = MIN_SEED_DISTANCE_KM,
) -> List[float]:
    """Evenly spread distances from min_distance_km up to max_radius_km (inclusive)."""
    if n <= 0:
        return []
    lo = max(0.05, float(min_distance_km))
    hi = max(lo, float(max_radius_km))
    if n == 1:
        return [(lo + hi) / 2]
    return [lo + (hi - lo) * (i / (n - 1)) for i in range(n)]


class ApiClient:
    def __init__(
        self,
        base_url: str,
        delay_sec: float,
        session: Optional[requests.Session] = None,
        *,
        retool_bypass: Optional[str] = None,
    ):
        self.base_url = _normalize_api_base(base_url)
        self.delay_sec = delay_sec
        self.session = session or requests.Session()
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        bypass = DEFAULT_RETOOL_BYPASS if retool_bypass is None else retool_bypass
        if bypass:
            headers[RETOOL_BYPASS_HEADER] = bypass
        self.session.headers.update(headers)

    def _sleep(self) -> None:
        if self.delay_sec > 0:
            time.sleep(self.delay_sec)

    def create_restaurant(
        self,
        *,
        location_id: str,
        name: str,
        lat: float,
        lng: float,
        cuisine: Sequence[str],
        rating: float,
        owner_id: str,
        restaurant_images: Sequence[str],
    ) -> Optional[Dict[str, Any]]:
        imgs = [str(u) for u in restaurant_images if str(u).strip()]
        payload: Dict[str, Any] = {
            "locationId": location_id,
            "name": name,
            "latitude": lat,
            "longitude": lng,
            "isOpen": True,
            "cuisine": list(cuisine),
            "rating": rating,
            "ownerId": owner_id,
            "restaurantImage": imgs[0] if len(imgs) == 1 else imgs,
        }
        self._sleep()
        url = f"{self.base_url}/api/v1/restaurants"
        r = self.session.post(url, json=payload, timeout=30)
        if r.status_code == 201:
            return r.json()
        print(f"   ❌ Restaurant create failed: {r.status_code} — {r.text[:500]}")
        print(f"   ↳ POST {url}")
        return None

    def create_menu_item(self, restaurant_id: str, body: Dict[str, Any]) -> bool:
        self._sleep()
        url = f"{self.base_url}/api/v1/restaurants/{restaurant_id}/menu"
        r = self.session.post(url, json=body, timeout=30)
        if r.status_code == 201:
            return True
        print(f"      ✗ {body.get('name')} — {r.status_code} {r.text[:200]}")
        print(f"        POST {url}")
        return False


def run(
    *,
    api_url: str,
    num_restaurants: int,
    location_id: str,
    delay_sec: float,
    dry_run: bool,
    log_path: Optional[str],
    radius_km: float = RADIUS_KM,
    min_distance_km: float = MIN_SEED_DISTANCE_KM,
) -> None:
    distances = distance_schedule(
        num_restaurants,
        max_radius_km=radius_km,
        min_distance_km=min_distance_km,
    )
    client = ApiClient(api_url, delay_sec)
    created: List[Dict[str, Any]] = []
    menu_failures = 0

    print("=" * 72)
    print("Insert mock restaurants (geohash via API) + large menus")
    print(f"API: {api_url}")
    print(f"Center: {CENTER_LAT}, {CENTER_LNG}")
    print(
        f"Placement: {min_distance_km:.2f}–{radius_km:.2f} km from center (radius cap {radius_km:g} km)"
    )
    print(f"Restaurants: {num_restaurants} | Categories each: {NUM_CATEGORIES} | Items each: {NUM_CATEGORIES * ITEMS_PER_CATEGORY}")
    if DEFAULT_RETOOL_BYPASS:
        print(f"Auth bypass: {RETOOL_BYPASS_HEADER} present (len={len(DEFAULT_RETOOL_BYPASS)})")
    print("=" * 72)

    for i in range(num_restaurants):
        tpl = RESTAURANT_ARCHETYPES[i % len(RESTAURANT_ARCHETYPES)]
        dist = distances[i] if i < len(distances) else distances[-1]
        bearing = random.uniform(0, 360)
        lat, lng = generate_coordinate_at_distance(CENTER_LAT, CENTER_LNG, dist, bearing)
        name = f"{tpl.name} #{i + 1}"
        owner_id = f"OWNER{1000 + i}"
        rating = round(random.uniform(3.7, 4.95), 2)
        gallery_rng = random.Random(30_000 + i)
        restaurant_gallery = pick_restaurant_gallery(gallery_rng)
        menu_items = build_all_menu_items(restaurant_gallery, seed=10_000 + i)

        print(f"\n📍 [{i + 1}/{num_restaurants}] {name}")
        print(f"   lat={lat:.6f}, lng={lng:.6f} (~{dist:.2f} km, bearing {bearing:.0f}°)")
        print(f"   Gallery: {len(restaurant_gallery)} images | Menu: {len(menu_items)} items, {len({m['category'] for m in menu_items})} categories")

        if dry_run:
            continue

        resp = client.create_restaurant(
            location_id=location_id,
            name=name,
            lat=lat,
            lng=lng,
            cuisine=tpl.cuisine,
            rating=rating,
            owner_id=owner_id,
            restaurant_images=restaurant_gallery,
        )
        if not resp:
            continue

        rid = resp.get("restaurantId")
        gh = resp.get("geohash", "N/A")
        print(f"   ✅ restaurantId={rid} geohash={gh}")

        ok = 0
        for j, item in enumerate(menu_items):
            if client.create_menu_item(rid, item):
                ok += 1
            else:
                menu_failures += 1
            if (j + 1) % 50 == 0:
                print(f"      … {j + 1}/{len(menu_items)} items")

        print(f"   ✅ Menu items created: {ok}/{len(menu_items)}")
        created.append({"restaurantId": rid, "name": name, "items_ok": ok, "geohash": gh})

    if log_path and created:
        with open(log_path, "w", encoding="utf-8") as f:
            for row in created:
                f.write(json.dumps(row) + "\n")
        print(f"\n📝 Wrote log: {log_path}")

    print("\n" + "=" * 72)
    print(f"Done. Restaurants created: {len(created)}. Menu POST failures: {menu_failures}")
    print("=" * 72)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help="API base URL (or set HONESTEATS_API_URL)",
    )
    p.add_argument("--location-id", default=DEFAULT_LOCATION_ID)
    p.add_argument("--count", type=int, default=NUM_RESTAURANTS, help="Number of restaurants")
    p.add_argument(
        "--delay",
        type=float,
        default=float(os.environ.get("REQUEST_DELAY_SEC", "0.05")),
        help="Seconds between HTTP calls",
    )
    p.add_argument("--dry-run", action="store_true", help="Print plan only; no HTTP")
    p.add_argument("--log-jsonl", default="", help="Append-only JSONL path for created restaurant IDs")
    p.add_argument(
        "--radius-km",
        type=float,
        default=RADIUS_KM,
        help=f"Max distance from center for each restaurant (default: {RADIUS_KM})",
    )
    p.add_argument(
        "--min-distance-km",
        type=float,
        default=MIN_SEED_DISTANCE_KM,
        help=f"Min distance from center for closest seed (default: {MIN_SEED_DISTANCE_KM})",
    )
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    radius_km = max(0.1, args.radius_km)
    min_distance_km = max(0.05, min(args.min_distance_km, radius_km))
    run(
        api_url=args.api_url,
        num_restaurants=max(1, args.count),
        location_id=args.location_id,
        delay_sec=max(0.0, args.delay),
        dry_run=args.dry_run,
        log_path=args.log_jsonl or None,
        radius_km=radius_km,
        min_distance_km=min_distance_km,
    )


if __name__ == "__main__":
    main()
