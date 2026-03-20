"""
Seed script: Insert home hero banner + promo cards into DynamoDB ConfigTable.

Usage:
  ENVIRONMENT=dev python scripts/seed_home_banners.py
  ENVIRONMENT=prod python scripts/seed_home_banners.py
"""
import os
import json
import boto3

ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
TABLE_NAME = f"food-delivery-config-{ENVIRONMENT}"

dynamodb = boto3.client("dynamodb", region_name="ap-south-1")


def python_to_dynamodb(obj):
    """Simple Python -> DynamoDB attribute value converter."""
    if obj is None:
        return {"NULL": True}
    if isinstance(obj, bool):
        return {"BOOL": obj}
    if isinstance(obj, (int, float)):
        return {"N": str(obj)}
    if isinstance(obj, str):
        return {"S": obj}
    if isinstance(obj, list):
        return {"L": [python_to_dynamodb(i) for i in obj]}
    if isinstance(obj, dict):
        return {"M": {k: python_to_dynamodb(v) for k, v in obj.items()}}
    return {"S": str(obj)}


HERO_BANNER = {
    "id": "home-hero-dark",
    # Replace this with your production CDN/S3 image URL.
    "backgroundImageUrl": "https://images.unsplash.com/photo-1504674900247-0877df9cc836?auto=format&fit=crop&w=1600&q=80",
    "badgeText": "NEW USER OFFER",
    "title": "15% EXTRA DISCOUNT",
    "subtitle": "Get your first order delivery free!",
    "button": "explore now",
    # Use restaurant://<restaurant-id> to open a restaurant from the customer app.
    "ctaAction": "restaurant://REPLACE_WITH_RESTAURANT_ID",
    "startDate": "2026-01-01",
    "endDate": "2027-12-31",
    "isActive": True,
    "priority": 1,
}

# API returns heroBanners as a list; legacy single-object config is still supported.
HERO_BANNERS_CONFIG = {
    "banners": [HERO_BANNER],
}

PROMO_CARDS = {
    "cards": [
        {
            "id": "flash-sale",
            "type": "promo",
            "title": "FLASH SALE",
            "subtitle": "Limited time deals on your favorite meals",
            "icon": "tag",
            "priority": 1,
            "startDate": "2026-01-01",
            "endDate": "2027-12-31",
            "isActive": True,
        },
        {
            "id": "cuisine-biryani",
            "type": "cuisine_push",
            "title": "Craving Biryani?",
            "subtitle": "5 new options near you",
            "icon": "utensils",
            "priority": 2,
            "startDate": "2026-01-01",
            "endDate": "2027-12-31",
            "isActive": True,
        },
        {
            "id": "rainy-day",
            "type": "contextual",
            "title": "Rainy Day Special",
            "subtitle": "Hot soup delivered in 30 mins",
            "icon": "bike",
            "priority": 3,
            "startDate": "2026-01-01",
            "endDate": "2027-12-31",
            "isActive": False,
        },
    ]
}


def put_item(pk: str, sk: str, config: dict, label: str):
    item = {
        "partitionkey": {"S": pk},
        "sortKey": {"S": sk},
        "config": python_to_dynamodb(config),
    }
    dynamodb.put_item(TableName=TABLE_NAME, Item=item)
    print(f"  Inserted {label} -> {pk} / {sk}")


def main():
    print(f"Seeding home hero banner & promo cards into: {TABLE_NAME}\n")

    put_item("BANNER#HOME_HERO", "ACTIVE", HERO_BANNERS_CONFIG, "Home Hero Banner(s)")
    put_item("PROMO#HOME", "ACTIVE", PROMO_CARDS, "Promo Cards")

    print(f"\nDone! {len(HERO_BANNERS_CONFIG['banners'])} hero banner(s) and {len(PROMO_CARDS['cards'])} promo cards seeded.")
    print("\nActive items (shown to users now):")
    if HERO_BANNER.get("isActive"):
        print(f"  Hero Banner: {HERO_BANNER.get('id')} - {HERO_BANNER.get('backgroundImageUrl')}")
    for c in PROMO_CARDS["cards"]:
        if c["isActive"]:
            print(f"  Promo:  {c['id']} - {c['title']}")
    print("\nInactive items (ready to activate via DynamoDB/Retool):")
    if not HERO_BANNER.get("isActive"):
        print(f"  Hero Banner: {HERO_BANNER.get('id')}")
    for c in PROMO_CARDS["cards"]:
        if not c["isActive"]:
            print(f"  Promo:  {c['id']} - {c['title']}")


if __name__ == "__main__":
    main()
