"""App configuration routes"""
import json
from datetime import datetime, timezone
from aws_lambda_powertools import Logger, Tracer, Metrics
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import python_to_dynamodb, dynamodb_to_python
from utils.datetime_ist import now_ist_iso

logger = Logger()
tracer = Tracer()
metrics = Metrics()

# Fixed keys for global app config (not from request body)
CONFIG_PK = "CONFIG#GLOBAL"
CONFIG_SK = "CONFIG"


def _restaurant_config_pk(restaurant_id: str) -> str:
    """Build partition key for restaurant-specific config."""
    return f"CONFIG#RESTAURANT#{restaurant_id.strip()}"


def _extract_payload(body: dict):
    """Prefer explicit config object; otherwise store request body except key fields."""
    if "config" in body:
        return body.get("config")

    filtered = {
        k: v
        for k, v in body.items()
        if k not in {"configType", "configKey", "restaurantId"}
    }
    return filtered


def _fetch_config_item(pk: str, sk: str):
    """Fetch one config row from DynamoDB."""
    response = dynamodb_client.get_item(
        TableName=TABLES["CONFIG"],
        Key={
            "partitionkey": {"S": pk},
            "sortKey": {"S": sk}
        }
    )
    return response.get("Item")


def register_config_routes(app):
    """Register app config routes"""

    @app.post("/api/v1/globalconfig")
    @tracer.capture_method
    def upsert_config():
        """
        Store config JSON in DynamoDB.
        Request:
        {
          "restaurantId": "RES-...", // optional. if present stores restaurant specific config
          "config": { ... }  // optional; if absent full body is stored minus key fields
        }
        """
        try:
            body = app.current_event.json_body or {}
            restaurant_id = str(body.get("restaurantId", "")).strip()
            if restaurant_id:
                pk = _restaurant_config_pk(restaurant_id)
                sk = CONFIG_SK
            else:
                pk = CONFIG_PK
                sk = CONFIG_SK

            payload = _extract_payload(body)

            item = {
                "partitionkey": {"S": pk},
                "sortKey": {"S": sk},
                "config": python_to_dynamodb(payload),
                "updatedAt": {"S": now_ist_iso()}
            }

            dynamodb_client.put_item(
                TableName=TABLES["CONFIG"],
                Item=item
            )

            metrics.add_metric(name="ConfigSaved", unit="Count", value=1)
            return {
                "message": "Config saved",
                "partitionkey": pk,
                "sortKey": sk,
                "scope": "RESTAURANT" if restaurant_id else "GLOBAL"
            }, 200
        except Exception as e:
            logger.error("Error saving config", exc_info=True)
            return {"error": "Failed to save config", "message": str(e)}, 500

    @app.get("/api/v1/globalconfig")
    @tracer.capture_method
    def get_config():
        """Fetch config. If restaurant config is not found, fallback to global."""
        try:
            query_params = app.current_event.query_string_parameters or {}
            restaurant_id = str(query_params.get("restaurantId", "")).strip()

            source = "GLOBAL"
            pk, sk = CONFIG_PK, CONFIG_SK
            item = None

            if restaurant_id:
                restaurant_pk = _restaurant_config_pk(restaurant_id)
                item = _fetch_config_item(restaurant_pk, CONFIG_SK)
                if item:
                    pk = restaurant_pk
                    source = "RESTAURANT"

            if not item:
                item = _fetch_config_item(CONFIG_PK, CONFIG_SK)
                pk = CONFIG_PK
                source = "GLOBAL"

            if not item:
                return {"error": "Config not found"}, 404

            config_payload = dynamodb_to_python(item.get("config", {"NULL": True}))
            metrics.add_metric(name="ConfigFetched", unit="Count", value=1)
            return {
                "partitionkey": pk,
                "sortKey": CONFIG_SK,
                "config": config_payload,
                "updatedAt": item.get("updatedAt", {}).get("S"),
                "source": source
            }, 200
        except Exception as e:
            logger.error("Error fetching config", exc_info=True)
            return {"error": "Failed to fetch config", "message": str(e)}, 500

    @app.get("/api/v1/config/home-hero-banner")
    @tracer.capture_method
    def get_home_hero_banner():
        """Return active home hero banner(s). Supports one banner or a list under config.banners."""
        try:
            item = _fetch_config_item("BANNER#HOME_HERO", "ACTIVE")
            if not item:
                return {"heroBanners": [], "heroBanner": None}, 200

            config = dynamodb_to_python(item.get("config", {"NULL": True}))
            if isinstance(config, str):
                try:
                    config = json.loads(config)
                except json.JSONDecodeError:
                    config = {}
            if not isinstance(config, dict):
                return {"heroBanners": [], "heroBanner": None}, 200

            now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            def _banner_active(b: dict) -> bool:
                if not isinstance(b, dict):
                    return False
                is_active = b.get("isActive", True)
                starts_ok = not b.get("startDate") or b["startDate"] <= now
                ends_ok = not b.get("endDate") or b["endDate"] >= now
                return bool(is_active and starts_ok and ends_ok)

            # New shape: { "banners": [ {...}, ... ] }. Empty list = no banners (do not treat whole row as legacy).
            # Legacy: single banner stored as the whole config object (no "banners" key).
            raw_list = config.get("banners")
            if "banners" in config:
                if isinstance(raw_list, list):
                    candidates = [b for b in raw_list if isinstance(b, dict)]
                else:
                    candidates = []
            else:
                candidates = [config] if isinstance(config, dict) else []

            active = [b for b in candidates if _banner_active(b)]
            active.sort(key=lambda b: (b.get("priority", 99), b.get("id", "")))

            if not active:
                return {"heroBanners": [], "heroBanner": None}, 200

            return {
                "heroBanners": active,
                "heroBanner": active[0],
            }, 200
        except Exception as e:
            logger.error("Error fetching home hero banner", exc_info=True)
            return {"error": str(e)}, 500

    @app.get("/api/v1/config/promo-cards")
    @tracer.capture_method
    def get_promo_cards():
        """Return active promotional cards for the home screen."""
        try:
            item = _fetch_config_item("PROMO#HOME", "ACTIVE")
            if not item:
                return {"cards": []}, 200

            config = dynamodb_to_python(item.get("config", {"NULL": True}))
            cards = config.get("cards", []) if isinstance(config, dict) else []

            now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            active = [
                c for c in cards
                if c.get("isActive", False)
                and (not c.get("startDate") or c["startDate"] <= now)
                and (not c.get("endDate") or c["endDate"] >= now)
            ]
            active.sort(key=lambda c: c.get("priority", 99))

            # Promo cards are display-only; do not expose deep-link actions to the app.
            sanitized = []
            for c in active:
                card = dict(c)
                card.pop("ctaAction", None)
                sanitized.append(card)

            return {"cards": sanitized}, 200
        except Exception as e:
            logger.error("Error fetching promo cards", exc_info=True)
            return {"error": str(e)}, 500
