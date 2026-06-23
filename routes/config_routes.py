"""App configuration routes"""
import json
from datetime import datetime, timezone
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.cod_config_service import (
    COD_CONFIG_PK,
    COD_CONFIG_SK,
    LEGACY_COD_KEY,
    parse_hhmm,
)
from services.yumcoins_config_service import (
    YUMCOINS_CONFIG_PK,
    YUMCOINS_CONFIG_SK,
    YUMCOINS_KEYS,
)
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import python_to_dynamodb, dynamodb_to_python
from utils.datetime_ist import now_ist_iso, now_ist_strftime

logger = Logger()
tracer = Tracer()
metrics = Metrics()

# Fixed keys for global app config (not from request body)
CONFIG_PK = "CONFIG#GLOBAL"
CONFIG_SK = "CONFIG"


def _restaurant_config_pk(restaurant_id: str) -> str:
    """Build partition key for restaurant-specific config."""
    return f"CONFIG#RESTAURANT#{restaurant_id.strip()}"


def _theatre_config_pk(theatre_name: str) -> str:
    """Build partition key for theatre show-timing config."""
    return f"THEATRE#{theatre_name.strip()}"


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


def _coerce_bool(value, default=False) -> bool:
    """Coerce common JSON/admin values to bool while preserving explicit false."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
        return default
    return bool(value)


def _normalize_home_hero_banner(banner: dict) -> dict:
    """Return a banner copy with defaults expected by the customer app."""
    normalized = dict(banner)
    normalized["mask"] = _coerce_bool(normalized.get("mask"), True)
    return normalized


def _is_active_in_date(config: dict, today: str) -> bool:
    """Return true when a config object is active and inside its date window."""
    if not isinstance(config, dict):
        return False
    is_active = _coerce_bool(config.get("isActive"), True)
    starts_ok = not config.get("startDate") or config["startDate"] <= today
    ends_ok = not config.get("endDate") or config["endDate"] >= today
    return bool(is_active and starts_ok and ends_ok)


def _normalize_daily_deal_popup(popup: dict) -> dict:
    """Normalize popup and slide content types while preserving admin-authored content."""
    normalized = dict(popup)
    content_type = str(normalized.get("contentType") or "image").lower()
    if content_type not in {"image", "html", "video"}:
        content_type = "image"
    normalized["contentType"] = content_type

    slides = normalized.get("slides")
    if isinstance(slides, list):
        clean_slides = []
        for index, slide in enumerate(slides):
            if not isinstance(slide, dict):
                continue
            clean_slide = dict(slide)
            slide_type = str(clean_slide.get("contentType") or content_type).lower()
            if slide_type not in {"image", "html", "video"}:
                slide_type = content_type
            clean_slide["contentType"] = slide_type
            clean_slide["id"] = str(clean_slide.get("id") or f"slide-{index + 1}")
            clean_slides.append(clean_slide)
        normalized["slides"] = clean_slides

    return normalized


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

    @app.get("/api/v1/yumcoins-config")
    @tracer.capture_method
    def get_yumcoins_config():
        """Fetch the dedicated YumCoins config (walletConfig / referralConfig /
        orderCashbackConfig). Falls back to legacy CONFIG#GLOBAL keys if the
        dedicated row hasn't been created yet."""
        try:
            item = _fetch_config_item(YUMCOINS_CONFIG_PK, YUMCOINS_CONFIG_SK)
            source = "YUMCOINS"
            updated_at = None
            config = {}

            if item:
                parsed = dynamodb_to_python(item.get("config", {"NULL": True}))
                config = parsed if isinstance(parsed, dict) else {}
                updated_at = item.get("updatedAt", {}).get("S")
            else:
                # Legacy fallback: pull the coin keys out of the global config row.
                legacy = _fetch_config_item(CONFIG_PK, CONFIG_SK)
                if legacy:
                    parsed = dynamodb_to_python(legacy.get("config", {"NULL": True}))
                    if isinstance(parsed, dict):
                        config = {k: parsed[k] for k in YUMCOINS_KEYS if k in parsed}
                source = "GLOBAL_LEGACY"

            metrics.add_metric(name="YumCoinsConfigFetched", unit="Count", value=1)
            return {
                "walletConfig": config.get("walletConfig", {}),
                "referralConfig": config.get("referralConfig", {}),
                "orderCashbackConfig": config.get("orderCashbackConfig", {}),
                "updatedAt": updated_at,
                "source": source,
            }, 200
        except Exception as e:
            logger.error("Error fetching YumCoins config", exc_info=True)
            return {"error": "Failed to fetch YumCoins config", "message": str(e)}, 500

    @app.post("/api/v1/yumcoins-config")
    @tracer.capture_method
    def save_yumcoins_config():
        """Create/replace the dedicated YumCoins config row. Accepts the three
        config objects either at the top level or nested under `config`; only the
        known keys are persisted."""
        try:
            body = app.current_event.json_body or {}
            source = body.get("config") if isinstance(body.get("config"), dict) else body
            if not isinstance(source, dict):
                return {"error": "Body must be a JSON object"}, 400

            config = {}
            for key in YUMCOINS_KEYS:
                value = source.get(key)
                if value is None:
                    continue
                if not isinstance(value, dict):
                    return {"error": f"{key} must be an object"}, 400
                config[key] = value

            item = {
                "partitionkey": {"S": YUMCOINS_CONFIG_PK},
                "sortKey": {"S": YUMCOINS_CONFIG_SK},
                "config": python_to_dynamodb(config),
                "updatedAt": {"S": now_ist_iso()},
            }
            dynamodb_client.put_item(TableName=TABLES["CONFIG"], Item=item)

            metrics.add_metric(name="YumCoinsConfigSaved", unit="Count", value=1)
            return {"message": "YumCoins config saved", "config": config}, 200
        except Exception as e:
            logger.error("Error saving YumCoins config", exc_info=True)
            return {"error": "Failed to save YumCoins config", "message": str(e)}, 500

    @app.get("/api/v1/cod-config")
    @tracer.capture_method
    def get_cod_config():
        """Fetch the dedicated COD config (disableCod / minAmount / maxAmount).
        Falls back to legacy CONFIG#GLOBAL.codConfig if the dedicated row hasn't
        been created yet."""
        try:
            item = _fetch_config_item(COD_CONFIG_PK, COD_CONFIG_SK)
            source = "COD"
            updated_at = None
            cod = {}

            if item:
                parsed = dynamodb_to_python(item.get("config", {"NULL": True}))
                cod = parsed if isinstance(parsed, dict) else {}
                updated_at = item.get("updatedAt", {}).get("S")
            else:
                legacy = _fetch_config_item(CONFIG_PK, CONFIG_SK)
                if legacy:
                    parsed = dynamodb_to_python(legacy.get("config", {"NULL": True}))
                    if isinstance(parsed, dict) and isinstance(parsed.get(LEGACY_COD_KEY), dict):
                        cod = parsed[LEGACY_COD_KEY]
                source = "GLOBAL_LEGACY"

            metrics.add_metric(name="CodConfigFetched", unit="Count", value=1)
            return {"codConfig": cod, "updatedAt": updated_at, "source": source}, 200
        except Exception as e:
            logger.error("Error fetching COD config", exc_info=True)
            return {"error": "Failed to fetch COD config", "message": str(e)}, 500

    @app.post("/api/v1/cod-config")
    @tracer.capture_method
    def save_cod_config():
        """Create/replace the dedicated COD config row. Accepts the settings
        either at the top level or nested under `codConfig`."""
        try:
            body = app.current_event.json_body or {}
            source = body.get("codConfig") if isinstance(body.get("codConfig"), dict) else body
            if not isinstance(source, dict):
                return {"error": "Body must be a JSON object"}, 400

            cod = {}
            if "disableCod" in source:
                cod["disableCod"] = _coerce_bool(source.get("disableCod"))
            for key in ("minAmount", "maxAmount"):
                if source.get(key) is None:
                    continue
                try:
                    cod[key] = float(source[key])
                except (TypeError, ValueError):
                    return {"error": f"{key} must be a number"}, 400
            if "minAmount" in cod and "maxAmount" in cod and cod["minAmount"] > cod["maxAmount"]:
                return {"error": "minAmount cannot exceed maxAmount"}, 400

            # Optional availability window (24h "HH:MM"). Empty string clears it.
            for key in ("availableFrom", "availableTo"):
                value = source.get(key)
                if value is None or (isinstance(value, str) and not value.strip()):
                    continue
                if parse_hhmm(value) is None:
                    return {"error": f"{key} must be a 24h time in HH:MM format"}, 400
                cod[key] = value.strip()
            if ("availableFrom" in cod) != ("availableTo" in cod):
                return {"error": "availableFrom and availableTo must be set together"}, 400

            item = {
                "partitionkey": {"S": COD_CONFIG_PK},
                "sortKey": {"S": COD_CONFIG_SK},
                "config": python_to_dynamodb(cod),
                "updatedAt": {"S": now_ist_iso()},
            }
            dynamodb_client.put_item(TableName=TABLES["CONFIG"], Item=item)

            metrics.add_metric(name="CodConfigSaved", unit="Count", value=1)
            return {"message": "COD config saved", "codConfig": cod}, 200
        except Exception as e:
            logger.error("Error saving COD config", exc_info=True)
            return {"error": "Failed to save COD config", "message": str(e)}, 500

    @app.get("/api/v1/config/home-hero-banner")
    @tracer.capture_method
    def get_home_hero_banner():
        """Return home hero banner(s).
        Customer mode (default): returns only active/in-date banners.
        Admin mode (?admin=true): returns ALL banners (including inactive) for management.
        """
        try:
            query_params = app.current_event.query_string_parameters or {}
            admin_mode = str(query_params.get("admin", "")).lower() == "true"

            item = _fetch_config_item("BANNER#HOME_HERO", "ACTIVE")
            if not item:
                if admin_mode:
                    return {"banners": [], "total": 0}, 200
                return {"heroBanners": [], "heroBanner": None}, 200

            config = dynamodb_to_python(item.get("config", {"NULL": True}))
            if isinstance(config, str):
                try:
                    config = json.loads(config)
                except json.JSONDecodeError:
                    config = {}
            if not isinstance(config, dict):
                if admin_mode:
                    return {"banners": [], "total": 0}, 200
                return {"heroBanners": [], "heroBanner": None}, 200

            # Admin mode: return ALL banners unfiltered.
            if admin_mode:
                raw_list = config.get("banners", [])
                if not isinstance(raw_list, list):
                    raw_list = []
                banners = [_normalize_home_hero_banner(b) for b in raw_list if isinstance(b, dict)]
                return {"banners": banners, "total": len(banners)}, 200

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

            active = [_normalize_home_hero_banner(b) for b in candidates if _banner_active(b)]
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

    @app.post("/api/v1/config/home-hero-banner")
    @tracer.capture_method
    def save_home_hero_banner():
        """Save/replace the full list of home hero banners."""
        try:
            import uuid as _uuid
            body = app.current_event.json_body or {}
            banners = body.get("banners", [])

            if not isinstance(banners, list):
                return {"error": "banners must be a list"}, 400

            # Auto-assign IDs and normalize defaults for any banner missing them.
            for b in banners:
                if isinstance(b, dict):
                    if not b.get("id"):
                        b["id"] = _uuid.uuid4().hex[:8]
                    b["mask"] = _coerce_bool(b.get("mask"), True)

            config = {"banners": banners}

            item = {
                "partitionkey": {"S": "BANNER#HOME_HERO"},
                "sortKey": {"S": "ACTIVE"},
                "config": python_to_dynamodb(config),
                "updatedAt": {"S": now_ist_iso()}
            }

            dynamodb_client.put_item(
                TableName=TABLES["CONFIG"],
                Item=item
            )

            metrics.add_metric(name="HeroBannerSaved", unit="Count", value=1)
            return {"message": "Banners saved", "count": len(banners), "banners": banners}, 200
        except Exception as e:
            logger.error("Error saving hero banners", exc_info=True)
            return {"error": str(e)}, 500

    @app.get("/api/v1/config/app-version")
    @tracer.capture_method
    def get_app_version_config():
        """Return minimum required app versions and store URLs for force-update checks."""
        try:
            item = _fetch_config_item("CONFIG#APP_VERSION", "MINIMUM")
            if not item:
                return {"minAppVersions": {}, "storeUrls": {}}, 200

            config = dynamodb_to_python(item.get("config", {"NULL": True}))
            if not isinstance(config, dict):
                return {"minAppVersions": {}, "storeUrls": {}}, 200

            return {
                "minAppVersions": config.get("minAppVersions", {}),
                "storeUrls": config.get("storeUrls", {}),
            }, 200
        except Exception as e:
            logger.error("Error fetching app version config", exc_info=True)
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

    @app.get("/api/v1/config/daily-deal-popup")
    @tracer.capture_method
    def get_daily_deal_popup():
        """Return the active daily deal popup payload for the customer app."""
        try:
            query_params = app.current_event.query_string_parameters or {}
            admin_mode = str(query_params.get("admin", "")).lower() == "true"

            item = _fetch_config_item("POPUP#DAILY_DEAL", "ACTIVE")
            if not item:
                return {"popup": None}, 200

            config = dynamodb_to_python(item.get("config", {"NULL": True}))
            if isinstance(config, str):
                try:
                    config = json.loads(config)
                except json.JSONDecodeError:
                    config = {}
            if not isinstance(config, dict):
                return {"popup": None}, 200

            popup = _normalize_daily_deal_popup(config)
            popup["id"] = str(popup.get("id") or now_ist_strftime("%Y-%m-%d"))

            if admin_mode:
                return {"popup": popup}, 200

            today = now_ist_strftime("%Y-%m-%d")
            if not _is_active_in_date(config, today):
                return {"popup": None}, 200

            return {"popup": popup}, 200
        except Exception as e:
            logger.error("Error fetching daily deal popup", exc_info=True)
            return {"error": str(e)}, 500

    @app.post("/api/v1/config/daily-deal-popup")
    @tracer.capture_method
    def save_daily_deal_popup():
        """Save/replace the daily deal popup payload."""
        try:
            import uuid as _uuid

            body = app.current_event.json_body or {}
            popup = body.get("popup") if isinstance(body.get("popup"), dict) else body
            if not isinstance(popup, dict):
                return {"error": "popup must be an object"}, 400

            content_type = str(popup.get("contentType") or "image").lower()
            if content_type not in {"image", "html", "video"}:
                return {"error": "contentType must be image, html, or video"}, 400

            normalized = _normalize_daily_deal_popup(popup)
            normalized["id"] = str(normalized.get("id") or _uuid.uuid4().hex[:8])
            normalized["isActive"] = _coerce_bool(normalized.get("isActive"), True)

            item = {
                "partitionkey": {"S": "POPUP#DAILY_DEAL"},
                "sortKey": {"S": "ACTIVE"},
                "config": python_to_dynamodb(normalized),
                "updatedAt": {"S": now_ist_iso()}
            }

            dynamodb_client.put_item(
                TableName=TABLES["CONFIG"],
                Item=item
            )

            metrics.add_metric(name="DailyDealPopupSaved", unit="Count", value=1)
            return {"message": "Daily deal popup saved", "popup": normalized}, 200
        except Exception as e:
            logger.error("Error saving daily deal popup", exc_info=True)
            return {"error": str(e)}, 500

    @app.get("/api/v1/config/theatre-show-timings")
    @tracer.capture_method
    def get_theatre_show_timings():
        """Return theatre show timings keyed by theatreName from QR/deep link."""
        try:
            query_params = app.current_event.query_string_parameters or {}
            theatre_name = str(
                query_params.get("theatreName") or query_params.get("theaterName") or ""
            ).strip()

            if not theatre_name:
                return {"error": "theatreName is required"}, 400

            # Preferred key: THEATRE#{name}. Fallback to raw name for manually
            # inserted legacy rows where theatreName itself was used as the PK.
            pk_candidates = [_theatre_config_pk(theatre_name), theatre_name]
            item = None
            used_pk = None
            for pk in pk_candidates:
                item = _fetch_config_item(pk, "SHOW_TIMINGS")
                if item:
                    used_pk = pk
                    break

            if not item:
                return {
                    "error": "Theatre show timings not found",
                    "theatreName": theatre_name,
                }, 404

            config = dynamodb_to_python(item.get("config", {"NULL": True}))
            if not isinstance(config, dict):
                return {"error": "Invalid theatre show timing config"}, 500

            return {
                "partitionkey": used_pk,
                "sortKey": "SHOW_TIMINGS",
                "theatreName": config.get("theatreName") or theatre_name,
                "config": config,
                "updatedAt": item.get("updatedAt", {}).get("S"),
            }, 200
        except Exception as e:
            logger.error("Error fetching theatre show timings", exc_info=True)
            return {"error": str(e)}, 500
