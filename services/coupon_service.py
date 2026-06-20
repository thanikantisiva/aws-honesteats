"""Coupon helpers shared across routes."""

from datetime import datetime, timezone
from typing import Optional

from aws_lambda_powertools import Logger

from config.pricing import round_nearest_half
from utils import normalize_phone
from utils.dynamodb import TABLES, dynamodb_client
from utils.dynamodb_helpers import dynamodb_to_python

logger = Logger()

CONFIG_PK = "CONFIG#GLOBAL"
CONFIG_SK = "CONFIG"


class CouponService:
    """Shared coupon lookup and discount helpers."""

    @staticmethod
    def _fetch_global_config() -> dict:
        """Read the global config row used by admin-managed coupon controls."""
        try:
            response = dynamodb_client.get_item(
                TableName=TABLES["CONFIG"],
                Key={
                    "partitionkey": {"S": CONFIG_PK},
                    "sortKey": {"S": CONFIG_SK},
                },
            )
            config = dynamodb_to_python(response.get("Item", {}).get("config", {"NULL": True}))
            return config if isinstance(config, dict) else {}
        except Exception as exc:
            logger.warning(f"Failed to fetch global coupon config: {exc}")
            return {}

    @staticmethod
    def _coupon_code_set(value) -> set:
        """Normalize coupon code config values to an uppercase set."""
        if isinstance(value, dict):
            for key in ("couponCodes", "codes", "blockedCoupons"):
                if key in value:
                    return CouponService._coupon_code_set(value.get(key))
            return set()

        if isinstance(value, str):
            raw_codes = value.replace("\n", ",").split(",")
        elif isinstance(value, (list, tuple, set)):
            raw_codes = value
        else:
            return set()

        return {
            str(code).strip().upper()
            for code in raw_codes
            if str(code or "").strip()
        }

    @staticmethod
    def get_blocked_coupon_codes_for_restaurant(
        restaurant_id: Optional[str],
        config: Optional[dict] = None,
    ) -> set:
        """Return globally configured coupon codes blocked for a restaurant.

        Supported global config shape:
        {
          "blockedCouponsByRestaurant": {
            "RES-123": ["SAVE20", "FREEDEL"]
          }
        }
        """
        normalized_restaurant_id = str(restaurant_id or "").strip()
        if not normalized_restaurant_id:
            return set()

        config_payload = config if isinstance(config, dict) else CouponService._fetch_global_config()
        field_names = (
            "blockedCouponsByRestaurant",
            "couponBlocklistByRestaurant",
            "restaurantCouponBlocklist",
        )

        for field_name in field_names:
            block_config = config_payload.get(field_name)
            if isinstance(block_config, dict):
                if normalized_restaurant_id in block_config:
                    return CouponService._coupon_code_set(block_config.get(normalized_restaurant_id))

                normalized_lookup = normalized_restaurant_id.lower()
                for key, value in block_config.items():
                    if str(key or "").strip().lower() == normalized_lookup:
                        return CouponService._coupon_code_set(value)

            if isinstance(block_config, list):
                for entry in block_config:
                    if not isinstance(entry, dict):
                        continue
                    entry_restaurant_id = str(
                        entry.get("restaurantId")
                        or entry.get("restaurant_id")
                        or entry.get("id")
                        or ""
                    ).strip()
                    if entry_restaurant_id.lower() == normalized_restaurant_id.lower():
                        return CouponService._coupon_code_set(entry)

        return set()

    @staticmethod
    def is_coupon_blocked_for_restaurant(
        coupon_code: Optional[str],
        restaurant_id: Optional[str],
        config: Optional[dict] = None,
    ) -> bool:
        normalized_code = str(coupon_code or "").strip().upper()
        if not normalized_code:
            return False
        return normalized_code in CouponService.get_blocked_coupon_codes_for_restaurant(
            restaurant_id,
            config,
        )

    @staticmethod
    def _parse_iso_or_date(value: Optional[str]):
        if not value:
            return None

        value = str(value).strip()
        if not value:
            return None

        try:
            if len(value) == 10 and value[4] == "-" and value[7] == "-":
                return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)

            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None

    @staticmethod
    def is_coupon_active(start_date: Optional[str], end_date: Optional[str]) -> bool:
        """Validate coupon active window if start/end are provided."""
        now = datetime.now(timezone.utc)
        parsed_start = CouponService._parse_iso_or_date(start_date) if start_date else None
        parsed_end = CouponService._parse_iso_or_date(end_date) if end_date else None

        if parsed_start and now < parsed_start:
            return False
        if parsed_end and now > parsed_end:
            return False
        return True

    @staticmethod
    def parse_coupon_item(normalized_code: str, item: Optional[dict]) -> Optional[dict]:
        """Parse a raw DynamoDB coupon item (typed format from GetItem/BatchGetItem) into a coupon dict."""
        if not item:
            return None

        coupon_type = item.get("couponType", {}).get("S") or item.get("type", {}).get("S")
        coupon_value_raw = item.get("couponValue", {}).get("N") or item.get("value", {}).get("N")
        if not coupon_type or coupon_value_raw is None:
            return None

        try:
            coupon_value = float(coupon_value_raw)
        except (TypeError, ValueError):
            return None

        return {
            "couponCode": normalized_code,
            "couponType": coupon_type,
            "couponValue": coupon_value,
            "startDate": item.get("startDate", {}).get("S"),
            "endDate": item.get("endDate", {}).get("S"),
            "issuedBy": item.get("issuedBy", {}).get("S"),
            "isOncePerUser": item.get("isOncePerUser", {}).get("BOOL", False),
            "isOncePerDay": item.get("isOncePerDay", {}).get("BOOL", False),
            "couponTarget": item.get("couponTarget", {}).get("S") or "delivery",
            "minOrderValue": float(item.get("minOrderValue", {}).get("N") or 0) or None,
            "couponRestaurant": item.get("couponRestaurant", {}).get("S"),
            "couponItem": item.get("couponItem", {}).get("S"),
            "couponItems": item.get("couponItems", {}).get("SS", []),
            "description": item.get("description", {}).get("S") or None,
            "targetCustomerPhones": item.get("targetCustomerPhones", {}).get("SS", []),
        }

    @staticmethod
    def create_dine_in_price_match_item_coupons(
        restaurant_id: str,
        item_banner_text: Optional[str] = None,
        coupon_type: str = "price_match",
        coupon_value: Optional[float] = None,
        issued_by: str = "YUMDUDE",
        description: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> dict:
        """Create item coupons in bulk for a restaurant.

        Coupon codes are deterministic per restaurant/item so repeated runs update
        the same coupon instead of generating duplicates.
        """
        from services.menu_service import MenuService

        normalized_restaurant_id = str(restaurant_id or "").strip()
        if not normalized_restaurant_id:
            raise ValueError("restaurantId is required")

        normalized_coupon_type = str(coupon_type or "price_match").strip().lower().replace("-", "_")
        if normalized_coupon_type in ("pricematch", "dine_in", "dine_in_price_match"):
            normalized_coupon_type = "price_match"
        if normalized_coupon_type not in ("price_match", "percentage", "fixed"):
            raise ValueError("couponType must be price_match, percentage, or fixed")

        normalized_issued_by = str(issued_by or "YUMDUDE").strip().upper()
        if normalized_issued_by not in ("YUMDUDE", "RESTAURANT"):
            raise ValueError("issuedBy must be YUMDUDE or RESTAURANT")

        fixed_coupon_value = None
        if normalized_coupon_type in ("percentage", "fixed"):
            try:
                fixed_coupon_value = float(coupon_value)
            except (TypeError, ValueError):
                raise ValueError("couponValue must be a number")
            if fixed_coupon_value <= 0:
                raise ValueError("couponValue must be greater than 0")
            if normalized_coupon_type == "percentage" and fixed_coupon_value > 100:
                raise ValueError("percentage couponValue cannot exceed 100")

        menu_items = [
            item for item in MenuService.list_menu_items(normalized_restaurant_id)
            if item.item_id and item.restaurant_price is not None
        ]

        created_items = []
        skipped_items = []

        for item in menu_items:
            restaurant_price = float(item.restaurant_price or 0.0)
            display_price = float(item.price)
            hiked_amount = round(display_price - restaurant_price, 2)

            if normalized_coupon_type == "price_match":
                applied_coupon_type = "fixed"
                applied_coupon_value = hiked_amount
                skip_reason = "No positive hiked amount"
            else:
                applied_coupon_type = normalized_coupon_type
                applied_coupon_value = fixed_coupon_value
                skip_reason = "Invalid coupon value"

            if display_price <= 0 or applied_coupon_value is None or applied_coupon_value <= 0:
                skipped_items.append({
                    "itemId": item.item_id,
                    "itemName": item.item_name,
                    "restaurantPrice": restaurant_price,
                    "displayPrice": display_price,
                    "reason": skip_reason,
                })
                continue

            if normalized_coupon_type == "price_match" and (restaurant_price <= 0 or hiked_amount <= 0):
                skipped_items.append({
                    "itemId": item.item_id,
                    "itemName": item.item_name,
                    "restaurantPrice": restaurant_price,
                    "displayPrice": display_price,
                    "reason": skip_reason,
                })
                continue

            coupon_code = f"YUMDINE-{normalized_restaurant_id}-{item.item_id}"
            coupon_item = {
                "partitionkey": {"S": f"COUPON#{coupon_code}"},
                "sortKey": {"S": "DETAILS"},
                "couponType": {"S": applied_coupon_type},
                "couponValue": {"N": str(applied_coupon_value)},
                "issuedBy": {"S": normalized_issued_by},
                "couponRestaurant": {"S": normalized_restaurant_id},
                "couponItem": {"S": item.item_id},
                "couponTarget": {"S": "item"},
                "isOncePerUser": {"BOOL": False},
                "isOncePerDay": {"BOOL": False},
                "description": {"S": str(description or f"Bulk item coupon funded by {normalized_issued_by}").strip()},
            }
            if start_date:
                coupon_item["startDate"] = {"S": str(start_date)}
            if end_date:
                coupon_item["endDate"] = {"S": str(end_date)}

            dynamodb_client.put_item(
                TableName=TABLES["CONFIG"],
                Item=coupon_item,
            )

            item_updates = {"itemOfferCouponCode": coupon_code}
            if item_banner_text:
                item_updates["topOfferBanner"] = item_banner_text
            MenuService.update_menu_item(normalized_restaurant_id, item.item_id, item_updates)

            created_items.append({
                "itemId": item.item_id,
                "itemName": item.item_name,
                "restaurantPrice": restaurant_price,
                "displayPrice": display_price,
                "hikedAmount": hiked_amount,
                "couponType": applied_coupon_type,
                "couponValue": applied_coupon_value,
                "issuedBy": normalized_issued_by,
                "couponCode": coupon_code,
            })

        return {
            "restaurantId": normalized_restaurant_id,
            "couponType": normalized_coupon_type,
            "couponValue": fixed_coupon_value,
            "issuedBy": normalized_issued_by,
            "createdCount": len(created_items),
            "skippedCount": len(skipped_items),
            "items": created_items,
            "skippedItems": skipped_items,
        }

    @staticmethod
    def get_coupon(coupon_code: Optional[str]) -> Optional[dict]:
        """Fetch a coupon record from the config table."""
        normalized_code = str(coupon_code or "").strip()
        if not normalized_code:
            return None

        response = dynamodb_client.get_item(
            TableName=TABLES["CONFIG"],
            Key={
                "partitionkey": {"S": f"COUPON#{normalized_code}"},
                "sortKey": {"S": "DETAILS"},
            }
        )
        return CouponService.parse_coupon_item(normalized_code, response.get("Item"))

    @staticmethod
    def is_coupon_valid_for_restaurant(coupon: Optional[dict], restaurant_id: Optional[str]) -> bool:
        """Validate whether a coupon applies to the given restaurant."""
        if not coupon:
            return False

        coupon_restaurant = str(coupon.get("couponRestaurant") or "").strip()
        if not coupon_restaurant:
            return True

        return coupon_restaurant == str(restaurant_id or "").strip()

    @staticmethod
    def is_coupon_valid_for_customer(coupon: Optional[dict], mobile_number: Optional[str]) -> bool:
        """Validate whether a coupon is public or targeted to this customer phone."""
        if not coupon:
            return False

        target_phones = set(coupon.get("targetCustomerPhones") or [])
        if not target_phones:
            return True

        normalized_mobile = normalize_phone(str(mobile_number).strip()) if mobile_number else None
        if not normalized_mobile:
            return False

        normalized_targets = {
            normalize_phone(str(phone).strip())
            for phone in target_phones
            if str(phone or "").strip()
        }
        return normalized_mobile in normalized_targets

    @staticmethod
    def get_item_coupon_discount(coupon_code, base_price, restaurant_id=None, item_id=None) -> dict:
        """Fetch an item-offer coupon by code and compute its discount against ``base_price``.

        Single source of truth for the item-coupon math: same validation
        (active / restaurant-valid / not-blocked / couponItem match / skip
        targeted) and the same percentage|fixed + round_nearest_half rules used
        for menu pricing — but the base price is supplied by the caller, so the
        revenue calc can re-derive a line's discount from its own price basis.

        Returns ``{discountAmount, discountedPrice, issuedBy, couponCode}``;
        a zero discount (issuedBy/couponCode = None) when the coupon is
        missing / expired / invalid / blocked / targeted / item-mismatched.
        """
        base_price = float(base_price or 0.0)
        none_result = {
            "discountAmount": 0.0,
            "discountedPrice": base_price,
            "issuedBy": None,
            "couponCode": None,
        }

        coupon = CouponService.get_coupon(coupon_code)
        if not coupon:
            return none_result
        if not CouponService.is_coupon_active(coupon.get("startDate"), coupon.get("endDate")):
            return none_result
        if not CouponService.is_coupon_valid_for_restaurant(coupon, restaurant_id):
            return none_result
        if CouponService.is_coupon_blocked_for_restaurant(coupon.get("couponCode"), restaurant_id):
            return none_result

        coupon_item = str(coupon.get("couponItem") or "").strip()
        coupon_items = {
            str(value or "").strip()
            for value in (coupon.get("couponItems") or [])
            if str(value or "").strip()
        }
        normalized_item_id = str(item_id or "").strip()
        if coupon_item and coupon_item != normalized_item_id:
            return none_result
        if coupon_items and normalized_item_id not in coupon_items:
            return none_result

        # No customer identity in this path, so targeted coupons must not apply.
        if coupon.get("targetCustomerPhones"):
            return none_result

        coupon_type = str(coupon.get("couponType") or "").strip().lower()
        coupon_value = float(coupon.get("couponValue") or 0.0)
        if coupon_type == "percentage":
            discounted_price = base_price * (1 - (coupon_value / 100.0))
        elif coupon_type == "fixed":
            discounted_price = base_price - coupon_value
        else:
            logger.info(f"Unsupported menu coupon type: {coupon_type}")
            return none_result

        discounted_price = round_nearest_half(max(0.0, discounted_price))
        if discounted_price >= base_price:
            return none_result

        return {
            "discountAmount": round(base_price - discounted_price, 2),
            "discountedPrice": discounted_price,
            "issuedBy": coupon.get("issuedBy"),
            "couponCode": coupon.get("couponCode"),
        }

    @staticmethod
    def get_menu_item_prices(menu_item) -> dict:
        """Return authoritative menu pricing, including coupon discount when valid."""
        base_price = float(menu_item.price)
        disc = CouponService.get_item_coupon_discount(
            getattr(menu_item, "item_offer_coupon_code", None),
            base_price,
            getattr(menu_item, "restaurant_id", None),
            getattr(menu_item, "item_id", None),
        )

        if disc["discountAmount"] <= 0:
            return {
                "price": base_price,
                "originalPrice": None,
                "grossPrice": base_price,
                "discountAmount": 0.0,
                "couponCode": None,
                "couponIssuedBy": None,
            }

        return {
            "price": disc["discountedPrice"],
            "originalPrice": base_price,
            "grossPrice": base_price,
            "discountAmount": disc["discountAmount"],
            "couponCode": disc["couponCode"],
            "couponIssuedBy": disc["issuedBy"],
        }
