"""Coupon helpers shared across routes."""

from datetime import datetime, timezone
from typing import Optional

from aws_lambda_powertools import Logger

from config.pricing import round_nearest_half
from utils.dynamodb import TABLES, dynamodb_client

logger = Logger()


class CouponService:
    """Shared coupon lookup and discount helpers."""

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
        item = response.get("Item")
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
            "couponRestaurant": item.get("couponRestaurant", {}).get("S"),
            "couponItem": item.get("couponItem", {}).get("S"),
            "description": item.get("description", {}).get("S") or None,
        }

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
    def get_menu_item_prices(menu_item) -> dict:
        """Return authoritative menu pricing, including coupon discount when valid."""
        base_price = float(menu_item.price)
        coupon_code = getattr(menu_item, "item_offer_coupon_code", None)
        coupon = CouponService.get_coupon(coupon_code)

        default_result = {
            "price": base_price,
            "originalPrice": None,
            "grossPrice": base_price,
            "discountAmount": 0.0,
            "couponCode": None,
            "couponIssuedBy": None,
        }

        if not coupon:
            return default_result

        if not CouponService.is_coupon_active(coupon.get("startDate"), coupon.get("endDate")):
            return default_result

        coupon_type = str(coupon.get("couponType") or "").strip().lower()
        coupon_value = float(coupon.get("couponValue") or 0.0)

        if coupon_type == "percentage":
            discounted_price = base_price * (1 - (coupon_value / 100.0))
        elif coupon_type == "fixed":
            discounted_price = base_price - coupon_value
        else:
            logger.info(f"Unsupported menu coupon type: {coupon_type}")
            return default_result

        discounted_price = round_nearest_half(max(0.0, discounted_price))
        if discounted_price >= base_price:
            return default_result

        return {
            "price": discounted_price,
            "originalPrice": base_price,
            "grossPrice": base_price,
            "discountAmount": round(base_price - discounted_price, 2),
            "couponCode": coupon.get("couponCode"),
            "couponIssuedBy": coupon.get("issuedBy"),
        }
