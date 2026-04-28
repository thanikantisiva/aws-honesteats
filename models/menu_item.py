"""Menu item model"""
from typing import Any, Dict, List, Optional, Union
from config.pricing import calculate_customer_price_from_hike
from utils.dynamodb_helpers import dynamodb_to_python, python_to_dynamodb


class MenuItem:
    """Restaurant menu item model"""

    @staticmethod
    def _normalize_image_list(value: Optional[Union[str, List[str]]]) -> List[str]:
        """Normalize image field to a list for backward compatibility."""
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value if v is not None and str(v).strip()]
        if isinstance(value, str) and value.strip():
            return [value]
        return []

    @staticmethod
    def _normalize_add_on_options(value: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Normalize add-on options to a safe list format."""
        if not isinstance(value, list):
            return []

        normalized: List[Dict[str, Any]] = []
        for option in value:
            if not isinstance(option, dict):
                continue
            name = str(option.get("name", "")).strip()
            if not name:
                continue
            option_id = str(option.get("optionId", "")).strip()
            if not option_id:
                option_id = f"addon_{len(normalized) + 1}"
            extra_price_raw = option.get("extraPrice", 0)
            try:
                extra_price = float(extra_price_raw)
            except (TypeError, ValueError):
                extra_price = 0.0
            normalized.append(
                {
                    "optionId": option_id,
                    "name": name,
                    "extraPrice": extra_price,
                }
            )
        return normalized

    @staticmethod
    def _to_python_attr(value: Any) -> Any:
        """Convert DynamoDB-typed attribute or passthrough plain value."""
        if isinstance(value, dict) and any(k in value for k in ("S", "N", "BOOL", "NULL", "L", "M")):
            return dynamodb_to_python(value)
        return value

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def __init__(
        self,
        restaurant_id: str,
        item_id: str,
        item_name: str,
        restaurant_price: float,
        hike_percentage: Optional[float] = 0,
        category: Optional[str] = None,
        is_available: bool = True,
        is_veg: Optional[bool] = None,
        description: Optional[str] = None,
        image: Optional[Union[str, List[str]]] = None,
        add_on_options: Optional[List[Dict[str, Any]]] = None,
        sub_category: Optional[str] = None,
        ordered_count: int = 0,
        top_offer_banner: Optional[str] = None,
        item_offer_coupon_code: Optional[str] = None,
        shift_timings: Optional[List[Dict[str, Any]]] = None
    ):
        self.restaurant_id = restaurant_id
        self.item_id = item_id
        self.item_name = item_name
        self.restaurant_price = float(restaurant_price)
        self.hike_percentage = float(hike_percentage) if hike_percentage is not None else 0.0
        self.category = category
        self.sub_category = sub_category
        self.is_available = is_available
        self.is_veg = is_veg
        self.description = description
        self.image = self._normalize_image_list(image)
        self.add_on_options = self._normalize_add_on_options(add_on_options)
        self.ordered_count = int(ordered_count or 0)
        self.top_offer_banner = top_offer_banner
        self.item_offer_coupon_code = item_offer_coupon_code
        self.shift_timings = shift_timings or []

    @property
    def price(self) -> float:
        """Customer-facing price: restaurantPrice * (1 + hikePercentage/100), rounded to nearest 0.5."""
        return calculate_customer_price_from_hike(self.restaurant_price, self.hike_percentage)

    @property
    def pk(self) -> str:
        """Get partition key"""
        return f"RESTAURANT#{self.restaurant_id}"

    @property
    def sk(self) -> str:
        """Get sort key"""
        return f"ITEM#{self.item_id}"

    def to_dict(self, price: Optional[float] = None, original_price: Optional[float] = None) -> dict:
        """Convert to dictionary"""
        resolved_price = float(self.price if price is None else price)
        result = {
            "restaurant_id": self.restaurant_id,
            "itemId": self.item_id,
            "itemName": self.item_name,
            "price": resolved_price,
            "restaurantPrice": self.restaurant_price,
            "hikePercentage": self.hike_percentage,
            "isAvailable": self.is_available,
            "orderedCount": self.ordered_count
        }
        if self.category:
            result["category"] = self.category
        if self.sub_category:
            result["subCategory"] = self.sub_category
        if self.is_veg is not None:
            result["isVeg"] = self.is_veg
        if self.description:
            result["description"] = self.description
        if self.image:
            result["image"] = self.image
        result["addOnOptions"] = self.add_on_options
        if self.top_offer_banner:
            result["topOfferBanner"] = self.top_offer_banner
        if self.item_offer_coupon_code:
            result["itemOfferCouponCode"] = self.item_offer_coupon_code
        if self.shift_timings:
            result["shiftTimings"] = self.shift_timings
        if original_price is not None and float(original_price) > resolved_price:
            result["originalPrice"] = float(original_price)
        return result

    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "MenuItem":
        """Create MenuItem from DynamoDB item. Reads only restaurantPrice and hikePercentage; does not use price."""
        pk = cls._to_python_attr(item.get("PK", ""))
        sk = cls._to_python_attr(item.get("SK", ""))

        restaurant_id = str(pk).replace("RESTAURANT#", "") if isinstance(pk, str) and pk.startswith("RESTAURANT#") else ""
        item_id = str(sk).replace("ITEM#", "") if isinstance(sk, str) and sk.startswith("ITEM#") else ""
        if not item_id:
            item_id = str(cls._to_python_attr(item.get("itemId", "")) or "")

        restaurant_price = cls._safe_float(cls._to_python_attr(item.get("restaurantPrice", 0)))
        hike_percentage = cls._safe_float(cls._to_python_attr(item.get("hikePercentage", 0.0)))

        image = cls._to_python_attr(item.get("image"))

        top_offer_banner = cls._to_python_attr(item.get("topOfferBanner"))
        item_offer_coupon_code = cls._to_python_attr(item.get("itemOfferCouponCode"))
        add_on_options_raw = cls._to_python_attr(item.get("addOnOptions", []))
        add_on_options = add_on_options_raw if isinstance(add_on_options_raw, list) else []

        item_name = cls._to_python_attr(item.get("itemName", "")) or ""
        category = cls._to_python_attr(item.get("category"))
        sub_category = cls._to_python_attr(item.get("subCategory"))
        is_available_attr = cls._to_python_attr(item.get("isAvailable"))
        is_available = True if is_available_attr is None else bool(is_available_attr)
        is_veg = cls._to_python_attr(item.get("isVeg"))
        description = cls._to_python_attr(item.get("description"))
        ordered_count = int(cls._safe_float(cls._to_python_attr(item.get("orderedCount", 0)), 0))

        return cls(
            restaurant_id=restaurant_id,
            item_id=item_id,
            item_name=item_name,
            restaurant_price=restaurant_price,
            hike_percentage=hike_percentage,
            category=category,
            sub_category=sub_category,
            is_available=is_available,
            is_veg=is_veg,
            description=description,
            image=image,
            add_on_options=add_on_options,
            ordered_count=ordered_count,
            top_offer_banner=top_offer_banner,
            item_offer_coupon_code=item_offer_coupon_code,
            shift_timings=cls._to_python_attr(item.get("shiftTimings")) if "shiftTimings" in item else []
        )

    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format. Writes only restaurantPrice and hikePercentage; no price."""
        item = {
            "PK": {"S": self.pk},
            "SK": {"S": self.sk},
            "itemId": {"S": self.item_id},
            "itemName": {"S": self.item_name},
            "restaurantPrice": {"N": str(self.restaurant_price)},
            "hikePercentage": {"N": str(self.hike_percentage)},
            "isAvailable": {"BOOL": self.is_available}
        }
        if self.category:
            item["category"] = {"S": self.category}
        if self.sub_category:
            item["subCategory"] = {"S": self.sub_category}
        if self.is_veg is not None:
            item["isVeg"] = {"BOOL": self.is_veg}
        if self.description:
            item["description"] = {"S": self.description}
        if self.image:
            item["image"] = {"L": [{"S": img} for img in self.image]}
        if self.add_on_options:
            item["addOnOptions"] = python_to_dynamodb(self.add_on_options)
        item["orderedCount"] = {"N": str(self.ordered_count)}
        if self.top_offer_banner:
            item["topOfferBanner"] = {"S": self.top_offer_banner}
        if self.item_offer_coupon_code:
            item["itemOfferCouponCode"] = {"S": self.item_offer_coupon_code}
        if self.shift_timings:
            item["shiftTimings"] = python_to_dynamodb(self.shift_timings)
        return item
