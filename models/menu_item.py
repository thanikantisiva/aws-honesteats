"""Menu item model"""
from typing import Optional, List, Union


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
    def _round_nearest_half(value: float) -> float:
        """Round to nearest 0.5 (e.g. 67.8 -> 68, 67.3 -> 67.5)."""
        return round(value * 2) / 2

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
        sub_category: Optional[str] = None
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

    @property
    def price(self) -> float:
        """Customer-facing price: restaurantPrice * (1 + hikePercentage/100), rounded to nearest 0.5."""
        raw = self.restaurant_price * (1 + self.hike_percentage / 100)
        return self._round_nearest_half(raw)

    @property
    def pk(self) -> str:
        """Get partition key"""
        return f"RESTAURANT#{self.restaurant_id}"

    @property
    def sk(self) -> str:
        """Get sort key"""
        return f"ITEM#{self.item_id}"

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        result = {
            "restaurant_id": self.restaurant_id,
            "itemId": self.item_id,
            "itemName": self.item_name,
            "price": self.price,
            "restaurantPrice": self.restaurant_price,
            "hikePercentage": self.hike_percentage,
            "isAvailable": self.is_available
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
        return result

    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "MenuItem":
        """Create MenuItem from DynamoDB item. Reads only restaurantPrice and hikePercentage; does not use price."""
        pk = item.get("PK", {}).get("S", "")
        sk = item.get("SK", {}).get("S", "")

        restaurant_id = pk.replace("RESTAURANT#", "") if pk.startswith("RESTAURANT#") else ""
        item_id = sk.replace("ITEM#", "") if sk.startswith("ITEM#") else ""

        restaurant_price = float(item.get("restaurantPrice", {}).get("N", "0"))
        hike_percentage = float(item.get("hikePercentage", {}).get("N", "0")) if "hikePercentage" in item else 0.0

        image = None
        if "image" in item:
            image_attr = item.get("image", {})
            if "L" in image_attr:
                image = [img.get("S", "") for img in image_attr["L"] if img.get("S")]
            elif "S" in image_attr:
                image = image_attr.get("S")

        return cls(
            restaurant_id=restaurant_id,
            item_id=item_id,
            item_name=item.get("itemName", {}).get("S", ""),
            restaurant_price=restaurant_price,
            hike_percentage=hike_percentage,
            category=item.get("category", {}).get("S") if "category" in item else None,
            sub_category=item.get("subCategory", {}).get("S") if "subCategory" in item else None,
            is_available=item.get("isAvailable", {}).get("BOOL", True) if "isAvailable" in item else True,
            is_veg=item.get("isVeg", {}).get("BOOL") if "isVeg" in item else None,
            description=item.get("description", {}).get("S") if "description" in item else None,
            image=image
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
        return item
