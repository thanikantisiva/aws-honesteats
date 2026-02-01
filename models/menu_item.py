"""Menu item model"""
from typing import Optional


class MenuItem:
    """Restaurant menu item model"""
    
    def __init__(
        self,
        restaurant_id: str,
        item_id: str,
        item_name: str,
        price: float,
        restaurant_price: Optional[float] = None,
        category: Optional[str] = None,
        is_available: bool = True,
        is_veg: Optional[bool] = None,
        description: Optional[str] = None,
        image: Optional[str] = None
    ):
        self.restaurant_id = restaurant_id
        self.item_id = item_id
        self.item_name = item_name
        self.price = price  # Customer-facing price
        self.restaurant_price = restaurant_price if restaurant_price is not None else price  # Restaurant's price
        self.category = category
        self.is_available = is_available
        self.is_veg = is_veg
        self.description = description
        self.image = image
    
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
            "price": self.price,  # Customer pays this
            "restaurantPrice": self.restaurant_price,  # Restaurant gets this
            "isAvailable": self.is_available
        }
        if self.category:
            result["category"] = self.category
        if self.is_veg is not None:
            result["isVeg"] = self.is_veg
        if self.description:
            result["description"] = self.description
        if self.image:
            result["image"] = self.image
        return result
    
    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "MenuItem":
        """Create MenuItem from DynamoDB item"""
        # Extract restaurantId and itemId from PK and SK
        pk = item.get("PK", {}).get("S", "")
        sk = item.get("SK", {}).get("S", "")
        
        restaurant_id = pk.replace("RESTAURANT#", "") if pk.startswith("RESTAURANT#") else ""
        item_id = sk.replace("ITEM#", "") if sk.startswith("ITEM#") else ""
        
        price = float(item.get("price", {}).get("N", "0"))
        # restaurantPrice might not exist in old items, default to price
        restaurant_price = float(item.get("restaurantPrice", {}).get("N")) if "restaurantPrice" in item else price
        
        return cls(
            restaurant_id=restaurant_id,
            item_id=item_id,
            item_name=item.get("itemName", {}).get("S", ""),
            price=price,
            restaurant_price=restaurant_price,
            category=item.get("category", {}).get("S") if "category" in item else None,
            is_available=item.get("isAvailable", {}).get("BOOL", True) if "isAvailable" in item else True,
            is_veg=item.get("isVeg", {}).get("BOOL") if "isVeg" in item else None,
            description=item.get("description", {}).get("S") if "description" in item else None,
            image=item.get("image", {}).get("S") if "image" in item else None
        )
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format"""
        item = {
            "PK": {"S": self.pk},
            "SK": {"S": self.sk},
            "itemId": {"S": self.item_id},
            "itemName": {"S": self.item_name},
            "price": {"N": str(self.price)},
            "restaurantPrice": {"N": str(self.restaurant_price)},  # Store restaurant price
            "isAvailable": {"BOOL": self.is_available}
        }
        if self.category:
            item["category"] = {"S": self.category}
        if self.is_veg is not None:
            item["isVeg"] = {"BOOL": self.is_veg}
        if self.description:
            item["description"] = {"S": self.description}
        if self.image:
            item["image"] = {"S": self.image}
        return item
