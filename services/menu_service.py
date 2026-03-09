"""Menu service"""
from typing import List, Optional
from botocore.exceptions import ClientError
from models.menu_item import MenuItem
from utils.dynamodb import dynamodb_client, TABLES
from aws_lambda_powertools import Logger

logger = Logger()


class MenuService:
    """Service for menu item operations"""

    @staticmethod
    def _normalize_image_list(value):
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value if v is not None and str(v).strip()]
        if isinstance(value, str) and value.strip():
            return [value]
        return []
    
    @staticmethod
    def get_menu_item(restaurant_id: str, item_id: str) -> Optional[MenuItem]:
        """Get menu item by restaurant ID and item ID"""
        try:
            pk = f"RESTAURANT#{restaurant_id}"
            sk = f"ITEM#{item_id}"
            
            response = dynamodb_client.get_item(
                TableName=TABLES['MENU_ITEMS'],
                Key={
                    'PK': {'S': pk},
                    'SK': {'S': sk}
                }
            )
            
            if 'Item' not in response:
                return None
            
            return MenuItem.from_dynamodb_item(response['Item'])
        except ClientError as e:
            raise Exception(f"Failed to get menu item: {str(e)}")
    
    @staticmethod
    def create_menu_item(menu_item: MenuItem) -> MenuItem:
        """Create a new menu item"""
        try:
            dynamodb_client.put_item(
                TableName=TABLES['MENU_ITEMS'],
                Item=menu_item.to_dynamodb_item()
            )
            return menu_item
        except ClientError as e:
            raise Exception(f"Failed to create menu item: {str(e)}")
    
    @staticmethod
    def list_menu_items(restaurant_id: str) -> List[MenuItem]:
        """List all menu items for a restaurant"""
        try:
            pk = f"RESTAURANT#{restaurant_id}"
            response = dynamodb_client.query(
                TableName=TABLES['MENU_ITEMS'],
                KeyConditionExpression='PK = :pk',
                ExpressionAttributeValues={
                    ':pk': {'S': pk}
                }
            )
            
            menu_items = []
            for item in response.get('Items', []):
                menu_items.append(MenuItem.from_dynamodb_item(item))
            
            return menu_items
        except ClientError as e:
            raise Exception(f"Failed to list menu items: {str(e)}")
    
    @staticmethod
    def update_menu_item(restaurant_id: str, item_id: str, updates: dict) -> MenuItem:
        """Update menu item"""
        try:
            pk = f"RESTAURANT#{restaurant_id}"
            sk = f"ITEM#{item_id}"
            
            update_expressions = []
            expression_attribute_names = {}
            expression_attribute_values = {}
            
            if 'name' in updates or 'itemName' in updates:
                update_expressions.append('#itemName = :itemName')
                expression_attribute_names['#itemName'] = 'itemName'
                name_value = updates.get('name') or updates.get('itemName')
                expression_attribute_values[':itemName'] = {'S': name_value}
            
            if 'restaurantPrice' in updates:
                update_expressions.append('restaurantPrice = :restaurantPrice')
                expression_attribute_values[':restaurantPrice'] = {'N': str(updates['restaurantPrice'])}
            if 'hikePercentage' in updates:
                update_expressions.append('hikePercentage = :hikePercentage')
                expression_attribute_values[':hikePercentage'] = {'N': str(updates['hikePercentage'])}
            
            if 'category' in updates:
                update_expressions.append('#category = :category')
                expression_attribute_names['#category'] = 'category'
                expression_attribute_values[':category'] = {'S': updates['category']}

            if 'subCategory' in updates:
                update_expressions.append('#subCategory = :subCategory')
                expression_attribute_names['#subCategory'] = 'subCategory'
                expression_attribute_values[':subCategory'] = {'S': updates['subCategory']}
            
            if 'isVeg' in updates:
                update_expressions.append('#isVeg = :isVeg')
                expression_attribute_names['#isVeg'] = 'isVeg'
                expression_attribute_values[':isVeg'] = {'BOOL': updates['isVeg']}
            
            if 'isAvailable' in updates:
                update_expressions.append('#isAvailable = :isAvailable')
                expression_attribute_names['#isAvailable'] = 'isAvailable'
                expression_attribute_values[':isAvailable'] = {'BOOL': updates['isAvailable']}
            
            if 'description' in updates:
                update_expressions.append('#description = :description')
                expression_attribute_names['#description'] = 'description'
                expression_attribute_values[':description'] = {'S': updates['description']}
            
            if 'image' in updates:
                normalized_images = MenuService._normalize_image_list(updates['image'])
                update_expressions.append('#image = :image')
                expression_attribute_names['#image'] = 'image'
                expression_attribute_values[':image'] = {
                    'L': [{'S': img} for img in normalized_images]
                }
            
            if not update_expressions:
                return MenuService.get_menu_item(restaurant_id, item_id)
            
            dynamodb_client.update_item(
                TableName=TABLES['MENU_ITEMS'],
                Key={
                    'PK': {'S': pk},
                    'SK': {'S': sk}
                },
                UpdateExpression=f"SET {', '.join(update_expressions)}",
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            
            return MenuService.get_menu_item(restaurant_id, item_id)
        except ClientError as e:
            raise Exception(f"Failed to update menu item: {str(e)}")
    
    @staticmethod
    def delete_menu_item(restaurant_id: str, item_id: str) -> None:
        """Delete a menu item"""
        try:
            pk = f"RESTAURANT#{restaurant_id}"
            sk = f"ITEM#{item_id}"
            
            dynamodb_client.delete_item(
                TableName=TABLES['MENU_ITEMS'],
                Key={
                    'PK': {'S': pk},
                    'SK': {'S': sk}
                }
            )
        except ClientError as e:
            raise Exception(f"Failed to delete menu item: {str(e)}")

    @staticmethod
    def increment_ordered_count(restaurant_id: str, items: List[dict]) -> None:
        """Increment orderedCount for each delivered order item."""
        if not items:
            return

        # Merge increments by itemId to avoid multiple updates for same item in one order.
        increments = {}
        for item in items:
            item_id = item.get("itemId")
            if not item_id:
                continue
            quantity = item.get("quantity", 1)
            try:
                quantity = int(quantity)
            except (TypeError, ValueError):
                quantity = 1
            if quantity < 1:
                quantity = 1
            increments[item_id] = increments.get(item_id, 0) + quantity

        for item_id, qty in increments.items():
            try:
                dynamodb_client.update_item(
                    TableName=TABLES["MENU_ITEMS"],
                    Key={
                        "PK": {"S": f"RESTAURANT#{restaurant_id}"},
                        "SK": {"S": f"ITEM#{item_id}"}
                    },
                    UpdateExpression="ADD orderedCount :inc",
                    ExpressionAttributeValues={
                        ":inc": {"N": str(qty)}
                    }
                )
                logger.info(f"Incremented orderedCount for restaurantId={restaurant_id} itemId={item_id} by {qty}")
            except ClientError as e:
                logger.error(f"Failed increment orderedCount for itemId={item_id}: {str(e)}")
