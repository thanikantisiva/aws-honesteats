"""Menu service"""
from typing import List, Optional
from botocore.exceptions import ClientError
from models.menu_item import MenuItem
from utils.dynamodb import dynamodb_client, TABLES


class MenuService:
    """Service for menu item operations"""
    
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
            
            if 'price' in updates:
                update_expressions.append('#price = :price')
                expression_attribute_names['#price'] = 'price'
                expression_attribute_values[':price'] = {'N': str(updates['price'])}
            
            if 'category' in updates:
                update_expressions.append('#category = :category')
                expression_attribute_names['#category'] = 'category'
                expression_attribute_values[':category'] = {'S': updates['category']}
            
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
                update_expressions.append('#image = :image')
                expression_attribute_names['#image'] = 'image'
                expression_attribute_values[':image'] = {'S': updates['image']}
            
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
