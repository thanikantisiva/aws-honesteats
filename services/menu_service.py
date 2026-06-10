"""Menu service"""
from typing import List, Optional
from botocore.exceptions import ClientError
from models.menu_item import MenuItem
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import python_to_dynamodb
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
            menu_items = []
            query_kwargs = {
                'TableName': TABLES['MENU_ITEMS'],
                'KeyConditionExpression': 'PK = :pk',
                'ExpressionAttributeValues': {
                    ':pk': {'S': pk}
                }
            }

            while True:
                response = dynamodb_client.query(**query_kwargs)
                for item in response.get('Items', []):
                    menu_items.append(MenuItem.from_dynamodb_item(item))

                last_key = response.get('LastEvaluatedKey')
                if not last_key:
                    break
                query_kwargs['ExclusiveStartKey'] = last_key
            
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

            if 'addOnOptions' in updates:
                update_expressions.append('addOnOptions = :addOnOptions')
                expression_attribute_values[':addOnOptions'] = python_to_dynamodb(updates['addOnOptions'])

            if 'shiftTimings' in updates:
                update_expressions.append('#shiftTimings = :shiftTimings')
                expression_attribute_names['#shiftTimings'] = 'shiftTimings'
                expression_attribute_values[':shiftTimings'] = python_to_dynamodb(updates['shiftTimings'])

            if 'topOfferBanner' in updates:
                expression_attribute_names['#topOfferBanner'] = 'topOfferBanner'
                if updates['topOfferBanner'] is None:
                    pass
                else:
                    update_expressions.append('#topOfferBanner = :topOfferBanner')
                    expression_attribute_values[':topOfferBanner'] = {'S': str(updates['topOfferBanner'])}

            if 'itemOfferCouponCode' in updates:
                expression_attribute_names['#itemOfferCouponCode'] = 'itemOfferCouponCode'
                if updates['itemOfferCouponCode'] is None:
                    pass
                else:
                    update_expressions.append('#itemOfferCouponCode = :itemOfferCouponCode')
                    expression_attribute_values[':itemOfferCouponCode'] = {'S': str(updates['itemOfferCouponCode'])}

            if 'theaterMode' in updates:
                update_expressions.append('theaterMode = :theaterMode')
                expression_attribute_values[':theaterMode'] = {'BOOL': bool(updates['theaterMode'])}

            if 'inventoryCount' in updates:
                update_expressions.append('inventoryCount = :inventoryCount')
                expression_attribute_values[':inventoryCount'] = {'N': str(int(updates['inventoryCount']))}

            set_expressions = [expr for expr in update_expressions if expr]
            remove_expressions = []
            if 'topOfferBanner' in updates and updates['topOfferBanner'] is None:
                remove_expressions.append('#topOfferBanner')
            if 'itemOfferCouponCode' in updates and updates['itemOfferCouponCode'] is None:
                remove_expressions.append('#itemOfferCouponCode')

            if not set_expressions and not remove_expressions:
                return MenuService.get_menu_item(restaurant_id, item_id)
            
            update_kwargs = {
                'TableName': TABLES['MENU_ITEMS'],
                'Key': {
                    'PK': {'S': pk},
                    'SK': {'S': sk}
                },
                'UpdateExpression': " ".join(
                    part for part in [
                        f"SET {', '.join(set_expressions)}" if set_expressions else "",
                        f"REMOVE {', '.join(remove_expressions)}" if remove_expressions else ""
                    ] if part
                ),
                'ExpressionAttributeNames': expression_attribute_names,
            }

            if expression_attribute_names:
                update_kwargs['ExpressionAttributeNames'] = expression_attribute_names
            elif 'ExpressionAttributeNames' in update_kwargs:
                del update_kwargs['ExpressionAttributeNames']

            if expression_attribute_values:
                update_kwargs['ExpressionAttributeValues'] = expression_attribute_values

            dynamodb_client.update_item(**update_kwargs)
            
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

    @staticmethod
    def decrement_inventory(restaurant_id: str, item_id: str, quantity: int) -> bool:
        """Atomically decrement inventoryCount for a theater item.

        Uses a DynamoDB conditional update so we never oversell:
        only succeeds when inventoryCount >= quantity AND theaterMode = true.

        Returns True on success, False if the condition fails (sold out / race).
        """
        if quantity <= 0:
            return True
        try:
            dynamodb_client.update_item(
                TableName=TABLES["MENU_ITEMS"],
                Key={
                    "PK": {"S": f"RESTAURANT#{restaurant_id}"},
                    "SK": {"S": f"ITEM#{item_id}"},
                },
                UpdateExpression="ADD inventoryCount :neg",
                ConditionExpression="theaterMode = :true AND inventoryCount >= :qty",
                ExpressionAttributeValues={
                    ":neg": {"N": str(-int(quantity))},
                    ":qty": {"N": str(int(quantity))},
                    ":true": {"BOOL": True},
                },
            )
            logger.info(
                f"Decremented inventoryCount for restaurantId={restaurant_id} "
                f"itemId={item_id} by {quantity}"
            )
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                logger.warning(
                    f"Inventory decrement rejected (sold out / not theater) for "
                    f"restaurantId={restaurant_id} itemId={item_id} qty={quantity}"
                )
                return False
            raise Exception(f"Failed to decrement inventory: {str(e)}")

    @staticmethod
    def restock_inventory(restaurant_id: str, item_id: str, quantity: int) -> None:
        """Atomically add back inventoryCount (used on cancel / refund / partial-fail restock)."""
        if quantity <= 0:
            return
        try:
            dynamodb_client.update_item(
                TableName=TABLES["MENU_ITEMS"],
                Key={
                    "PK": {"S": f"RESTAURANT#{restaurant_id}"},
                    "SK": {"S": f"ITEM#{item_id}"},
                },
                UpdateExpression="ADD inventoryCount :inc",
                ExpressionAttributeValues={":inc": {"N": str(int(quantity))}},
            )
            logger.info(
                f"Restocked inventoryCount for restaurantId={restaurant_id} "
                f"itemId={item_id} by {quantity}"
            )
        except ClientError as e:
            logger.error(
                f"Failed to restock inventory for itemId={item_id}: {str(e)}"
            )

    @staticmethod
    def generate_pickup_token(restaurant_id: str) -> str:
        """Atomically generate a daily-rolling pickup token per restaurant.

        Uses a counter row at PK=RESTAURANT#{id}, SK=COUNTER#PICKUP#YYYYMMDD
        on the MENU_ITEMS table (already shares the RESTAURANT# partition).
        Token format: A###  (e.g. A042). Resets daily per restaurant.
        """
        from datetime import datetime
        try:
            today = datetime.utcnow().strftime("%Y%m%d")
            response = dynamodb_client.update_item(
                TableName=TABLES["MENU_ITEMS"],
                Key={
                    "PK": {"S": f"RESTAURANT#{restaurant_id}"},
                    "SK": {"S": f"COUNTER#PICKUP#{today}"},
                },
                UpdateExpression="ADD #count :inc",
                ExpressionAttributeNames={"#count": "count"},
                ExpressionAttributeValues={":inc": {"N": "1"}},
                ReturnValues="UPDATED_NEW",
            )
            current = int(response.get("Attributes", {}).get("count", {}).get("N", "0"))
            token = f"A{current:03d}"
            logger.info(
                f"Generated pickupToken={token} for restaurantId={restaurant_id} today={today}"
            )
            return token
        except ClientError as e:
            logger.error(f"Failed to generate pickup token: {str(e)}")
            # Fallback to a timestamp-derived token; still readable but uncoordinated
            import time
            return f"A{int(time.time()) % 1000:03d}"

    @staticmethod
    def bulk_price_hike(restaurant_id: str, percentage: float) -> list:
        """Increase restaurantPrice of all valid menu items for a restaurant by the given percentage.

        Returns a list of dicts with itemId, itemName, oldPrice, newPrice for each updated item.
        """
        try:
            items = MenuService.list_menu_items(restaurant_id)
            valid_items = [
                item for item in items
                if item.item_id and item.restaurant_price is not None and item.restaurant_price > 0
            ]

            multiplier = 1 + (percentage / 100)
            results = []

            for item in valid_items:
                old_price = item.restaurant_price
                new_price = round(old_price * multiplier, 2)

                dynamodb_client.update_item(
                    TableName=TABLES['MENU_ITEMS'],
                    Key={
                        'PK': {'S': f'RESTAURANT#{restaurant_id}'},
                        'SK': {'S': f'ITEM#{item.item_id}'}
                    },
                    UpdateExpression='SET restaurantPrice = :newPrice',
                    ExpressionAttributeValues={
                        ':newPrice': {'N': str(new_price)}
                    }
                )

                results.append({
                    'itemId': item.item_id,
                    'itemName': item.item_name,
                    'oldPrice': old_price,
                    'newPrice': new_price
                })

            return results
        except ClientError as e:
            raise Exception(f"Failed to bulk hike prices: {str(e)}")
