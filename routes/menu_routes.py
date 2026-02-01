"""Menu routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.menu_service import MenuService
from services.restaurant_service import RestaurantService
from models.menu_item import MenuItem
from utils.dynamodb import generate_id
from config.pricing import calculate_customer_price, get_platform_commission, get_markup_percentage

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_menu_routes(app):
    """Register menu routes"""
    
    @app.get("/api/v1/restaurants/<restaurant_id>/menu")
    @tracer.capture_method
    def list_menu_items(restaurant_id: str):
        """List all menu items for a restaurant"""
        try:
            logger.info(f"Listing menu items for restaurant: {restaurant_id}")
            menu_items = MenuService.list_menu_items(restaurant_id)
            metrics.add_metric(name="MenuItemsListed", unit="Count", value=1)
            
            return {
                "restaurantId": restaurant_id,
                "items": [item.to_dict() for item in menu_items],
                "total": len(menu_items)
            }, 200
        except Exception as e:
            logger.error("Error listing menu items", exc_info=True)
            return {"error": "Failed to list menu items", "message": str(e)}, 500
    
    @app.get("/api/v1/restaurants/<restaurant_id>/menu/<item_id>")
    @tracer.capture_method
    def get_menu_item(restaurant_id: str, item_id: str):
        """Get menu item by ID"""
        try:
            logger.info(f"Getting menu item: {item_id} from restaurant: {restaurant_id}")
            menu_item = MenuService.get_menu_item(restaurant_id, item_id)
            
            if not menu_item:
                return {"error": "Menu item not found"}, 404
            
            metrics.add_metric(name="MenuItemRetrieved", unit="Count", value=1)
            return menu_item.to_dict(), 200
        except Exception as e:
            logger.error("Error getting menu item", exc_info=True)
            return {"error": "Failed to get menu item", "message": str(e)}, 500
    
    @app.post("/api/v1/restaurants/<restaurant_id>/menu")
    @tracer.capture_method
    def create_menu_item(restaurant_id: str):
        """Create menu item - admin provides restaurantPrice, backend calculates customer price"""
        try:
            body = app.current_event.json_body
            name = body.get('name')
            restaurant_price = body.get('restaurantPrice')  # Admin provides restaurant's base price
            category = body.get('category')
            
            if not name or restaurant_price is None:
                return {"error": "Name and restaurantPrice are required"}, 400
            
            restaurant_price = float(restaurant_price)
            
            # Auto-calculate customer-facing price based on category markup
            customer_price = calculate_customer_price(restaurant_price, category)
            platform_commission = get_platform_commission(customer_price, restaurant_price)
            markup_percentage = get_markup_percentage(category) * 100
            
            logger.info(f"📝 Creating menu item: {name}")
            logger.info(f"💰 Restaurant price: ₹{restaurant_price}")
            logger.info(f"📁 Category: {category or 'default'}")
            logger.info(f"📊 Markup: {markup_percentage}%")
            logger.info(f"💵 Customer price: ₹{customer_price}")
            logger.info(f"💸 Platform commission: ₹{platform_commission}")
            
            menu_item = MenuItem(
                restaurant_id=restaurant_id,
                item_id=generate_id('ITM'),
                item_name=name,
                price=customer_price,  # Calculated with markup
                restaurant_price=restaurant_price,  # From admin input
                category=category,
                is_veg=body.get('isVeg'),
                is_available=body.get('isAvailable', True),
                description=body.get('description'),
                image=body.get('image')
            )
            
            created_item = MenuService.create_menu_item(menu_item)
            metrics.add_metric(name="MenuItemCreated", unit="Count", value=1)
            
            # Add pricing details to response for transparency
            response = created_item.to_dict()
            response['platformCommission'] = platform_commission
            response['markupPercentage'] = markup_percentage
            
            return response, 201
        except Exception as e:
            logger.error("Error creating menu item", exc_info=True)
            return {"error": "Failed to create menu item", "message": str(e)}, 500
    
    @app.put("/api/v1/restaurants/<restaurant_id>/menu/<item_id>")
    @tracer.capture_method
    def update_menu_item(restaurant_id: str, item_id: str):
        """Update menu item"""
        try:
            body = app.current_event.json_body
            updates = {}
            
            if 'name' in body or 'itemName' in body:
                updates['itemName'] = body.get('name') or body.get('itemName')
            if 'price' in body:
                updates['price'] = float(body['price'])
            if 'category' in body:
                updates['category'] = body['category']
            if 'isVeg' in body:
                updates['isVeg'] = body['isVeg']
            if 'isAvailable' in body:
                updates['isAvailable'] = body['isAvailable']
            if 'description' in body:
                updates['description'] = body['description']
            if 'image' in body:
                updates['image'] = body['image']
            
            if not updates:
                return {"error": "No fields to update"}, 400
            
            logger.info(f"Updating menu item: {item_id} from restaurant: {restaurant_id}")
            
            updated_item = MenuService.update_menu_item(restaurant_id, item_id, updates)
            metrics.add_metric(name="MenuItemUpdated", unit="Count", value=1)
            
            return updated_item.to_dict(), 200
        except Exception as e:
            logger.error("Error updating menu item", exc_info=True)
            return {"error": "Failed to update menu item", "message": str(e)}, 500
    
    @app.delete("/api/v1/restaurants/<restaurant_id>/menu/<item_id>")
    @tracer.capture_method
    def delete_menu_item(restaurant_id: str, item_id: str):
        """Delete a menu item"""
        try:
            logger.info(f"Deleting menu item: {item_id} from restaurant: {restaurant_id}")
            MenuService.delete_menu_item(restaurant_id, item_id)
            metrics.add_metric(name="MenuItemDeleted", unit="Count", value=1)
            
            return {"message": "Menu item deleted successfully"}, 200
        except Exception as e:
            logger.error("Error deleting menu item", exc_info=True)
            return {"error": "Failed to delete menu item", "message": str(e)}, 500

