"""Menu routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.coupon_service import CouponService
from services.menu_service import MenuService
from services.restaurant_service import RestaurantService
from models.menu_item import MenuItem
from utils.dynamodb import generate_id, dynamodb_client, TABLES
from config.pricing import get_platform_commission

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_menu_routes(app):
    """Register menu routes"""

    MAX_TOP_OFFER_BANNER_LENGTH = 20

    def _normalize_top_offer_banner(value):
        if value is None:
            return None, None
        if not isinstance(value, str):
            return None, ({"error": "topOfferBanner must be a string"}, 400)

        normalized = value.strip()
        if not normalized:
            return None, None
        if len(normalized) > MAX_TOP_OFFER_BANNER_LENGTH:
            return None, ({"error": f"topOfferBanner must be {MAX_TOP_OFFER_BANNER_LENGTH} characters or fewer"}, 400)
        return normalized, None

    def _normalize_item_offer_coupon_code(value):
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    def _serialize_menu_item(menu_item: MenuItem) -> dict:
        pricing = CouponService.get_menu_item_prices(menu_item)
        return menu_item.to_dict(
            price=pricing["price"],
            original_price=pricing.get("originalPrice"),
        )

    @app.get("/api/v1/restaurants/<restaurant_id>/menu/changed")
    @tracer.capture_method
    def get_menu_change_marker(restaurant_id: str):
        """Check if menu has changed for a restaurant within TTL window."""
        try:
            pk = f"RESTAURANT#{restaurant_id}"
            sk = "CHANGED"
            response = dynamodb_client.get_item(
                TableName=TABLES["MENU_ITEMS"],
                Key={
                    "PK": {"S": pk},
                    "SK": {"S": sk}
                }
            )
            exists = "Item" in response and response["Item"] is not None
            metrics.add_metric(name="MenuChangeChecked", unit="Count", value=1)
            return {
                "restaurantId": restaurant_id,
                "changed": bool(exists)
            }, 200
        except Exception as e:
            logger.error("Error checking menu change marker", exc_info=True)
            return {"error": "Failed to check menu change marker", "message": str(e)}, 500

    @app.get("/api/v1/restaurants/<restaurant_id>/menu")
    @tracer.capture_method
    def list_menu_items(restaurant_id: str):
        """List all menu items for a restaurant"""
        try:
            logger.info(f"Listing menu items for restaurant: {restaurant_id}")
            menu_items = MenuService.list_menu_items(restaurant_id)
            # Exclude items without itemId
            valid_items = [item for item in menu_items if item.item_id]
            metrics.add_metric(name="MenuItemsListed", unit="Count", value=1)
            
            return {
                "restaurantId": restaurant_id,
                "items": [_serialize_menu_item(item) for item in valid_items],
                "total": len(valid_items)
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
            return _serialize_menu_item(menu_item), 200
        except Exception as e:
            logger.error("Error getting menu item", exc_info=True)
            return {"error": "Failed to get menu item", "message": str(e)}, 500
    
    @app.post("/api/v1/restaurants/<restaurant_id>/menu")
    @tracer.capture_method
    def create_menu_item(restaurant_id: str):
        """Create menu item - admin provides restaurantPrice and optional hikePercentage; customer price is computed."""
        try:
            body = app.current_event.json_body or {}
            name = body.get('name')
            restaurant_price = body.get('restaurantPrice')
            hike_percentage = body.get('hikePercentage', 0)
            category = body.get('category')
            sub_category = body.get('subCategory')
            top_offer_banner, banner_error = _normalize_top_offer_banner(body.get('topOfferBanner'))
            item_offer_coupon_code = _normalize_item_offer_coupon_code(body.get('itemOfferCouponCode'))
            
            if not name or restaurant_price is None:
                return {"error": "Name and restaurantPrice are required"}, 400
            if banner_error:
                return banner_error
            
            restaurant_price = float(restaurant_price)
            hike_percentage = float(hike_percentage) if hike_percentage is not None else 0.0
            
            logger.info(f"Creating menu item: {name}, restaurantPrice=₹{restaurant_price}, hikePercentage={hike_percentage}%")
            
            menu_item = MenuItem(
                restaurant_id=restaurant_id,
                item_id=generate_id('ITM'),
                item_name=name,
                restaurant_price=restaurant_price,
                hike_percentage=hike_percentage,
                category=category,
                sub_category=sub_category,
                is_veg=body.get('isVeg'),
                is_available=body.get('isAvailable', True),
                description=body.get('description'),
                image=body.get('image'),
                top_offer_banner=top_offer_banner,
                item_offer_coupon_code=item_offer_coupon_code
            )
            
            created_item = MenuService.create_menu_item(menu_item)
            metrics.add_metric(name="MenuItemCreated", unit="Count", value=1)
            
            pricing = CouponService.get_menu_item_prices(created_item)
            response = created_item.to_dict(
                price=pricing["price"],
                original_price=pricing.get("originalPrice"),
            )
            response['platformCommission'] = get_platform_commission(pricing["price"], restaurant_price)
            response['hikePercentage'] = created_item.hike_percentage
            
            return response, 201
        except Exception as e:
            logger.error("Error creating menu item", exc_info=True)
            return {"error": "Failed to create menu item", "message": str(e)}, 500
    
    @app.put("/api/v1/restaurants/<restaurant_id>/menu/<item_id>")
    @tracer.capture_method
    def update_menu_item(restaurant_id: str, item_id: str):
        """Update menu item"""
        try:
            body = app.current_event.json_body or {}
            updates = {}
            
            if 'name' in body or 'itemName' in body:
                updates['itemName'] = body.get('name') or body.get('itemName')
            if 'restaurantPrice' in body:
                updates['restaurantPrice'] = float(body['restaurantPrice'])
            if 'hikePercentage' in body:
                updates['hikePercentage'] = float(body['hikePercentage'])
            if 'category' in body:
                updates['category'] = body['category']
            if 'subCategory' in body:
                updates['subCategory'] = body['subCategory']
            if 'isVeg' in body:
                updates['isVeg'] = body['isVeg']
            if 'isAvailable' in body:
                updates['isAvailable'] = body['isAvailable']
            if 'description' in body:
                updates['description'] = body['description']
            if 'image' in body:
                updates['image'] = body['image']
            if 'topOfferBanner' in body:
                top_offer_banner, banner_error = _normalize_top_offer_banner(body.get('topOfferBanner'))
                if banner_error:
                    return banner_error
                updates['topOfferBanner'] = top_offer_banner
            if 'itemOfferCouponCode' in body:
                updates['itemOfferCouponCode'] = _normalize_item_offer_coupon_code(body.get('itemOfferCouponCode'))
            
            if not updates:
                return {"error": "No fields to update"}, 400
            
            logger.info(f"Updating menu item: {item_id} from restaurant: {restaurant_id}")
            
            updated_item = MenuService.update_menu_item(restaurant_id, item_id, updates)
            metrics.add_metric(name="MenuItemUpdated", unit="Count", value=1)
            
            return _serialize_menu_item(updated_item), 200
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

    @app.post("/api/v1/restaurants/<restaurant_id>/menu/price-hike")
    @tracer.capture_method
    def bulk_price_hike(restaurant_id: str):
        """Increase restaurantPrice of all menu items for a restaurant by a given percentage.

        Body: { "percentage": <float> }  — required, must be > 0 and <= 500.
        """
        try:
            body = app.current_event.json_body or {}
            percentage = body.get('percentage')

            if percentage is None:
                return {"error": "percentage is required"}, 400

            try:
                percentage = float(percentage)
            except (ValueError, TypeError):
                return {"error": "percentage must be a valid number"}, 400

            if percentage <= 0:
                return {"error": "percentage must be greater than 0"}, 400
            if percentage > 500:
                return {"error": "percentage cannot exceed 500"}, 400

            logger.info(f"Applying {percentage}% price hike to all menu items for restaurant: {restaurant_id}")

            updated_items = MenuService.bulk_price_hike(restaurant_id, percentage)
            metrics.add_metric(name="MenuPriceHikeApplied", unit="Count", value=1)

            return {
                "restaurantId": restaurant_id,
                "percentage": percentage,
                "updatedCount": len(updated_items),
                "items": updated_items
            }, 200
        except Exception as e:
            logger.error("Error applying price hike", exc_info=True)
            return {"error": "Failed to apply price hike", "message": str(e)}, 500
