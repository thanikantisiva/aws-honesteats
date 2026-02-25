"""Rating routes for restaurants and riders"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.restaurant_service import RestaurantService
from services.rider_service import RiderService
from services.order_service import OrderService

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_rating_routes(app):
    """Register rating routes"""

    @app.post("/api/v1/ratings")
    @tracer.capture_method
    def submit_rating():
        """
        Submit rating for rider or item.
        Request body:
        {
          "entityType": "RIDER" | "ITEM",
          "entityId": "<RDR-... | ITM-...>",
          "orderId": "<required for ITEM>",
          "rating": 1..5
        }
        """
        try:
            body = app.current_event.json_body or {}
            entity_type = str(body.get("entityType", "")).strip().upper()
            entity_id = str(body.get("entityId", "")).strip()
            rating_value = body.get("rating")
            order_id = str(body.get("orderId", "")).strip()

            if entity_type not in {"RIDER", "ITEM"}:
                return {"error": "entityType must be RIDER or ITEM"}, 400
            if not entity_id:
                return {"error": "entityId is required"}, 400
            if rating_value is None:
                return {"error": "rating is required"}, 400

            try:
                rating_value = float(rating_value)
            except (TypeError, ValueError):
                return {"error": "rating must be a number between 1 and 5"}, 400

            if rating_value < 1 or rating_value > 5:
                return {"error": "rating must be between 1 and 5"}, 400

            if entity_type == "ITEM":
                if not order_id:
                    return {"error": "orderId is required when entityType is ITEM"}, 400

                order = OrderService.get_order(order_id)
                if not order:
                    return {"error": "Order not found"}, 404
                if not order.restaurant_id:
                    return {"error": "Restaurant not found for order"}, 400

                # Store item rating at order level.
                OrderService.update_order(order_id, {"rating": rating_value})

                # Update restaurant aggregate rating using restaurant fetched from order.
                updated_restaurant = RestaurantService.add_rating(order.restaurant_id, rating_value)
                metrics.add_metric(name="ItemRated", unit="Count", value=1)
                return {
                    "entityType": "ITEM",
                    "entityId": entity_id,
                    "orderId": order_id,
                    "rating": rating_value,
                    "restaurantId": updated_restaurant.restaurant_id,
                    "restaurantRating": updated_restaurant.rating,
                    "restaurantRatedCount": updated_restaurant.rated_count
                }, 200

            updated = RiderService.add_rating(entity_id, rating_value)
            metrics.add_metric(name="RiderRated", unit="Count", value=1)
            return {
                "entityType": "RIDER",
                "entityId": updated.rider_id,
                "rating": updated.rating,
                "ratedCount": updated.rated_count
            }, 200
        except Exception as e:
            logger.error("Failed to submit rating", exc_info=True)
            return {"error": "Failed to submit rating", "message": str(e)}, 500
