"""Food category routes"""
import re
from aws_lambda_powertools import Logger, Tracer, Metrics
from utils.dynamodb import dynamodb_client, TABLES
from utils.datetime_ist import now_ist_iso

logger = Logger()
tracer = Tracer()
metrics = Metrics()

FOOD_CATEGORY_PK = "FOOD_CATEGORIES"


def _normalize_key(value: str) -> str:
    """Normalize display value into a stable key-safe token."""
    value = str(value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def _build_sort_key(category: str, sub_category: str) -> str:
    category_key = _normalize_key(category)
    sub_category_key = _normalize_key(sub_category)
    return f"CAT#{category_key}#SUB#{sub_category_key}"


VALID_TIME_SLOTS = frozenset({"morning", "brunch", "afternoon", "evening", "night"})


def _parse_db_item(item: dict) -> dict:
    result = {
        "category": item.get("category", {}).get("S", ""),
        "subCategory": item.get("subCategory", {}).get("S", ""),
        "imageUrl": item.get("imageUrl", {}).get("S", ""),
        "isDisplayItem": item.get("isDisplayItem", {}).get("S", ""),
        "createdAt": item.get("createdAt", {}).get("S"),
        "updatedAt": item.get("updatedAt", {}).get("S"),
    }
    # preferredTimeSlots: DynamoDB L of S -> list for client (morning, brunch, afternoon, evening, night)
    raw_list = item.get("preferredTimeSlots", {}).get("L", [])
    slots = [x.get("S", "").strip().lower() for x in raw_list if isinstance(x, dict) and x.get("S")]
    result["preferredTimeSlots"] = [s for s in slots if s in VALID_TIME_SLOTS]
    return result

def _coerce_text(value, default=""):
    if value is None:
        return default
    return str(value).strip()


def register_food_category_routes(app):
    """Register food category CRUD routes."""

    @app.post("/api/v1/food-categories")
    @tracer.capture_method
    def create_food_category():
        """Create (or upsert) category + subcategory row."""
        try:
            body = app.current_event.json_body or {}
            category = body.get("category")
            sub_category = body.get("subCategory")
            image_url = body.get("imageUrl")
            is_display_item = _coerce_text(body.get("isDisplayItem"), default="true")
            preferred_time_slots = body.get("preferredTimeSlots")

            if not category or not sub_category or not image_url:
                return {"error": "category, subCategory, imageUrl are required"}, 400

            sort_key = _build_sort_key(category, sub_category)
            now = now_ist_iso()

            # Preserve createdAt if row exists.
            existing = dynamodb_client.get_item(
                TableName=TABLES["CONFIG"],
                Key={
                    "partitionkey": {"S": FOOD_CATEGORY_PK},
                    "sortKey": {"S": sort_key},
                },
            ).get("Item")
            created_at = existing.get("createdAt", {}).get("S", now) if existing else now

            item = {
                "partitionkey": {"S": FOOD_CATEGORY_PK},
                "sortKey": {"S": sort_key},
                "category": {"S": str(category).strip()},
                "subCategory": {"S": str(sub_category).strip()},
                "imageUrl": {"S": str(image_url).strip()},
                "isDisplayItem": {"S": is_display_item},
                "createdAt": {"S": created_at},
                "updatedAt": {"S": now},
            }
            if preferred_time_slots is not None and isinstance(preferred_time_slots, list):
                valid = [str(s).strip().lower() for s in preferred_time_slots if str(s).strip().lower() in VALID_TIME_SLOTS]
                item["preferredTimeSlots"] = {"L": [{"S": s} for s in valid]}

            dynamodb_client.put_item(
                TableName=TABLES["CONFIG"],
                Item=item,
            )

            metrics.add_metric(name="FoodCategoryCreated", unit="Count", value=1)
            return {
                "message": "Food category saved",
                "partitionkey": FOOD_CATEGORY_PK,
                "sortKey": sort_key,
            }, 200
        except Exception as e:
            logger.error("Error saving food category", exc_info=True)
            return {"error": "Failed to save food category", "message": str(e)}, 500

    @app.get("/api/v1/food-categories")
    @tracer.capture_method
    def list_food_categories():
        """
        List categories.
        Access patterns:
        - all rows: GET /api/v1/food-categories
        - selected categories: GET /api/v1/food-categories?category=veg,pizza
        """
        try:
            query_params = app.current_event.query_string_parameters or {}
            category_filter = query_params.get("category")
            rows = []

            if not category_filter:
                response = dynamodb_client.query(
                    TableName=TABLES["CONFIG"],
                    KeyConditionExpression="partitionkey = :pk",
                    ExpressionAttributeValues={":pk": {"S": FOOD_CATEGORY_PK}},
                )
                rows = response.get("Items", [])
            else:
                seen = set()
                categories = [
                    c.strip()
                    for c in str(category_filter).split(",")
                    if c and c.strip()
                ]
                for category in categories:
                    prefix = f"CAT#{_normalize_key(category)}#"
                    response = dynamodb_client.query(
                        TableName=TABLES["CONFIG"],
                        KeyConditionExpression="partitionkey = :pk AND begins_with(sortKey, :prefix)",
                        ExpressionAttributeValues={
                            ":pk": {"S": FOOD_CATEGORY_PK},
                            ":prefix": {"S": prefix},
                        },
                    )
                    for item in response.get("Items", []):
                        key = item.get("sortKey", {}).get("S")
                        if key and key not in seen:
                            seen.add(key)
                            rows.append(item)

            data = [_parse_db_item(item) for item in rows]
            metrics.add_metric(name="FoodCategoriesListed", unit="Count", value=1)
            return {"items": data, "total": len(data)}, 200
        except Exception as e:
            logger.error("Error listing food categories", exc_info=True)
            return {"error": "Failed to list food categories", "message": str(e)}, 500

    @app.get("/api/v1/food-categories/display")
    @tracer.capture_method
    def list_display_food_categories():
        """List categories using isDisplayItem GSI. Defaults to true."""
        try:
            query_params = app.current_event.query_string_parameters or {}
            display_value = _coerce_text(query_params.get("isDisplayItem"), default="true")

            response = dynamodb_client.query(
                TableName=TABLES["CONFIG"],
                IndexName="isDisplayItem-index",
                KeyConditionExpression="isDisplayItem = :display",
                ExpressionAttributeValues={
                    ":display": {"S": display_value},
                },
            )
            rows = response.get("Items", [])
            data = [_parse_db_item(item) for item in rows]
            metrics.add_metric(name="FoodCategoriesDisplayListed", unit="Count", value=1)
            return data, 200
        except Exception as e:
            logger.error("Error listing display food categories", exc_info=True)
            return {"error": "Failed to list display food categories", "message": str(e)}, 500

    @app.get("/api/v1/food-categories/<category>/<sub_category>")
    @tracer.capture_method
    def get_food_category(category: str, sub_category: str):
        """Get one category/subcategory row."""
        try:
            sort_key = _build_sort_key(category, sub_category)
            response = dynamodb_client.get_item(
                TableName=TABLES["CONFIG"],
                Key={
                    "partitionkey": {"S": FOOD_CATEGORY_PK},
                    "sortKey": {"S": sort_key},
                },
            )
            item = response.get("Item")
            if not item:
                return {"error": "Food category not found"}, 404

            metrics.add_metric(name="FoodCategoryFetched", unit="Count", value=1)
            return _parse_db_item(item), 200
        except Exception as e:
            logger.error("Error fetching food category", exc_info=True)
            return {"error": "Failed to fetch food category", "message": str(e)}, 500

    @app.put("/api/v1/food-categories/<category>/<sub_category>")
    @tracer.capture_method
    def update_food_category(category: str, sub_category: str):
        """Update image and/or rename category/subcategory."""
        try:
            body = app.current_event.json_body or {}
            new_category = body.get("category", category)
            new_sub_category = body.get("subCategory", sub_category)
            image_url = body.get("imageUrl")
            is_display_item = body.get("isDisplayItem")
            preferred_time_slots = body.get("preferredTimeSlots")

            old_sort_key = _build_sort_key(category, sub_category)
            get_response = dynamodb_client.get_item(
                TableName=TABLES["CONFIG"],
                Key={
                    "partitionkey": {"S": FOOD_CATEGORY_PK},
                    "sortKey": {"S": old_sort_key},
                },
            )
            old_item = get_response.get("Item")
            if not old_item:
                return {"error": "Food category not found"}, 404

            new_sort_key = _build_sort_key(new_category, new_sub_category)
            now = now_ist_iso()
            final_image = image_url if image_url is not None else old_item.get("imageUrl", {}).get("S", "")
            final_display = _coerce_text(
                is_display_item,
                default=old_item.get("isDisplayItem", {}).get("S", "true")
            )

            put_item = {
                "partitionkey": {"S": FOOD_CATEGORY_PK},
                "sortKey": {"S": new_sort_key},
                "category": {"S": str(new_category).strip()},
                "subCategory": {"S": str(new_sub_category).strip()},
                "imageUrl": {"S": str(final_image).strip()},
                "isDisplayItem": {"S": final_display},
                "createdAt": {"S": old_item.get("createdAt", {}).get("S", now)},
                "updatedAt": {"S": now},
            }
            if preferred_time_slots is not None and isinstance(preferred_time_slots, list):
                valid = [str(s).strip().lower() for s in preferred_time_slots if str(s).strip().lower() in VALID_TIME_SLOTS]
                put_item["preferredTimeSlots"] = {"L": [{"S": s} for s in valid]}
            else:
                # Preserve existing preferredTimeSlots
                if "preferredTimeSlots" in old_item:
                    put_item["preferredTimeSlots"] = old_item["preferredTimeSlots"]

            dynamodb_client.put_item(
                TableName=TABLES["CONFIG"],
                Item=put_item,
            )

            if new_sort_key != old_sort_key:
                dynamodb_client.delete_item(
                    TableName=TABLES["CONFIG"],
                    Key={
                        "partitionkey": {"S": FOOD_CATEGORY_PK},
                        "sortKey": {"S": old_sort_key},
                    },
                )

            metrics.add_metric(name="FoodCategoryUpdated", unit="Count", value=1)
            return {"message": "Food category updated", "sortKey": new_sort_key}, 200
        except Exception as e:
            logger.error("Error updating food category", exc_info=True)
            return {"error": "Failed to update food category", "message": str(e)}, 500

    @app.delete("/api/v1/food-categories/<category>/<sub_category>")
    @tracer.capture_method
    def delete_food_category(category: str, sub_category: str):
        """Delete a category/subcategory row."""
        try:
            sort_key = _build_sort_key(category, sub_category)
            dynamodb_client.delete_item(
                TableName=TABLES["CONFIG"],
                Key={
                    "partitionkey": {"S": FOOD_CATEGORY_PK},
                    "sortKey": {"S": sort_key},
                },
            )
            metrics.add_metric(name="FoodCategoryDeleted", unit="Count", value=1)
            return {"message": "Food category deleted", "sortKey": sort_key}, 200
        except Exception as e:
            logger.error("Error deleting food category", exc_info=True)
            return {"error": "Failed to delete food category", "message": str(e)}, 500
