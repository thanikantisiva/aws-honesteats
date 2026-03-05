"""App configuration routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import python_to_dynamodb, dynamodb_to_python
from utils.datetime_ist import now_ist_iso

logger = Logger()
tracer = Tracer()
metrics = Metrics()

# Fixed keys for global app config (not from request body)
CONFIG_PK = "CONFIG#GLOBAL"
CONFIG_SK = "CONFIG"


def _restaurant_config_pk(restaurant_id: str) -> str:
    """Build partition key for restaurant-specific config."""
    return f"CONFIG#RESTAURANT#{restaurant_id.strip()}"


def _extract_payload(body: dict):
    """Prefer explicit config object; otherwise store request body except key fields."""
    if "config" in body:
        return body.get("config")

    filtered = {
        k: v
        for k, v in body.items()
        if k not in {"configType", "configKey", "restaurantId"}
    }
    return filtered


def _fetch_config_item(pk: str, sk: str):
    """Fetch one config row from DynamoDB."""
    response = dynamodb_client.get_item(
        TableName=TABLES["CONFIG"],
        Key={
            "partitionkey": {"S": pk},
            "sortKey": {"S": sk}
        }
    )
    return response.get("Item")


def register_config_routes(app):
    """Register app config routes"""

    @app.post("/api/v1/globalconfig")
    @tracer.capture_method
    def upsert_config():
        """
        Store config JSON in DynamoDB.
        Request:
        {
          "restaurantId": "RES-...", // optional. if present stores restaurant specific config
          "config": { ... }  // optional; if absent full body is stored minus key fields
        }
        """
        try:
            body = app.current_event.json_body or {}
            restaurant_id = str(body.get("restaurantId", "")).strip()
            if restaurant_id:
                pk = _restaurant_config_pk(restaurant_id)
                sk = CONFIG_SK
            else:
                pk = CONFIG_PK
                sk = CONFIG_SK

            payload = _extract_payload(body)

            item = {
                "partitionkey": {"S": pk},
                "sortKey": {"S": sk},
                "config": python_to_dynamodb(payload),
                "updatedAt": {"S": now_ist_iso()}
            }

            dynamodb_client.put_item(
                TableName=TABLES["CONFIG"],
                Item=item
            )

            metrics.add_metric(name="ConfigSaved", unit="Count", value=1)
            return {
                "message": "Config saved",
                "partitionkey": pk,
                "sortKey": sk,
                "scope": "RESTAURANT" if restaurant_id else "GLOBAL"
            }, 200
        except Exception as e:
            logger.error("Error saving config", exc_info=True)
            return {"error": "Failed to save config", "message": str(e)}, 500

    @app.get("/api/v1/globalconfig")
    @tracer.capture_method
    def get_config():
        """Fetch config. If restaurant config is not found, fallback to global."""
        try:
            query_params = app.current_event.query_string_parameters or {}
            restaurant_id = str(query_params.get("restaurantId", "")).strip()

            source = "GLOBAL"
            pk, sk = CONFIG_PK, CONFIG_SK
            item = None

            if restaurant_id:
                restaurant_pk = _restaurant_config_pk(restaurant_id)
                item = _fetch_config_item(restaurant_pk, CONFIG_SK)
                if item:
                    pk = restaurant_pk
                    source = "RESTAURANT"

            if not item:
                item = _fetch_config_item(CONFIG_PK, CONFIG_SK)
                pk = CONFIG_PK
                source = "GLOBAL"

            if not item:
                return {"error": "Config not found"}, 404

            config_payload = dynamodb_to_python(item.get("config", {"NULL": True}))
            metrics.add_metric(name="ConfigFetched", unit="Count", value=1)
            return {
                "partitionkey": pk,
                "sortKey": CONFIG_SK,
                "config": config_payload,
                "updatedAt": item.get("updatedAt", {}).get("S"),
                "source": source
            }, 200
        except Exception as e:
            logger.error("Error fetching config", exc_info=True)
            return {"error": "Failed to fetch config", "message": str(e)}, 500
