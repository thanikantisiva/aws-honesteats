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


def _extract_payload(body: dict):
    """Prefer explicit config object; otherwise store request body except key fields."""
    if "config" in body:
        return body.get("config")

    filtered = {
        k: v
        for k, v in body.items()
        if k not in {"configType", "configKey"}
    }
    return filtered


def register_config_routes(app):
    """Register app config routes"""

    @app.post("/api/v1/globalconfig")
    @tracer.capture_method
    def upsert_config():
        """
        Store app config JSON in DynamoDB.
        Request:
        {
          "configType": "APP",
          "configKey": "CURRENT",
          "config": { ... }  // optional; if absent full body is stored minus key fields
        }
        """
        try:
            body = app.current_event.json_body or {}
            pk, sk = CONFIG_PK, CONFIG_SK
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
                "sortKey": sk
            }, 200
        except Exception as e:
            logger.error("Error saving config", exc_info=True)
            return {"error": "Failed to save config", "message": str(e)}, 500

    @app.get("/api/v1/globalconfig")
    @tracer.capture_method
    def get_config():
        """Fetch global config document."""
        try:
            pk, sk = CONFIG_PK, CONFIG_SK

            response = dynamodb_client.get_item(
                TableName=TABLES["CONFIG"],
                Key={
                    "partitionkey": {"S": pk},
                    "sortKey": {"S": sk}
                }
            )

            item = response.get("Item")
            if not item:
                return {"error": "Config not found"}, 404

            config_payload = dynamodb_to_python(item.get("config", {"NULL": True}))
            metrics.add_metric(name="ConfigFetched", unit="Count", value=1)
            return {
                "partitionkey": pk,
                "sortKey": sk,
                "config": config_payload,
                "updatedAt": item.get("updatedAt", {}).get("S")
            }, 200
        except Exception as e:
            logger.error("Error fetching config", exc_info=True)
            return {"error": "Failed to fetch config", "message": str(e)}, 500
