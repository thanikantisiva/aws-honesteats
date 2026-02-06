"""Image routes"""
import base64

import boto3
from aws_lambda_powertools import Logger, Tracer, Metrics

logger = Logger()
tracer = Tracer()
metrics = Metrics()

S3_BUCKET = "customerapp"
S3_PREFIX = "homescreen/"


def _list_s3_objects(s3_client, bucket: str, prefix: str):
    """List all S3 objects under a prefix, handling pagination."""
    continuation_token = None
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**params)
        contents = response.get("Contents", [])
        for item in contents:
            yield item

        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break


def register_image_routes(app):
    """Register image routes"""
    s3_client = boto3.client("s3")

    @app.get("/api/v1/homescreen/images")
    @tracer.capture_method
    def list_homescreen_images():
        """Fetch all homescreen images from S3 and return as base64."""
        try:
            logger.info("Listing homescreen images from S3")

            images = []
            for obj in _list_s3_objects(s3_client, S3_BUCKET, S3_PREFIX):
                key = obj.get("Key")
                if not key or key.endswith("/"):
                    continue

                s3_object = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                image_bytes = s3_object["Body"].read()
                encoded = base64.b64encode(image_bytes).decode("utf-8")

                images.append({
                    "key": key,
                    "base64": encoded
                })

            metrics.add_metric(name="HomescreenImagesListed", unit="Count", value=1)

            return {
                "bucket": S3_BUCKET,
                "prefix": S3_PREFIX,
                "total": len(images),
                "images": images
            }, 200
        except Exception as e:
            logger.error("Error fetching homescreen images", exc_info=True)
            return {"error": "Failed to fetch images", "message": str(e)}, 500
