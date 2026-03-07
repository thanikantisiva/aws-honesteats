"""Image routes"""
import base64
import os
from datetime import datetime
from urllib.parse import quote

import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger, Tracer, Metrics

logger = Logger()
tracer = Tracer()
metrics = Metrics()

S3_BUCKET = "customerapp-offers"
S3_PREFIX = "homescreen/"
LOGIN_PREFIX = "login/"
RESTAURANT_IMAGES_BUCKET = os.environ.get("RESTAURANT_IMAGES_BUCKET", "yumdude-restaurant-images-dev")
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".svg")
IMAGE_CDN_BASE_URL = os.environ.get("IMAGE_CDN_BASE_URL", "").rstrip("/")


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


def _normalize_prefix(path: str) -> str:
    """Normalize request path to S3 prefix."""
    prefix = (path or "").strip()
    prefix = prefix.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    return prefix


def _parse_bucket_and_prefix(path: str):
    """
    Parse request path where first segment is bucket and rest is prefix.
    Example: /customerapp-offers/login/banner/ -> (customerapp-offers, login/banner/)
    """
    normalized = (path or "").strip().strip("/")
    if not normalized:
        return None, None
    parts = normalized.split("/", 1)
    bucket = parts[0]
    prefix = _normalize_prefix(parts[1] if len(parts) > 1 else "")
    return bucket, prefix


def _parse_bucket_and_key(path: str):
    """Parse request path into bucket and exact object key."""
    normalized = (path or "").strip().strip("/")
    if not normalized:
        return None, None
    parts = normalized.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def _build_image_url(bucket: str, key: str) -> str:
    """Build CDN URL only."""
    if not IMAGE_CDN_BASE_URL:
        raise Exception("IMAGE_CDN_BASE_URL not configured")
    # Keep already-encoded path segments intact to avoid % -> %25 double-encoding.
    encoded_key = quote(key, safe="/%")
    return f"{IMAGE_CDN_BASE_URL}/{encoded_key}"


def _decode_base64_image(value: str):
    """Decode base64 payload and infer file extension."""
    extension = "jpg"
    payload = (value or "").strip()
    if payload.startswith("data:"):
        header, payload = payload.split(",", 1)
        if "image/" in header:
            mime = header.split("image/")[1].split(";")[0].lower()
            if mime in ("jpeg", "jpg"):
                extension = "jpg"
            elif mime in ("png", "webp", "gif", "bmp", "svg+xml"):
                extension = "svg" if mime == "svg+xml" else mime
    data = base64.b64decode(payload)
    return data, extension


def register_image_routes(app):
    """Register image routes"""
    s3_client = boto3.client("s3")

    @app.get("/api/v1/homescreen/images")
    @tracer.capture_method
    def list_homescreen_images():
        """Fetch all homescreen images from S3 and return as base64."""
        try:
            if not IMAGE_CDN_BASE_URL:
                return {"error": "CDN base URL not configured"}, 500
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
                    "base64": encoded,
                    "url": _build_image_url(S3_BUCKET, key)
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

    @app.get("/api/v1/login/images")
    @tracer.capture_method
    def list_login_images():
        """Fetch all login images from S3 and return as base64."""
        try:
            if not IMAGE_CDN_BASE_URL:
                return {"error": "CDN base URL not configured"}, 500
            logger.info("Listing login images from S3")

            images = []
            for obj in _list_s3_objects(s3_client, S3_BUCKET, LOGIN_PREFIX):
                key = obj.get("Key")
                if not key or key.endswith("/"):
                    continue

                s3_object = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                image_bytes = s3_object["Body"].read()
                encoded = base64.b64encode(image_bytes).decode("utf-8")

                images.append({
                    "key": key,
                    "base64": encoded,
                    "url": _build_image_url(S3_BUCKET, key)
                })

            metrics.add_metric(name="LoginImagesListed", unit="Count", value=1)

            return {
                "bucket": S3_BUCKET,
                "prefix": LOGIN_PREFIX,
                "total": len(images),
                "images": images
            }, 200
        except Exception as e:
            logger.error("Error fetching login images", exc_info=True)
            return {"error": "Failed to fetch images", "message": str(e)}, 500

    def _list_images_by_input(raw_path: str):
        """
        Fetch images from a dynamic S3 folder/file and return as base64.

        Input path format:
        /customerapp-offers/folder/or/file.jpg
        """
        try:
            if not IMAGE_CDN_BASE_URL:
                return {"error": "CDN base URL not configured"}, 500
            if not raw_path:
                return {"error": "path is required"}, 400

            bucket, prefix = _parse_bucket_and_prefix(raw_path)
            _, exact_key = _parse_bucket_and_key(raw_path)
            if not bucket:
                return {"error": "Invalid path"}, 400
            

            logger.info(f"Listing images from S3 bucket={bucket}, prefix={prefix}, key={exact_key}")

            images = []

            # If path points to a file, try exact object retrieval first.
            if exact_key and exact_key.lower().endswith(IMAGE_EXTENSIONS):
                try:
                    s3_object = s3_client.get_object(Bucket=bucket, Key=exact_key)
                    image_bytes = s3_object["Body"].read()
                    encoded = base64.b64encode(image_bytes).decode("utf-8")
                    images.append({
                        "key": exact_key,
                        "base64": encoded,
                        "url": _build_image_url(bucket, exact_key)
                    })
                except ClientError as e:
                    code = e.response.get("Error", {}).get("Code", "")
                    if code not in ("NoSuchKey", "404"):
                        raise

            # For folder paths (or missing file), list by prefix.
            if not images:
                list_prefix = prefix or _normalize_prefix(exact_key)
                if list_prefix:
                    for obj in _list_s3_objects(s3_client, bucket, list_prefix):
                        key = obj.get("Key")
                        if not key or key.endswith("/"):
                            continue
                        if not key.lower().endswith(IMAGE_EXTENSIONS):
                            continue

                        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
                        image_bytes = s3_object["Body"].read()
                        encoded = base64.b64encode(image_bytes).decode("utf-8")

                        images.append({
                            "key": key,
                            "base64": encoded,
                            "url": _build_image_url(bucket, key)
                        })

            metrics.add_metric(name="ImagesByPathListed", unit="Count", value=1)
            return {
                "bucket": bucket,
                "prefix": prefix,
                "key": exact_key or "",
                "total": len(images),
                "images": images
            }, 200
        except Exception as e:
            logger.error("Error fetching images by path", exc_info=True)
            return {"error": "Failed to fetch images", "message": str(e)}, 500

    @app.get("/api/v1/images/by-path")
    @tracer.capture_method
    def list_images_by_path_query():
        """Fetch images by path passed in URL query param: ?path=/bucket/folder/..."""
        query_params = app.current_event.query_string_parameters or {}
        raw_path = query_params.get("path")
        return _list_images_by_input(raw_path)

    @app.post("/api/v1/images/upload")
    @tracer.capture_method
    def upload_images():
        """
        Upload image list and return CDN urls.

        Request body:
        {
          "listBase64": ["data:image/jpeg;base64,..."],
          "entity": "RESTAURANT" | "ITEM" | "SUBCATEGORY",
          "restaurantId": "RES-...",
          "itemId": "ITEM-..."  # required for ITEM
        }
        """
        try:
            if not IMAGE_CDN_BASE_URL:
                return {"error": "CDN base URL not configured"}, 500
            body = app.current_event.json_body or {}
            list_base64 = body.get("listBase64") or []
            entity = str(body.get("entity") or "").upper()
            restaurant_id = body.get("restaurantId")
            item_id = body.get("itemId")

            if not isinstance(list_base64, list) or len(list_base64) == 0:
                return {"error": "listBase64 must be a non-empty array"}, 400
            if entity not in ("RESTAURANT", "ITEM", "SUBCATEGORY"):
                return {"error": "entity must be RESTAURANT, ITEM, or SUBCATEGORY"}, 400
            if entity in ("RESTAURANT", "ITEM") and not restaurant_id:
                return {"error": "restaurantId is required"}, 400
            if entity == "ITEM" and not item_id:
                return {"error": "itemId is required for ITEM entity"}, 400

            if entity == "RESTAURANT":
                base_prefix = f"restaurant-images/{restaurant_id}"
            elif entity == "ITEM":
                base_prefix = f"restaurant-images/{restaurant_id}/{item_id}"
            else:
                base_prefix = "subcategory"

            uploaded = []
            now = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

            for idx, encoded in enumerate(list_base64):
                image_data, extension = _decode_base64_image(encoded)
                key = f"{base_prefix}/{now}-{idx + 1}.{extension}"

                s3_client.put_object(
                    Bucket=RESTAURANT_IMAGES_BUCKET,
                    Key=key,
                    Body=image_data,
                    ContentType=f"image/{'svg+xml' if extension == 'svg' else extension}",
                    ContentDisposition="inline"
                )

                uploaded.append({
                    "key": key,
                    "url": _build_image_url(RESTAURANT_IMAGES_BUCKET, key)
                })

            metrics.add_metric(name="ImagesUploaded", unit="Count", value=1)
            return {
                "bucket": RESTAURANT_IMAGES_BUCKET,
                "entity": entity,
                "restaurantId": restaurant_id if entity in ("RESTAURANT", "ITEM") else None,
                "itemId": item_id if entity == "ITEM" else None,
                "total": len(uploaded),
                "images": uploaded
            }, 200
        except Exception as e:
            logger.error("Error uploading images", exc_info=True)
            return {"error": "Failed to upload images", "message": str(e)}, 500
