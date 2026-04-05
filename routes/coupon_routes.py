"""Coupon routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.coupon_service import CouponService
from services.menu_service import MenuService
from utils.dynamodb import dynamodb_client, TABLES

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_coupon_routes(app):
    """Register coupon routes"""

    MAX_ITEM_BANNER_TEXT_LENGTH = 20

    def _normalize_item_banner_text(value):
        if value is None:
            return None, None
        if not isinstance(value, str):
            return None, ({"error": "itemBannerText must be a string"}, 400)

        normalized = value.strip()
        if not normalized:
            return None, None
        if len(normalized) > MAX_ITEM_BANNER_TEXT_LENGTH:
            return None, (
                {"error": f"itemBannerText must be {MAX_ITEM_BANNER_TEXT_LENGTH} characters or fewer"},
                400,
            )
        return normalized, None

    @app.post("/api/v1/coupons")
    @tracer.capture_method
    def create_coupon():
        """Create or update a coupon in config table"""
        try:
            body = app.current_event.json_body or {}
            code = body.get('couponCode')
            coupon_type = body.get('couponType')
            coupon_value = body.get('couponValue')
            start_date = body.get('startDate')
            end_date = body.get('endDate')
            issued_by = body.get('issuedBy')
            coupon_restaurant = body.get('couponRestaurant')
            item_id = body.get('itemId')
            item_banner_text, item_banner_error = _normalize_item_banner_text(body.get('itemBannerText'))
            is_once_per_user = body.get('isOncePerUser', False)
            if isinstance(is_once_per_user, str):
                is_once_per_user = is_once_per_user.strip().lower() in ('true', '1', 'yes')
            else:
                is_once_per_user = bool(is_once_per_user)

            if not code or not coupon_type or coupon_value is None:
                return {"error": "couponCode, couponType, couponValue are required"}, 400
            if item_banner_error:
                return item_banner_error

            normalized_issued_by = str(issued_by).strip().upper() if issued_by is not None else None
            normalized_coupon_restaurant = str(coupon_restaurant).strip() if coupon_restaurant is not None else None
            normalized_item_id = str(item_id).strip() if item_id is not None else None
            if normalized_coupon_restaurant == "":
                normalized_coupon_restaurant = None
            if normalized_item_id == "":
                normalized_item_id = None

            if normalized_issued_by == 'RESTAURANT' and not normalized_coupon_restaurant:
                return {"error": "couponRestaurant is required when issuedBy is RESTAURANT"}, 400
            if normalized_item_id and not normalized_coupon_restaurant:
                return {"error": "couponRestaurant is required when itemId is provided"}, 400
            if "itemBannerText" in body and not normalized_item_id:
                return {"error": "itemId is required when itemBannerText is provided"}, 400

            if normalized_coupon_restaurant and normalized_item_id:
                menu_item = MenuService.get_menu_item(normalized_coupon_restaurant, normalized_item_id)
                if not menu_item:
                    return {"error": "Menu item not found for couponRestaurant and itemId"}, 404

            pk = f"COUPON#{code}"
            sk = "DETAILS"

            item = {
                'partitionkey': {'S': pk},
                'sortKey': {'S': sk},
                'couponType': {'S': str(coupon_type)},
                'couponValue': {'N': str(coupon_value)}
            }
            if start_date:
                item['startDate'] = {'S': str(start_date)}
            if end_date:
                item['endDate'] = {'S': str(end_date)}
            if normalized_issued_by:
                item['issuedBy'] = {'S': normalized_issued_by}
            if normalized_coupon_restaurant:
                item['couponRestaurant'] = {'S': normalized_coupon_restaurant}
            if normalized_item_id:
                item['couponItem'] = {'S': normalized_item_id}
            item['isOncePerUser'] = {'BOOL': is_once_per_user}

            dynamodb_client.put_item(
                TableName=TABLES['CONFIG'],
                Item=item
            )

            if normalized_coupon_restaurant and normalized_item_id:
                item_updates = {
                    'itemOfferCouponCode': str(code).strip(),
                }
                if 'itemBannerText' in body:
                    item_updates['topOfferBanner'] = item_banner_text
                MenuService.update_menu_item(normalized_coupon_restaurant, normalized_item_id, item_updates)

            metrics.add_metric(name="CouponSaved", unit="Count", value=1)
            response = {"message": "Coupon saved", "couponCode": code}
            if normalized_coupon_restaurant:
                response["couponRestaurant"] = normalized_coupon_restaurant
            if normalized_item_id:
                response["itemId"] = normalized_item_id
            if 'itemBannerText' in body:
                response["itemBannerText"] = item_banner_text
            return response, 200
        except Exception as e:
            logger.error("Error saving coupon", exc_info=True)
            return {"error": "Failed to save coupon", "message": str(e)}, 500

    @app.delete("/api/v1/coupons/<coupon_code>")
    @tracer.capture_method
    def delete_coupon(coupon_code: str):
        """Delete a coupon from config table"""
        try:
            coupon = CouponService.get_coupon(coupon_code)
            pk = f"COUPON#{coupon_code}"
            sk = "DETAILS"

            coupon_restaurant = str((coupon or {}).get("couponRestaurant") or "").strip()
            coupon_item = str((coupon or {}).get("couponItem") or "").strip()

            if coupon_restaurant and coupon_item:
                menu_item = MenuService.get_menu_item(coupon_restaurant, coupon_item)
                if menu_item:
                    MenuService.update_menu_item(
                        coupon_restaurant,
                        coupon_item,
                        {
                            "itemOfferCouponCode": None,
                            "topOfferBanner": None,
                        },
                    )

            dynamodb_client.delete_item(
                TableName=TABLES['CONFIG'],
                Key={
                    'partitionkey': {'S': pk},
                    'sortKey': {'S': sk}
                }
            )

            metrics.add_metric(name="CouponDeleted", unit="Count", value=1)
            return {"message": "Coupon deleted", "couponCode": coupon_code}, 200
        except Exception as e:
            logger.error("Error deleting coupon", exc_info=True)
            return {"error": "Failed to delete coupon", "message": str(e)}, 500
