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

    def _normalize_optional_string_field(value, field_name: str):
        """Treat null, empty, and whitespace-only as omitted; require string when value is sent."""
        if value is None:
            return None, None
        if not isinstance(value, str):
            return None, ({"error": f"{field_name} must be a string or null"}, 400)
        normalized = value.strip()
        if not normalized:
            return None, None
        return normalized, None

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
            normalized_coupon_restaurant, coupon_restaurant_error = _normalize_optional_string_field(
                body.get('couponRestaurant'), 'couponRestaurant'
            )
            normalized_item_id, item_id_error = _normalize_optional_string_field(
                body.get('itemId'), 'itemId'
            )
            item_banner_text, item_banner_error = _normalize_item_banner_text(body.get('itemBannerText'))
            normalized_description, description_error = _normalize_optional_string_field(
                body.get('description'), 'description'
            )
            is_once_per_user = body.get('isOncePerUser', False)
            if isinstance(is_once_per_user, str):
                is_once_per_user = is_once_per_user.strip().lower() in ('true', '1', 'yes')
            else:
                is_once_per_user = bool(is_once_per_user)

            is_once_per_day = body.get('isOncePerDay', False)
            if isinstance(is_once_per_day, str):
                is_once_per_day = is_once_per_day.strip().lower() in ('true', '1', 'yes')
            else:
                is_once_per_day = bool(is_once_per_day)

            coupon_target_raw = str(body.get('couponTarget') or 'delivery').strip().lower()
            coupon_target = 'order' if coupon_target_raw == 'order' else 'delivery'

            min_order_value = body.get('minOrderValue')
            if min_order_value is not None:
                try:
                    min_order_value = float(min_order_value)
                    if min_order_value < 0:
                        return {"error": "minOrderValue must be non-negative"}, 400
                except (TypeError, ValueError):
                    return {"error": "minOrderValue must be a number"}, 400

            if not code or not coupon_type or coupon_value is None:
                return {"error": "couponCode, couponType, couponValue are required"}, 400
            if coupon_restaurant_error:
                return coupon_restaurant_error
            if item_id_error:
                return item_id_error
            if item_banner_error:
                return item_banner_error
            if description_error:
                return description_error

            normalized_issued_by = str(issued_by).strip().upper() if issued_by is not None else None

            if normalized_issued_by == 'RESTAURANT' and not normalized_coupon_restaurant:
                return {"error": "couponRestaurant is required when issuedBy is RESTAURANT"}, 400
            if normalized_item_id and not normalized_coupon_restaurant:
                return {"error": "couponRestaurant is required when itemId is provided"}, 400
            # Only non-empty banner text counts as "provided" (null/empty/whitespace = omitted)
            if item_banner_text and not normalized_item_id:
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
            item['isOncePerDay'] = {'BOOL': is_once_per_day}
            item['couponTarget'] = {'S': coupon_target}
            if min_order_value is not None:
                item['minOrderValue'] = {'N': str(min_order_value)}
            if normalized_description:
                item['description'] = {'S': normalized_description}

            dynamodb_client.put_item(
                TableName=TABLES['CONFIG'],
                Item=item
            )

            if normalized_coupon_restaurant and normalized_item_id:
                item_updates = {
                    'itemOfferCouponCode': str(code).strip(),
                }
                if item_banner_text:
                    item_updates['topOfferBanner'] = item_banner_text
                MenuService.update_menu_item(normalized_coupon_restaurant, normalized_item_id, item_updates)

            metrics.add_metric(name="CouponSaved", unit="Count", value=1)
            response = {"message": "Coupon saved", "couponCode": code}
            if normalized_coupon_restaurant:
                response["couponRestaurant"] = normalized_coupon_restaurant
            if normalized_item_id:
                response["itemId"] = normalized_item_id
            if item_banner_text:
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

    @app.get("/api/v1/coupons/available")
    @tracer.capture_method
    def get_available_coupons():
        """Return all active, eligible coupons for a given restaurant and user."""
        try:
            restaurant_id = app.current_event.get_query_string_value('restaurantId')
            mobile_number = app.current_event.get_query_string_value('mobileNumber')

            # Scan ConfigTable for all COUPON# records (handles DynamoDB pagination)
            scan_kwargs = {
                'TableName': TABLES['CONFIG'],
                'FilterExpression': 'begins_with(partitionkey, :prefix) AND sortKey = :sk',
                'ExpressionAttributeValues': {
                    ':prefix': {'S': 'COUPON#'},
                    ':sk': {'S': 'DETAILS'},
                },
            }
            all_items = []
            while True:
                resp = dynamodb_client.scan(**scan_kwargs)
                all_items.extend(resp.get('Items', []))
                last_key = resp.get('LastEvaluatedKey')
                if not last_key:
                    break
                scan_kwargs['ExclusiveStartKey'] = last_key

            # Fetch the user's used-coupon set once — avoids N queries for isOncePerUser coupons
            used_coupon_codes: set = set()
            if mobile_number:
                try:
                    user_resp = dynamodb_client.get_item(
                        TableName=TABLES['USERS'],
                        Key={
                            'phone': {'S': mobile_number},
                            'role': {'S': 'CUSTOMER'},
                        },
                        ProjectionExpression='usedCoupons',
                    )
                    used_coupon_codes = set(
                        user_resp.get('Item', {}).get('usedCoupons', {}).get('SS', [])
                    )
                except Exception as e:
                    logger.error(f"Error fetching usedCoupons for {mobile_number}: {e}")

            eligible = []
            for item in all_items:
                # Exclude item-level coupons (tied to a specific menu item, not checkout-level)
                if item.get('couponItem', {}).get('S'):
                    continue

                raw_pk = item.get('partitionkey', {}).get('S', '')
                if not raw_pk.startswith('COUPON#'):
                    continue
                coupon_code = raw_pk[len('COUPON#'):]
                if not coupon_code:
                    continue

                coupon_type = str(item.get('couponType', {}).get('S', '')).strip().lower()
                coupon_value_raw = item.get('couponValue', {}).get('N')
                if not coupon_type or coupon_value_raw is None:
                    continue
                try:
                    coupon_value = float(coupon_value_raw)
                except (TypeError, ValueError):
                    continue

                start_date = item.get('startDate', {}).get('S')
                end_date = item.get('endDate', {}).get('S')
                is_once_per_user = bool(item.get('isOncePerUser', {}).get('BOOL', False))
                is_once_per_day = bool(item.get('isOncePerDay', {}).get('BOOL', False))
                coupon_target = str(item.get('couponTarget', {}).get('S') or 'delivery').strip().lower()
                min_order_value_raw = item.get('minOrderValue', {}).get('N')
                min_order_value = float(min_order_value_raw) if min_order_value_raw else None
                coupon_restaurant = str(item.get('couponRestaurant', {}).get('S') or '').strip()
                issued_by = str(item.get('issuedBy', {}).get('S') or '').strip()
                description = str(item.get('description', {}).get('S') or '').strip() or None

                # Must be within active date window
                if not CouponService.is_coupon_active(start_date, end_date):
                    continue

                # Must match this restaurant, or be a global coupon (no restriction)
                if coupon_restaurant and restaurant_id and coupon_restaurant != str(restaurant_id).strip():
                    continue

                # Skip if already used by this user (isOncePerUser enforcement)
                if is_once_per_user and mobile_number and coupon_code in used_coupon_codes:
                    continue

                # Skip if already used today (isOncePerDay enforcement)
                if is_once_per_day and mobile_number:
                    from datetime import datetime, timezone
                    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                    try:
                        daily_resp = dynamodb_client.get_item(
                            TableName=TABLES['USERS'],
                            Key={
                                'phone': {'S': mobile_number},
                                'role': {'S': f'DAILY_COUPONS#{today_str}'},
                            },
                            ProjectionExpression='usedToday',
                        )
                        used_today = set(daily_resp.get('Item', {}).get('usedToday', {}).get('SS', []))
                        if coupon_code in used_today:
                            continue
                    except Exception as e:
                        logger.error(f'Error checking daily coupon usage for {mobile_number}: {e}')

                entry: dict = {
                    'couponCode': coupon_code,
                    'couponType': coupon_type,
                    'couponValue': coupon_value,
                    'isOncePerUser': is_once_per_user,
                    'isOncePerDay': is_once_per_day,
                    'couponTarget': coupon_target,
                }
                if min_order_value is not None:
                    entry['minOrderValue'] = min_order_value
                if issued_by:
                    entry['issuedBy'] = issued_by
                if end_date:
                    entry['endDate'] = end_date
                if description:
                    entry['description'] = description

                eligible.append(entry)

            return {'coupons': eligible}, 200
        except Exception as e:
            logger.error("Error fetching available coupons", exc_info=True)
            return {'error': 'Failed to fetch coupons', 'message': str(e)}, 500
