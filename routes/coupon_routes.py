"""Coupon routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.coupon_config_service import coupons_enabled_now
from services.coupon_service import CouponService
from services.menu_service import MenuService
from utils import normalize_phone
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python

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

    def _normalize_item_ids(body: dict, fallback_item_id):
        """Normalize itemId/itemIds payloads to a de-duplicated list of item IDs."""
        raw_values = []
        if fallback_item_id:
            raw_values.append(fallback_item_id)

        multi_value = None
        for field_name in ("itemIds", "item_ids", "couponItems"):
            if field_name in body and body[field_name] is not None:
                multi_value = body[field_name]
                break

        if multi_value is not None:
            if isinstance(multi_value, str):
                raw_values.extend(part.strip() for part in multi_value.replace("\n", ",").split(","))
            elif isinstance(multi_value, (list, tuple, set)):
                for item_id in multi_value:
                    if not isinstance(item_id, str):
                        return [], ({"error": "itemIds must contain only strings"}, 400)
                    raw_values.append(item_id.strip())
            else:
                return [], ({"error": "itemIds must be an array of strings or a comma-separated string"}, 400)

        normalized = []
        seen = set()
        for item_id in raw_values:
            value = str(item_id or "").strip()
            if value and value not in seen:
                normalized.append(value)
                seen.add(value)
        return normalized, None

    def _coupon_item_ids(coupon: dict) -> list:
        """Return item IDs from either legacy couponItem or multi-item couponItems."""
        item_ids = []
        seen = set()
        for item_id in [coupon.get("couponItem"), *(coupon.get("couponItems") or [])]:
            value = str(item_id or "").strip()
            if value and value not in seen:
                item_ids.append(value)
                seen.add(value)
        return item_ids

    def _normalize_target_customer_phones(body: dict):
        """Normalize optional targeted-customer phone lists for special coupons."""
        field_names = ("targetCustomerPhones", "targetCustomers", "eligibleCustomerPhones")
        values = [body[name] for name in field_names if name in body and body[name] is not None]
        if not values:
            return [], False, None

        raw_phones = []
        for value in values:
            if isinstance(value, str):
                raw_phones.extend(part.strip() for part in value.split(","))
            elif isinstance(value, (list, tuple, set)):
                for phone in value:
                    if not isinstance(phone, str):
                        return [], True, ({"error": "targetCustomerPhones must contain only strings"}, 400)
                    raw_phones.append(phone.strip())
            else:
                return [], True, (
                    {"error": "targetCustomerPhones must be an array of phone strings or a comma-separated string"},
                    400,
                )

        normalized = []
        seen = set()
        for phone in raw_phones:
            if not phone:
                continue
            normalized_phone = normalize_phone(phone)
            if normalized_phone and normalized_phone not in seen:
                normalized.append(normalized_phone)
                seen.add(normalized_phone)

        if not normalized:
            return [], True, ({"error": "targetCustomerPhones must include at least one phone when provided"}, 400)
        return normalized, True, None

    def _scan_all(scan_kwargs: dict):
        items = []
        while True:
            resp = dynamodb_client.scan(**scan_kwargs)
            items.extend(resp.get('Items', []))
            last_key = resp.get('LastEvaluatedKey')
            if not last_key:
                return items
            scan_kwargs['ExclusiveStartKey'] = last_key

    def _coupon_code_from_item(item: dict) -> str:
        raw_pk = item.get('partitionkey', {}).get('S', '')
        return raw_pk[len('COUPON#'):] if raw_pk.startswith('COUPON#') else ''

    def _coupon_from_item(item: dict, usage_counts=None):
        coupon_code = _coupon_code_from_item(item)
        if not coupon_code:
            return None

        coupon_value_raw = item.get('couponValue', {}).get('N')
        try:
            coupon_value = float(coupon_value_raw) if coupon_value_raw is not None else 0
        except (TypeError, ValueError):
            coupon_value = 0

        target_customer_phones = item.get('targetCustomerPhones', {}).get('SS', [])
        coupon = {
            'couponCode': coupon_code,
            'couponType': str(item.get('couponType', {}).get('S') or '').strip().lower(),
            'couponValue': coupon_value,
            'isOncePerUser': bool(item.get('isOncePerUser', {}).get('BOOL', False)),
            'isOncePerDay': bool(item.get('isOncePerDay', {}).get('BOOL', False)),
            'couponTarget': str(item.get('couponTarget', {}).get('S') or 'delivery').strip().lower(),
            'uses': int((usage_counts or {}).get(coupon_code, 0)),
            'targetCustomerCount': len(target_customer_phones),
        }

        optional_string_fields = (
            'startDate',
            'endDate',
            'issuedBy',
            'couponRestaurant',
            'couponItem',
            'description',
        )
        for field in optional_string_fields:
            value = str(item.get(field, {}).get('S') or '').strip()
            if value:
                coupon[field] = value

        coupon_items = item.get('couponItems', {}).get('SS', [])
        if coupon_items:
            coupon['couponItems'] = sorted(coupon_items)

        min_order_value_raw = item.get('minOrderValue', {}).get('N')
        if min_order_value_raw is not None:
            try:
                coupon['minOrderValue'] = float(min_order_value_raw)
            except (TypeError, ValueError):
                pass
        if target_customer_phones:
            coupon['targetCustomerPhones'] = target_customer_phones
        return coupon

    def _coupon_usage_counts() -> dict:
        counts: dict[str, int] = {}
        orders = _scan_all({
            'TableName': TABLES['ORDERS'],
            'ProjectionExpression': '#rev, couponCode',
            'ExpressionAttributeNames': {'#rev': 'revenue'},
        })
        for order in orders:
            code = ''
            if 'revenue' in order:
                revenue = dynamodb_to_python(order.get('revenue'))
                if isinstance(revenue, dict) and revenue.get('couponApplied'):
                    code = str(revenue.get('couponCode') or '').strip()
            if not code:
                code = str(order.get('couponCode', {}).get('S') or '').strip()
            if code:
                counts[code] = counts.get(code, 0) + 1
        return counts

    @app.get("/api/v1/coupons")
    @tracer.capture_method
    def list_coupons():
        """Return the admin coupon inventory with scope fields and total usage counts."""
        try:
            all_items = _scan_all({
                'TableName': TABLES['CONFIG'],
                'FilterExpression': 'begins_with(partitionkey, :prefix) AND sortKey = :sk',
                'ExpressionAttributeValues': {
                    ':prefix': {'S': 'COUPON#'},
                    ':sk': {'S': 'DETAILS'},
                },
            })
            usage_counts = _coupon_usage_counts()
            coupons = [
                coupon for coupon in
                (_coupon_from_item(item, usage_counts) for item in all_items)
                if coupon is not None
            ]
            coupons.sort(key=lambda c: c.get('couponCode', ''))
            return {'coupons': coupons}, 200
        except Exception as e:
            logger.error("Error listing coupons", exc_info=True)
            return {'error': 'Failed to list coupons', 'message': str(e)}, 500

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
            target_customer_phones, target_customer_phones_provided, target_customer_phones_error = (
                _normalize_target_customer_phones(body)
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
            if coupon_target_raw == 'item':
                coupon_target = 'item'
            elif coupon_target_raw == 'order':
                coupon_target = 'order'
            else:
                coupon_target = 'delivery'

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
            if target_customer_phones_error:
                return target_customer_phones_error

            normalized_item_ids, item_ids_error = _normalize_item_ids(body, normalized_item_id)
            if item_ids_error:
                return item_ids_error

            normalized_issued_by = str(issued_by).strip().upper() if issued_by is not None else None
            has_item_scope = bool(normalized_item_ids)

            if normalized_issued_by == 'RESTAURANT' and not normalized_coupon_restaurant:
                return {"error": "couponRestaurant is required when issuedBy is RESTAURANT"}, 400
            if has_item_scope and not normalized_coupon_restaurant:
                return {"error": "couponRestaurant is required when itemIds are provided"}, 400
            if coupon_target == 'item' and not has_item_scope:
                return {"error": "itemIds are required when couponTarget is item"}, 400
            if has_item_scope:
                coupon_target = 'item'
            if target_customer_phones_provided and has_item_scope:
                return {"error": "targetCustomerPhones is supported only for checkout coupons, not item coupons"}, 400
            # Only non-empty banner text counts as "provided" (null/empty/whitespace = omitted)
            if item_banner_text and not has_item_scope:
                return {"error": "itemIds are required when itemBannerText is provided"}, 400

            if normalized_coupon_restaurant and has_item_scope:
                for selected_item_id in normalized_item_ids:
                    menu_item = MenuService.get_menu_item(normalized_coupon_restaurant, selected_item_id)
                    if not menu_item:
                        return {"error": f"Menu item not found: {selected_item_id}"}, 404

            pk = f"COUPON#{code}"
            sk = "DETAILS"
            existing_coupon = CouponService.get_coupon(code)

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
            if len(normalized_item_ids) == 1:
                item['couponItem'] = {'S': normalized_item_ids[0]}
            elif len(normalized_item_ids) > 1:
                item['couponItems'] = {'SS': normalized_item_ids}
            item['isOncePerUser'] = {'BOOL': is_once_per_user}
            item['isOncePerDay'] = {'BOOL': is_once_per_day}
            item['couponTarget'] = {'S': coupon_target}
            if min_order_value is not None:
                item['minOrderValue'] = {'N': str(min_order_value)}
            if normalized_description:
                item['description'] = {'S': normalized_description}
            if target_customer_phones_provided:
                item['targetCustomerPhones'] = {'SS': target_customer_phones}

            dynamodb_client.put_item(
                TableName=TABLES['CONFIG'],
                Item=item
            )

            if normalized_coupon_restaurant and has_item_scope:
                previous_item_ids = set(_coupon_item_ids(existing_coupon or {}))
                previous_restaurant = str((existing_coupon or {}).get("couponRestaurant") or normalized_coupon_restaurant).strip()
                selected_item_ids = set(normalized_item_ids)
                removed_item_ids = previous_item_ids - selected_item_ids

                for removed_item_id in removed_item_ids:
                    menu_item = MenuService.get_menu_item(previous_restaurant, removed_item_id)
                    if menu_item and menu_item.item_offer_coupon_code == str(code).strip():
                        MenuService.update_menu_item(
                            previous_restaurant,
                            removed_item_id,
                            {
                                "itemOfferCouponCode": None,
                                "topOfferBanner": None,
                            },
                        )

                item_updates = {
                    'itemOfferCouponCode': str(code).strip(),
                }
                if item_banner_text:
                    item_updates['topOfferBanner'] = item_banner_text
                for selected_item_id in normalized_item_ids:
                    MenuService.update_menu_item(normalized_coupon_restaurant, selected_item_id, item_updates)

            metrics.add_metric(name="CouponSaved", unit="Count", value=1)
            response = {"message": "Coupon saved", "couponCode": code}
            if normalized_coupon_restaurant:
                response["couponRestaurant"] = normalized_coupon_restaurant
            if len(normalized_item_ids) == 1:
                response["itemId"] = normalized_item_ids[0]
            elif len(normalized_item_ids) > 1:
                response["itemIds"] = normalized_item_ids
            if item_banner_text:
                response["itemBannerText"] = item_banner_text
            if target_customer_phones_provided:
                response["targetCustomerCount"] = len(target_customer_phones)
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
            coupon_item_ids = _coupon_item_ids(coupon or {})

            if coupon_restaurant and coupon_item_ids:
                for coupon_item in coupon_item_ids:
                    menu_item = MenuService.get_menu_item(coupon_restaurant, coupon_item)
                    if not menu_item or menu_item.item_offer_coupon_code != str(coupon_code).strip():
                        continue
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
            # Global coupon kill-switch / time window — hide all coupons when off.
            if not coupons_enabled_now():
                return {'coupons': []}, 200

            restaurant_id = app.current_event.get_query_string_value('restaurantId')
            mobile_number = normalize_phone(app.current_event.get_query_string_value('mobileNumber'))
            blocked_coupon_codes = CouponService.get_blocked_coupon_codes_for_restaurant(restaurant_id)

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
                if item.get('couponItems', {}).get('SS'):
                    continue
                # Exclude coupons whose target is the item itself (item-scoped, not checkout-level)
                if (item.get('couponTarget', {}).get('S') or '').strip().lower() == 'item':
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
                target_customer_phones = item.get('targetCustomerPhones', {}).get('SS', [])

                # Must be within active date window
                if not CouponService.is_coupon_active(start_date, end_date):
                    continue

                # Hide globally blocked coupons for this restaurant from the customer app.
                if coupon_code.upper() in blocked_coupon_codes:
                    continue

                # Must match this restaurant, or be a global coupon (no restriction)
                if coupon_restaurant and restaurant_id and coupon_restaurant != str(restaurant_id).strip():
                    continue

                # Targeted coupons are visible only to selected customers.
                if not CouponService.is_coupon_valid_for_customer(
                    {'targetCustomerPhones': target_customer_phones},
                    mobile_number,
                ):
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
                if coupon_restaurant:
                    entry['couponRestaurant'] = coupon_restaurant
                if end_date:
                    entry['endDate'] = end_date
                if description:
                    entry['description'] = description

                eligible.append(entry)

            return {'coupons': eligible}, 200
        except Exception as e:
            logger.error("Error fetching available coupons", exc_info=True)
            return {'error': 'Failed to fetch coupons', 'message': str(e)}, 500
