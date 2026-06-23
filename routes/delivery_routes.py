"""Delivery fee calculation routes"""
from datetime import datetime, timezone
from typing import Optional

from aws_lambda_powertools import Logger, Tracer, Metrics
from config.pricing import compute_gst_breakdown
from services.coupon_config_service import coupons_enabled_now
from services.coupon_service import CouponService
from services.restaurant_service import RestaurantService
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python
from utils import normalize_phone

logger = Logger()
tracer = Tracer()
metrics = Metrics()

CONFIG_PK = "CONFIG#GLOBAL"
CONFIG_SK = "CONFIG"
REQUIRED_CONFIG_KEYS = [
    "platformFee",
    "riderBaseFare",
    "riderBaseFareApplicableUnderKms",
    "riderFarePerKm",
    "riderFreeDeliveryBelowKm",
    "freeDeliveryAboveThreshold",
]
# Optional config keys — absent means feature is disabled / unlimited.
# Customer-facing pricing override (decoupled from rider settlement):
#   * `customerViewRiderFarePerKm` — per-km rate charged to the customer for
#     the entire trip. The customer is billed `distance × customerViewRiderFarePerKm`
#     irrespective of distance (no flat short-zone component). When the key is
#     absent the customer side falls back to `riderFarePerKm`.
OPTIONAL_CONFIG_KEYS = [
    "maxDeliveryRadiusKm",
    "customerViewRiderFarePerKm",
]

def _to_float(value):
    """Safely parse numeric values from config payload."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fetch_global_delivery_config():
    """Fetch and validate global delivery fee config from config table."""
    response = dynamodb_client.get_item(
        TableName=TABLES["CONFIG"],
        Key={
            "partitionkey": {"S": CONFIG_PK},
            "sortKey": {"S": CONFIG_SK},
        },
    )
    item = response.get("Item")
    if not item:
        return None, REQUIRED_CONFIG_KEYS.copy()

    config_payload = dynamodb_to_python(item.get("config", {"NULL": True}))
    if not isinstance(config_payload, dict):
        return None, REQUIRED_CONFIG_KEYS.copy()

    parsed_config = {}
    missing_keys = []
    for key in REQUIRED_CONFIG_KEYS:
        parsed = _to_float(config_payload.get(key))
        if parsed is None:
            missing_keys.append(key)
        else:
            parsed_config[key] = parsed

    if missing_keys:
        return None, missing_keys

    # Parse optional keys — absence is not an error.
    for key in OPTIONAL_CONFIG_KEYS:
        parsed = _to_float(config_payload.get(key))
        if parsed is not None:
            parsed_config[key] = parsed

    return parsed_config, []


def _build_gst_breakdown(
    item_total: float, customer_delivery_fee: float, platform_fee: float
) -> dict:
    """GST on food (5%), on customer delivery charge and platform fee (18% each).

    Delegates to the shared `compute_gst_breakdown` so calculate-fee and the ops
    item-adjustment path use one source of truth for the rates.
    """
    return compute_gst_breakdown(item_total, customer_delivery_fee, platform_fee)


def _build_safe_zero_response(
    distance_km: float,
    missing_keys,
    item_total: float = 0.0,
    distance_source: Optional[str] = None,
):
    """Return a safe response when config is missing/invalid."""
    out = {
        "deliveryFee": 0.0,
        "riderSettlementAmount": 0.0,
        "platformFee": 0.0,
        "gst": _build_gst_breakdown(item_total, 0.0, 0.0),
        "breakdown": {
            "baseFee": 0.0,
            "distanceFee": 0.0,
            "deliveryFeeDiscount": 0.0,
            "couponDiscount": 0.0,
            "totalDiscount": 0.0,
            "discount": 0.0,
        },
        "freeDeliveryThreshold": 0.0,
        "isFreeDelivery": False,
        "distance": round(distance_km, 2),
        "couponApplied": False,
        "configMissing": True,
        "missingKeys": missing_keys,
    }
    if distance_source:
        out["distanceSource"] = distance_source
    return out


def calculate_delivery_fee(distance_km: float, item_total: float, config: dict) -> dict:
    """Calculate delivery fee using dynamic global config.

    Two independent fare schedules are supported:
      - rider settlement (always):
          * short zone : flat `riderBaseFare` (covers up to base_fare_km)
          * long  zone : `riderBaseFare + (distance − base_fare_km) × riderFarePerKm`
            i.e. base fare covers the first base_fare_km and only the *extra*
            kilometres are charged at the per-km rate. This guarantees the rider
            never earns less than `riderBaseFare` and the curve is monotonic
            across the boundary.
      - customer-facing bill (single, distance-only formula):
          * `distance × customerViewRiderFarePerKm` for ALL distances. There is
            no flat short-zone component on the customer side — the customer
            always pays the per-km rate × distance. When `customerViewRiderFarePerKm`
            is absent the customer rate falls back to `riderFarePerKm`.

    Customer pays no delivery fee (isFreeDelivery) when either:
      - itemTotal >= freeDeliveryAboveThreshold, or
      - distanceKm <= riderFreeDeliveryBelowKm (short-trip waiver; set to 0 to disable).

    The free-delivery waiver only zeroes out what the customer pays — the rider
    is still settled the full earned amount via `riderSettlementAmount`.
    """
    base_fare_km = config["riderBaseFareApplicableUnderKms"]
    rider_per_km = config["riderFarePerKm"]
    rider_base = config["riderBaseFare"]
    customer_per_km = config.get("customerViewRiderFarePerKm", rider_per_km)

    customer_base_fee = 0.0
    customer_distance_fee = distance_km * customer_per_km

    if distance_km <= base_fare_km:
        rider_base_fee = rider_base
        rider_distance_fee = 0.0
    else:
        # Rider: base fare covers the first base_fare_km; only the extra distance
        # is charged at the per-km rate. Guarantees rider settlement is monotonic
        # across the base_fare_km boundary (was previously a small drop just
        # past the threshold when riderBaseFare > base_fare_km × riderFarePerKm).
        rider_base_fee = rider_base
        rider_distance_fee = (distance_km - base_fare_km) * rider_per_km

    customer_calculated_fee = customer_base_fee + customer_distance_fee
    rider_calculated_fee = rider_base_fee + rider_distance_fee

    free_delivery_threshold = config["freeDeliveryAboveThreshold"]
    rider_free_below_km = config["riderFreeDeliveryBelowKm"]
    within_short_trip_waiver = rider_free_below_km > 0 and distance_km <= rider_free_below_km
    meets_cart_threshold = item_total >= free_delivery_threshold
    is_free_delivery = meets_cart_threshold or within_short_trip_waiver
    # Discount is what we waive from the customer's bill (rider is unaffected).
    delivery_fee_discount = customer_calculated_fee if is_free_delivery else 0.0
    final_delivery_fee = 0.0 if is_free_delivery else customer_calculated_fee

    logger.info(
        "Calculated delivery km split: "
        f"distanceKm={distance_km}, baseFareKm={base_fare_km}, "
        f"customerPerKm={customer_per_km}, "
        f"customerBaseFee={customer_base_fee}, customerDistanceFee={customer_distance_fee}, "
        f"riderBase={rider_base}, riderPerKm={rider_per_km}, "
        f"riderBaseFee={rider_base_fee}, riderDistanceFee={rider_distance_fee}, "
        f"customerCalculatedFee={customer_calculated_fee}, "
        f"riderCalculatedFee={rider_calculated_fee}"
    )
    logger.info(
        "Free delivery evaluation: "
        f"itemTotal={item_total}, freeDeliveryAboveThreshold={free_delivery_threshold}, "
        f"riderFreeDeliveryBelowKm={rider_free_below_km}, withinShortTripWaiver={within_short_trip_waiver}, "
        f"isFreeDelivery={is_free_delivery}, "
        f"deliveryFeeDiscount={delivery_fee_discount}"
    )

    delivery_fee_rounded = round(final_delivery_fee, 2)
    platform_fee_rounded = round(config["platformFee"], 2)

    return {
        "deliveryFee": delivery_fee_rounded,
        # Rider gets the full earned fee at the rider rate, regardless of customer-side waiver.
        "riderSettlementAmount": round(rider_calculated_fee, 2),
        "platformFee": platform_fee_rounded,
        "gst": _build_gst_breakdown(item_total, delivery_fee_rounded, platform_fee_rounded),
        "breakdown": {
            # Customer-facing breakdown (used by mobile cart for the line items).
            "baseFee": round(customer_base_fee, 2),
            "distanceFee": round(customer_distance_fee, 2),
            "deliveryFeeDiscount": round(delivery_fee_discount, 2),
            "couponDiscount": 0.0,
            "totalDiscount": round(delivery_fee_discount, 2),
            "discount": round(delivery_fee_discount, 2),  # backward compat for CartContext
        },
        "freeDeliveryThreshold": round(free_delivery_threshold, 2),
        "isFreeDelivery": is_free_delivery,
        "distance": round(distance_km, 2),
    }

def register_delivery_routes(app):
    """Register delivery fee calculation routes"""
    
    @app.post("/api/v1/delivery/calculate-fee")
    @tracer.capture_method
    def calculate_fee():
        """Calculate delivery fee based on distance, items, and total"""
        try:
            body = app.current_event.json_body or {}
            item_total = body.get('itemTotal')
            item_count = body.get('itemCount')  # Accepted for backward compatibility
            coupon_code = body.get('couponCode')
            restaurant_id = body.get('restaurantId')
            mobile_number = normalize_phone(body.get('mobileNumber'))

            address_lat = body.get('addressLat')
            address_lng = body.get('addressLng')
            restaurant_lat = body.get('restaurantLat')
            restaurant_lng = body.get('restaurantLng')
            distance_km_raw = body.get('distanceKm')

            use_coords = all(
                v is not None
                for v in (address_lat, address_lng, restaurant_lat, restaurant_lng)
            )

            if item_total is None:
                return {"error": "itemTotal is required"}, 400
            if coupon_code and not restaurant_id:
                return {"error": "restaurantId is required when couponCode is provided"}, 400
            # Coords and distanceKm are both optional: when neither is sent
            # the caller is doing a coupon-eligibility preview (no address
            # yet), and the delivery-fee calculation is skipped further down.

            item_total = float(item_total)

            if use_coords:
                try:
                    alat = float(address_lat)
                    alng = float(address_lng)
                    rlat = float(restaurant_lat)
                    rlng = float(restaurant_lng)
                except (TypeError, ValueError):
                    return {"error": "addressLat, addressLng, restaurantLat, restaurantLng must be numeric"}, 400

                # Road distance via Google Directions API (falls back to Haversine in RestaurantService)
                distance_km = RestaurantService.calculate_road_distance(rlat, rlng, alat, alng)
                distance_source = "google_directions_or_haversine_fallback"
                logger.info(
                    "Delivery fee request (coords): restaurant=(%s,%s) address=(%s,%s) -> distanceKm=%s source=%s",
                    rlat,
                    rlng,
                    alat,
                    alng,
                    distance_km,
                    distance_source,
                )
            elif distance_km_raw is not None:
                distance_km = float(distance_km_raw)
                distance_source = "client_supplied_distance"
            else:
                # Preview mode — no address yet. Delivery-fee math is skipped
                # below; coupon eligibility is still evaluated.
                distance_km = None
                distance_source = "none"

            logger.info(
                "Delivery fee request received: "
                f"distanceKm={distance_km}, distanceSource={distance_source}, itemTotal={item_total}, "
                f"itemCount={item_count}, couponCode={coupon_code}, restaurantId={restaurant_id}, "
                f"mobileNumber={mobile_number!r}, mobileNumberPresent={bool(mobile_number)}"
            )

            config, missing_keys = _fetch_global_delivery_config()
            if not config:
                logger.warning(
                    "Global delivery config missing or invalid for calculate-fee: "
                    f"missingKeys={missing_keys}"
                )
                metrics.add_metric(name="DeliveryFeeConfigMissing", unit="Count", value=1)
                return _build_safe_zero_response(distance_km or 0.0, missing_keys, item_total, distance_source), 200

            max_radius_km = config.get("maxDeliveryRadiusKm")  # None → unlimited
            customer_per_km_cfg = config.get("customerViewRiderFarePerKm")  # None → falls back to riderFarePerKm
            logger.info(
                "Global delivery config loaded: "
                f"platformFee={config['platformFee']}, riderBaseFare={config['riderBaseFare']}, "
                f"riderFarePerKm={config['riderFarePerKm']}, "
                f"customerViewRiderFarePerKm={customer_per_km_cfg}, "
                f"riderFreeDeliveryBelowKm={config['riderFreeDeliveryBelowKm']}, "
                f"freeDeliveryAboveThreshold={config['freeDeliveryAboveThreshold']}, "
                f"maxDeliveryRadiusKm={max_radius_km}"
            )

            # Reject the request early if the delivery location is outside the configured radius.
            # Guard: max_radius_km must be > 0 to prevent an accidental zero value blocking all deliveries.
            # In preview mode (no coords/distanceKm) distance_km is None — there is
            # nothing to compare against, so the radius check is skipped.
            if (
                distance_km is not None
                and max_radius_km is not None
                and max_radius_km > 0
                and distance_km > max_radius_km
            ):
                logger.info(
                    f"Delivery radius exceeded: distanceKm={distance_km}, maxDeliveryRadiusKm={max_radius_km}"
                )
                metrics.add_metric(name="DeliveryRadiusExceeded", unit="Count", value=1)
                return {
                    "outsideDeliveryRadius": True,
                    "maxDeliveryRadiusKm": max_radius_km,
                    "distance": round(distance_km, 2),
                    "distanceSource": distance_source,
                }, 200

            if distance_km is None:
                # Preview mode — no address. Skip the delivery-fee math and
                # build a zeroed response of the same shape so the coupon
                # block (and the client) can consume it unchanged. No new
                # keys are introduced; clients infer preview from the zeroed
                # fee fields and the empty/`none` distanceSource.
                platform_fee_rounded = round(config["platformFee"], 2)
                result = {
                    "deliveryFee": 0.0,
                    "riderSettlementAmount": 0.0,
                    "platformFee": platform_fee_rounded,
                    "gst": _build_gst_breakdown(item_total, 0.0, platform_fee_rounded),
                    "breakdown": {
                        "baseFee": 0.0,
                        "distanceFee": 0.0,
                        "deliveryFeeDiscount": 0.0,
                        "couponDiscount": 0.0,
                        "totalDiscount": 0.0,
                        "discount": 0.0,
                    },
                    "freeDeliveryThreshold": round(config["freeDeliveryAboveThreshold"], 2),
                    "isFreeDelivery": False,
                    "distance": 0.0,
                }
                logger.info(
                    "Delivery fee skipped (no coords/distanceKm): "
                    f"itemTotal={item_total}, couponCode={coupon_code}, "
                    f"restaurantId={restaurant_id}, mobileNumberPresent={bool(mobile_number)}"
                )
            else:
                result = calculate_delivery_fee(
                    distance_km=distance_km,
                    item_total=item_total,
                    config=config
                )
            result["distanceSource"] = distance_source
            result["outsideDeliveryRadius"] = False
            if max_radius_km is not None:
                result["maxDeliveryRadiusKm"] = max_radius_km

            # Coupon is informational only: discount is reported but deliveryFee is unchanged.
            coupon_applied = False
            if coupon_code:
                # Global coupon kill-switch / time window (CONFIG#COUPONS).
                if not coupons_enabled_now():
                    logger.info(
                        "Coupon not applied — coupon usage globally disabled / outside window: "
                        f"couponCode={coupon_code}"
                    )
                    result['couponApplied'] = False
                    result['couponRejectedReason'] = 'coupons_disabled'
                    return result, 200
                try:
                    # Fetch coupon record + user usage records in one BatchGetItem round trip
                    # instead of 2-3 sequential GetItem calls.
                    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                    _batch_request = {
                        TABLES['CONFIG']: {
                            'Keys': [
                                {
                                    'partitionkey': {'S': f'COUPON#{coupon_code}'},
                                    'sortKey': {'S': 'DETAILS'},
                                }
                            ]
                        }
                    }
                    if mobile_number:
                        # NOTE: `role` MUST be in the projection — DynamoDB's ProjectionExpression
                        # does NOT auto-include primary key attributes, and we key the response by
                        # `role` below to distinguish the CUSTOMER row from the DAILY_COUPONS row.
                        # `role` is a DynamoDB reserved word, so it must be aliased.
                        _batch_request[TABLES['USERS']] = {
                            'Keys': [
                                {'phone': {'S': mobile_number}, 'role': {'S': 'CUSTOMER'}},
                                {'phone': {'S': mobile_number}, 'role': {'S': f'DAILY_COUPONS#{today_str}'}},
                            ],
                            'ProjectionExpression': '#r, usedCoupons, usedToday',
                            'ExpressionAttributeNames': {'#r': 'role'},
                        }
                    logger.info(
                        "Coupon BatchGetItem request: "
                        f"couponCode={coupon_code}, mobileNumber={mobile_number!r}, "
                        f"todayStr={today_str}, "
                        f"configTable={TABLES['CONFIG']}, usersTable={TABLES['USERS']}, "
                        f"usersKeyCount={len(_batch_request.get(TABLES['USERS'], {}).get('Keys', []))}"
                    )
                    _batch_resp = dynamodb_client.batch_get_item(RequestItems=_batch_request)
                    if _batch_resp.get('UnprocessedKeys'):
                        logger.warning(
                            "BatchGetItem returned UnprocessedKeys during coupon validation: "
                            f"couponCode={coupon_code}, unprocessedKeys={_batch_resp.get('UnprocessedKeys')!r}"
                        )
                    _responses = _batch_resp.get('Responses', {})

                    # Parse coupon from batch response
                    _config_items = _responses.get(TABLES['CONFIG'], [])
                    coupon = CouponService.parse_coupon_item(coupon_code, _config_items[0] if _config_items else None)

                    # Pre-fetched user records keyed by role (only present when mobile_number was sent)
                    _users_response_items = _responses.get(TABLES['USERS'], [])
                    logger.info(
                        "Coupon BatchGetItem response (USERS): "
                        f"couponCode={coupon_code}, returnedItemCount={len(_users_response_items)}, "
                        f"itemAttributeKeys={[sorted(it.keys()) for it in _users_response_items]}, "
                        f"rawItems={_users_response_items!r}"
                    )
                    _user_items = {
                        i.get('role', {}).get('S', ''): i
                        for i in _users_response_items
                    }
                    _customer_item = _user_items.get('CUSTOMER')
                    _daily_item = _user_items.get(f'DAILY_COUPONS#{today_str}')
                    logger.info(
                        "Coupon user-row bucketing: "
                        f"couponCode={coupon_code}, bucketedRoleKeys={list(_user_items.keys())}, "
                        f"customerItemFound={_customer_item is not None}, "
                        f"dailyItemFound={_daily_item is not None}, "
                        f"customerItemKeys={sorted(_customer_item.keys()) if _customer_item else None}, "
                        f"dailyItemKeys={sorted(_daily_item.keys()) if _daily_item else None}"
                    )

                    if coupon:
                        coupon_type = str(coupon.get('couponType') or '').strip().lower()
                        coupon_value = float(coupon.get('couponValue') or 0.0)
                        is_once_per_user = bool(coupon.get('isOncePerUser'))
                        is_once_per_day = bool(coupon.get('isOncePerDay'))
                        coupon_target = str(coupon.get('couponTarget') or 'delivery').strip().lower()
                        min_order_value = coupon.get('minOrderValue')
                        coupon_issued_by = str(coupon.get('issuedBy') or '').strip().upper()
                        logger.info(
                            "Coupon parsed from config: "
                            f"couponCode={coupon_code}, couponType={coupon_type}, couponValue={coupon_value}, "
                            f"isOncePerUser={is_once_per_user}, isOncePerDay={is_once_per_day}, "
                            f"couponTarget={coupon_target}, minOrderValue={min_order_value}, "
                            f"issuedBy={coupon_issued_by}, "
                            f"couponRestaurant={coupon.get('couponRestaurant')!r}, "
                            f"targetCustomerCount={len(coupon.get('targetCustomerPhones') or [])}"
                        )

                        if not CouponService.is_coupon_active(coupon.get('startDate'), coupon.get('endDate')):
                            logger.info(
                                "Coupon not applied for calculate-fee: "
                                f"couponCode={coupon_code}, reason=inactive_or_invalid"
                            )
                            result['couponRejectedReason'] = 'inactive_or_invalid'
                        elif CouponService.is_coupon_blocked_for_restaurant(coupon_code, restaurant_id):
                            logger.info(
                                "Coupon rejected due to restaurant blocklist: "
                                f"couponCode={coupon_code}, restaurantId={restaurant_id}"
                            )
                            result['couponRejectedReason'] = 'coupon_blocked_for_restaurant'
                            result['couponApplied'] = False
                            return result, 200
                        elif not CouponService.is_coupon_valid_for_customer(coupon, mobile_number):
                            logger.info(
                                "Coupon rejected due to targeted-customer mismatch: "
                                f"couponCode={coupon_code}, mobileNumberPresent={bool(mobile_number)}, "
                                f"targetCustomerCount={len(coupon.get('targetCustomerPhones') or [])}"
                            )
                            result['couponRejectedReason'] = 'customer_not_eligible'
                            result['couponApplied'] = False
                            return result, 200
                        elif not CouponService.is_coupon_valid_for_restaurant(coupon, restaurant_id):
                            logger.info(
                                "Coupon rejected due to restaurant mismatch: "
                                f"couponCode={coupon_code}, restaurantId={restaurant_id}, "
                                f"couponRestaurant={coupon.get('couponRestaurant')}, issuedBy={coupon_issued_by}"
                            )
                            result['couponRejectedReason'] = 'restaurant_mismatch'
                            result['couponApplied'] = False
                            return result, 200
                        else:
                            if is_once_per_user and mobile_number:
                                used_set = (_customer_item or {}).get('usedCoupons', {}).get('SS', [])
                                logger.info(
                                    "Once-per-user check evaluating: "
                                    f"couponCode={coupon_code}, phone={mobile_number}, "
                                    f"customerItemPresent={_customer_item is not None}, "
                                    f"usedCouponsRaw={(_customer_item or {}).get('usedCoupons')!r}, "
                                    f"usedSet={used_set!r}, "
                                    f"couponInUsedSet={coupon_code in used_set}"
                                )
                                if coupon_code in used_set:
                                    logger.info(
                                        "Coupon already used by customer: "
                                        f"couponCode={coupon_code}, phone={mobile_number}"
                                    )
                                    coupon_applied = False
                                    result['couponApplied'] = coupon_applied
                                    result['couponRejectedReason'] = 'already_used'
                                    return result, 200
                            else:
                                logger.info(
                                    "Once-per-user check skipped: "
                                    f"couponCode={coupon_code}, isOncePerUser={is_once_per_user}, "
                                    f"mobileNumberPresent={bool(mobile_number)}"
                                )

                            if is_once_per_day and mobile_number:
                                used_today = set((_daily_item or {}).get('usedToday', {}).get('SS', []))
                                logger.info(
                                    "Once-per-day check evaluating: "
                                    f"couponCode={coupon_code}, phone={mobile_number}, "
                                    f"dailyItemPresent={_daily_item is not None}, "
                                    f"usedTodayRaw={(_daily_item or {}).get('usedToday')!r}, "
                                    f"usedTodaySet={used_today!r}, "
                                    f"couponInUsedToday={coupon_code in used_today}"
                                )
                                if coupon_code in used_today:
                                    logger.info(
                                        "Coupon already used today by customer: "
                                        f"couponCode={coupon_code}, phone={mobile_number}"
                                    )
                                    coupon_applied = False
                                    result['couponApplied'] = coupon_applied
                                    result['couponRejectedReason'] = 'already_used_today'
                                    return result, 200
                            else:
                                logger.info(
                                    "Once-per-day check skipped: "
                                    f"couponCode={coupon_code}, isOncePerDay={is_once_per_day}, "
                                    f"mobileNumberPresent={bool(mobile_number)}"
                                )

                            # Enforce minimum order value
                            if min_order_value and item_total < min_order_value:
                                logger.info(
                                    "Coupon rejected: item_total below minOrderValue: "
                                    f"couponCode={coupon_code}, item_total={item_total}, "
                                    f"minOrderValue={min_order_value}"
                                )
                                coupon_applied = False
                                result['couponApplied'] = coupon_applied
                                result['couponRejectedReason'] = 'below_min_order'
                                result['couponMinOrderValue'] = min_order_value
                                return result, 200

                            delivery_fee = result['deliveryFee']
                            coupon_discount = 0.0

                            if coupon_type == 'percentage':
                                base = item_total if coupon_target == 'order' else delivery_fee
                                coupon_discount = (base * coupon_value) / 100.0
                            elif coupon_type == 'fixed':
                                coupon_discount = coupon_value
                            else:
                                logger.info(
                                    "Coupon not applied for calculate-fee: "
                                    f"couponCode={coupon_code}, reason=unsupported_type, couponType={coupon_type}"
                                )
                                result['couponRejectedReason'] = 'inactive_or_invalid'
                                coupon_discount = 0.0

                            # Do not reduce deliveryFee; only report coupon discount
                            result['breakdown']['couponDiscount'] = round(coupon_discount, 2)
                            result['breakdown']['totalDiscount'] = round(
                                result['breakdown'].get('deliveryFeeDiscount', 0) + coupon_discount, 2
                            )
                            result['breakdown']['discount'] = result['breakdown']['totalDiscount']  # backward compat
                            coupon_applied = coupon_discount > 0
                            logger.info(
                                "Coupon evaluated successfully for calculate-fee: "
                                f"couponCode={coupon_code}, couponType={coupon_type}, "
                                f"couponValue={coupon_value}, couponDiscount={coupon_discount}, "
                                f"deliveryFeeUnchanged={result['deliveryFee']}"
                            )
                    else:
                        logger.info(
                            "Coupon not found for calculate-fee: "
                            f"couponCode={coupon_code}"
                        )
                        result['couponRejectedReason'] = 'inactive_or_invalid'
                except Exception as e:
                    logger.error(f"Error applying coupon {coupon_code}: {str(e)}")

            result['couponApplied'] = coupon_applied
            if coupon_applied and coupon_code:
                result['couponCode'] = coupon_code
            if mobile_number:
                result['mobileNumber'] = mobile_number

            # YumCoins redemption preview (informational; authoritatively
            # re-applied at checkout). Reduces the FOOD value the customer pays.
            try:
                coins_to_redeem = body.get('coinsToRedeem', 0)
                if coins_to_redeem and mobile_number:
                    from services.redemption_service import RedemptionService
                    rq = RedemptionService.quote(mobile_number, coins_to_redeem, item_total)
                    coin_discount = float(rq.get('coinDiscount', 0.0) or 0.0)
                    result['coinsRedeemed'] = rq.get('coinsApplied', 0)
                    result['coinDiscount'] = coin_discount
                    result['coinConversionRate'] = rq.get('rate')
                    if rq.get('reason'):
                        result['coinRejectedReason'] = rq.get('reason')
                    result['breakdown']['coinDiscount'] = round(coin_discount, 2)
                    result['breakdown']['totalDiscount'] = round(
                        result['breakdown'].get('totalDiscount', 0) + coin_discount, 2
                    )
                    result['breakdown']['discount'] = result['breakdown']['totalDiscount']
            except Exception as coin_err:
                logger.warning(f"Coin redemption preview failed: {coin_err}")

            logger.info(
                "Delivery fee calculation completed: "
                f"deliveryFee={result['deliveryFee']}, platformFee={result['platformFee']}, "
                f"isFreeDelivery={result['isFreeDelivery']}, couponApplied={coupon_applied}"
            )

            metrics.add_metric(name="DeliveryFeeCalculated", unit="Count", value=1)
            if result['isFreeDelivery']:
                metrics.add_metric(name="FreeDeliveryApplied", unit="Count", value=1)

            return result, 200

        except ValueError:
            return {
                "error": "Numeric fields (itemTotal, coordinates or distanceKm) are invalid"
            }, 400
        except Exception as e:
            logger.error("Error calculating delivery fee", exc_info=True)
            metrics.add_metric(name="DeliveryFeeCalculationFailed", unit="Count", value=1)
            return {"error": "Failed to calculate delivery fee", "message": str(e)}, 500
