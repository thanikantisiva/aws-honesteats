"""Delivery fee calculation routes"""
from datetime import datetime, timezone
from typing import Optional

from aws_lambda_powertools import Logger, Tracer, Metrics
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
    "riderSurgePricePerKm",
    "riderSurgeChargeAfterKms",
    "freeDeliveryAboveThreshold",
]
# Optional config keys — absent means feature is disabled / unlimited.
OPTIONAL_CONFIG_KEYS = [
    "maxDeliveryRadiusKm",
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
    """GST on food (5%), on customer delivery charge and platform fee (18% each)."""
    rounded_delivery = round(customer_delivery_fee, 2)
    rounded_platform = round(platform_fee, 2)
    return {
        "gstOnFood": round(item_total * 0.05, 2),
        "gstOnDeliveryFee": round(rounded_delivery * 0.18, 2),
        "gstOnPlatformFee": round(rounded_platform * 0.18, 2),
    }


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
            "surgeFee": 0.0,
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

    Fare tiers:
      - distance <= riderBaseFareApplicableUnderKms : flat riderBaseFare (minimum charge)
      - riderBaseFareApplicableUnderKms < distance <= riderSurgeChargeAfterKms : distance × riderFarePerKm
      - distance > riderSurgeChargeAfterKms : above + surge km × riderSurgePricePerKm

    Customer pays no delivery fee (isFreeDelivery) when there is NO surge and either:
      - itemTotal >= freeDeliveryAboveThreshold, or
      - distanceKm <= riderFreeDeliveryBelowKm (short-trip waiver; set to 0 to disable).
    """
    base_fare_km = config["riderBaseFareApplicableUnderKms"]
    surge_km_threshold = config["riderSurgeChargeAfterKms"]

    if distance_km <= base_fare_km:
        base_fee = config["riderBaseFare"]
        distance_fee = 0.0
        surge_fee = 0.0
    else:
        base_fee = 0.0
        normal_km = min(distance_km, surge_km_threshold)
        surge_km = max(0.0, distance_km - surge_km_threshold)
        distance_fee = normal_km * config["riderFarePerKm"]
        surge_fee = surge_km * config["riderSurgePricePerKm"]

    calculated_delivery_fee = base_fee + distance_fee + surge_fee

    # Surge zone blocks free delivery
    surge_active = distance_km > surge_km_threshold
    free_delivery_threshold = config["freeDeliveryAboveThreshold"]
    rider_free_below_km = config["riderFreeDeliveryBelowKm"]
    within_short_trip_waiver = rider_free_below_km > 0 and distance_km <= rider_free_below_km
    meets_cart_threshold = item_total >= free_delivery_threshold
    is_free_delivery = (not surge_active) and (meets_cart_threshold or within_short_trip_waiver)
    delivery_fee_discount = calculated_delivery_fee if is_free_delivery else 0.0
    final_delivery_fee = 0.0 if is_free_delivery else calculated_delivery_fee

    logger.info(
        "Calculated delivery km split: "
        f"distanceKm={distance_km}, baseFareKm={base_fare_km}, "
        f"surgeKmThreshold={surge_km_threshold}, surgeActive={surge_active}, "
        f"baseFee={base_fee}, distanceFee={distance_fee}, surgeFee={surge_fee}"
    )
    logger.info(
        "Free delivery evaluation: "
        f"itemTotal={item_total}, freeDeliveryAboveThreshold={free_delivery_threshold}, "
        f"riderFreeDeliveryBelowKm={rider_free_below_km}, withinShortTripWaiver={within_short_trip_waiver}, "
        f"surgeActive={surge_active}, isFreeDelivery={is_free_delivery}, "
        f"deliveryFeeDiscount={delivery_fee_discount}"
    )

    delivery_fee_rounded = round(final_delivery_fee, 2)
    platform_fee_rounded = round(config["platformFee"], 2)

    return {
        "deliveryFee": delivery_fee_rounded,
        "riderSettlementAmount": round(calculated_delivery_fee, 2),  # full earned fee regardless of free delivery
        "platformFee": platform_fee_rounded,
        "gst": _build_gst_breakdown(item_total, delivery_fee_rounded, platform_fee_rounded),
        "breakdown": {
            "baseFee": round(base_fee, 2),
            "distanceFee": round(distance_fee, 2),
            "surgeFee": round(surge_fee, 2),
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
            if not use_coords and distance_km_raw is None:
                return {
                    "error": (
                        "Provide itemTotal and either (addressLat, addressLng, restaurantLat, restaurantLng) "
                        "or distanceKm"
                    )
                }, 400
            if coupon_code and not restaurant_id:
                return {"error": "restaurantId is required when couponCode is provided"}, 400

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
            else:
                distance_km = float(distance_km_raw)
                distance_source = "client_supplied_distance"

            logger.info(
                "Delivery fee request received: "
                f"distanceKm={distance_km}, distanceSource={distance_source}, itemTotal={item_total}, "
                f"itemCount={item_count}, couponCode={coupon_code}, restaurantId={restaurant_id}"
            )

            config, missing_keys = _fetch_global_delivery_config()
            if not config:
                logger.warning(
                    "Global delivery config missing or invalid for calculate-fee: "
                    f"missingKeys={missing_keys}"
                )
                metrics.add_metric(name="DeliveryFeeConfigMissing", unit="Count", value=1)
                return _build_safe_zero_response(distance_km, missing_keys, item_total, distance_source), 200

            max_radius_km = config.get("maxDeliveryRadiusKm")  # None → unlimited
            logger.info(
                "Global delivery config loaded: "
                f"platformFee={config['platformFee']}, riderBaseFare={config['riderBaseFare']}, "
                f"riderFarePerKm={config['riderFarePerKm']}, "
                f"riderFreeDeliveryBelowKm={config['riderFreeDeliveryBelowKm']}, "
                f"riderSurgePricePerKm={config['riderSurgePricePerKm']}, "
                f"riderSurgeChargeAfterKms={config['riderSurgeChargeAfterKms']}, "
                f"freeDeliveryAboveThreshold={config['freeDeliveryAboveThreshold']}, "
                f"maxDeliveryRadiusKm={max_radius_km}"
            )

            # Reject the request early if the delivery location is outside the configured radius.
            if max_radius_km is not None and distance_km > max_radius_km:
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
                try:
                    coupon = CouponService.get_coupon(coupon_code)
                    if coupon:
                        coupon_type = str(coupon.get('couponType') or '').strip().lower()
                        coupon_value = float(coupon.get('couponValue') or 0.0)
                        is_once_per_user = bool(coupon.get('isOncePerUser'))
                        is_once_per_day = bool(coupon.get('isOncePerDay'))
                        coupon_target = str(coupon.get('couponTarget') or 'delivery').strip().lower()
                        min_order_value = coupon.get('minOrderValue')
                        coupon_issued_by = str(coupon.get('issuedBy') or '').strip().upper()

                        if not CouponService.is_coupon_active(coupon.get('startDate'), coupon.get('endDate')):
                            logger.info(
                                "Coupon not applied for calculate-fee: "
                                f"couponCode={coupon_code}, reason=inactive_or_invalid"
                            )
                            result['couponRejectedReason'] = 'inactive_or_invalid'
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
                                try:
                                    user_resp = dynamodb_client.get_item(
                                        TableName=TABLES['USERS'],
                                        Key={
                                            'phone': {'S': mobile_number},
                                            'role': {'S': 'CUSTOMER'},
                                        },
                                        ProjectionExpression='usedCoupons',
                                    )
                                    used_set = user_resp.get('Item', {}).get('usedCoupons', {}).get('SS', [])
                                    if coupon_code in used_set:
                                        logger.info(
                                            "Coupon already used by customer: "
                                            f"couponCode={coupon_code}, phone={mobile_number}"
                                        )
                                        coupon_applied = False
                                        result['couponApplied'] = coupon_applied
                                        result['couponRejectedReason'] = 'already_used'
                                        return result, 200
                                except Exception as e:
                                    logger.error(f"Error checking usedCoupons for {mobile_number}: {e}")

                            if is_once_per_day and mobile_number:
                                try:
                                    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
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
                                        logger.info(
                                            "Coupon already used today by customer: "
                                            f"couponCode={coupon_code}, phone={mobile_number}"
                                        )
                                        coupon_applied = False
                                        result['couponApplied'] = coupon_applied
                                        result['couponRejectedReason'] = 'already_used_today'
                                        return result, 200
                                except Exception as e:
                                    logger.error(f"Error checking daily coupon usage for {mobile_number}: {e}")

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
