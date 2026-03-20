"""Delivery fee calculation routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python
from datetime import datetime, timezone

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
    return parsed_config, []


def _build_safe_zero_response(distance_km: float, missing_keys):
    """Return a safe response when config is missing/invalid."""
    return {
        "deliveryFee": 0.0,
        "riderSettlementAmount": 0.0,
        "platformFee": 0.0,
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


def calculate_delivery_fee(distance_km: float, item_total: float, config: dict) -> dict:
    """Calculate delivery fee using dynamic global config.

    Fare tiers:
      - distance <= riderBaseFareApplicableUnderKms : flat riderBaseFare (minimum charge)
      - riderBaseFareApplicableUnderKms < distance <= riderSurgeChargeAfterKms : distance × riderFarePerKm
      - distance > riderSurgeChargeAfterKms : above + surge km × riderSurgePricePerKm

    Free delivery only applies when there is NO surge (distance <= riderSurgeChargeAfterKms)
    and itemTotal >= freeDeliveryAboveThreshold.
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
    is_free_delivery = (not surge_active) and (item_total >= free_delivery_threshold)
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
        f"surgeActive={surge_active}, isFreeDelivery={is_free_delivery}, "
        f"deliveryFeeDiscount={delivery_fee_discount}"
    )

    return {
        "deliveryFee": round(final_delivery_fee, 2),
        "riderSettlementAmount": round(calculated_delivery_fee, 2),  # full earned fee regardless of free delivery
        "platformFee": round(config["platformFee"], 2),
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


def _parse_iso_or_date(value: str):
    """Parse date strings in YYYY-MM-DD or ISO datetime format."""
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        # Handles YYYY-MM-DD directly
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        # Handles ISO formats with/without Z
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _is_coupon_active(start_date: str, end_date: str) -> bool:
    """Validate coupon active window if start/end are provided."""
    now = datetime.now(timezone.utc)
    parsed_start = _parse_iso_or_date(start_date) if start_date else None
    parsed_end = _parse_iso_or_date(end_date) if end_date else None

    if parsed_start and now < parsed_start:
        return False
    if parsed_end and now > parsed_end:
        return False
    return True


def register_delivery_routes(app):
    """Register delivery fee calculation routes"""
    
    @app.post("/api/v1/delivery/calculate-fee")
    @tracer.capture_method
    def calculate_fee():
        """Calculate delivery fee based on distance, items, and total"""
        try:
            body = app.current_event.json_body or {}
            distance_km = body.get('distanceKm')
            item_total = body.get('itemTotal')
            item_count = body.get('itemCount')  # Accepted for backward compatibility
            coupon_code = body.get('couponCode')

            if distance_km is None or item_total is None:
                return {
                    "error": "distanceKm and itemTotal are required"
                }, 400

            distance_km = float(distance_km)
            item_total = float(item_total)
            logger.info(
                "Delivery fee request received: "
                f"distanceKm={distance_km}, itemTotal={item_total}, "
                f"itemCount={item_count}, couponCode={coupon_code}"
            )

            config, missing_keys = _fetch_global_delivery_config()
            if not config:
                logger.warning(
                    "Global delivery config missing or invalid for calculate-fee: "
                    f"missingKeys={missing_keys}"
                )
                metrics.add_metric(name="DeliveryFeeConfigMissing", unit="Count", value=1)
                return _build_safe_zero_response(distance_km, missing_keys), 200

            logger.info(
                "Global delivery config loaded: "
                f"platformFee={config['platformFee']}, riderBaseFare={config['riderBaseFare']}, "
                f"riderFarePerKm={config['riderFarePerKm']}, "
                f"riderFreeDeliveryBelowKm={config['riderFreeDeliveryBelowKm']}, "
                f"riderSurgePricePerKm={config['riderSurgePricePerKm']}, "
                f"riderSurgeChargeAfterKms={config['riderSurgeChargeAfterKms']}, "
                f"freeDeliveryAboveThreshold={config['freeDeliveryAboveThreshold']}"
            )

            result = calculate_delivery_fee(
                distance_km=distance_km,
                item_total=item_total,
                config=config
            )

            # Coupon is informational only: discount is reported but deliveryFee is unchanged.
            coupon_applied = False
            if coupon_code:
                try:
                    pk = f"COUPON#{coupon_code}"
                    response = dynamodb_client.query(
                        TableName=TABLES['CONFIG'],
                        KeyConditionExpression='partitionkey = :pk',
                        ExpressionAttributeValues={':pk': {'S': pk}},
                        Limit=1
                    )
                    item = response.get('Items', [None])[0] if response.get('Items') else None
                    if item:
                        coupon_type = item.get('couponType', {}).get('S') or item.get('type', {}).get('S')
                        coupon_value = item.get('couponValue', {}).get('N') or item.get('value', {}).get('N')
                        start_date = item.get('startDate', {}).get('S')
                        end_date = item.get('endDate', {}).get('S')

                        if coupon_type and coupon_value and _is_coupon_active(start_date, end_date):
                            coupon_value = float(coupon_value)
                            delivery_fee = result['deliveryFee']
                            coupon_discount = 0.0

                            if coupon_type.lower() == 'percentage':
                                coupon_discount = (delivery_fee * coupon_value) / 100.0
                            elif coupon_type.lower() == 'fixed':
                                coupon_discount = coupon_value


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
                                "Coupon not applied for calculate-fee: "
                                f"couponCode={coupon_code}, reason=inactive_or_invalid"
                            )
                    else:
                        logger.info(
                            "Coupon not found for calculate-fee: "
                            f"couponCode={coupon_code}"
                        )
                except Exception as e:
                    logger.error(f"Error applying coupon {coupon_code}: {str(e)}")

            result['couponApplied'] = coupon_applied
            if coupon_applied and coupon_code:
                result['couponCode'] = coupon_code

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
                "error": "distanceKm and itemTotal must be numeric values"
            }, 400
        except Exception as e:
            logger.error("Error calculating delivery fee", exc_info=True)
            metrics.add_metric(name="DeliveryFeeCalculationFailed", unit="Count", value=1)
            return {"error": "Failed to calculate delivery fee", "message": str(e)}, 500
