"""Delivery fee calculation routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from utils.dynamodb import dynamodb_client, TABLES

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def calculate_delivery_fee(distance_km: float, item_count: int, item_total: float) -> dict:
    """
    Calculate delivery fee based on distance, items, and order value
    
    Pricing Logic:
    - Base fee: ₹12 per km, minimum ₹60
    - Free delivery if order > ₹500
    - Handling fee: ₹10 for every 5 items
    - Surge pricing for long distance (>10km): +20%
    
    Args:
        distance_km: Distance from restaurant to delivery address
        item_count: Number of items in cart
        item_total: Total value of items
    
    Returns:
        dict with deliveryFee, breakdown, and discount info
    """
    # Base delivery fee: ₹12/km, minimum ₹60
    base_fee = max(30, int(distance_km * 10))
    
    # Handling fee: ₹10 for every 5 items (bulk order handling)
    handling_fee = (item_count // 5) * 10
    
    # Surge pricing for long distance (>10km): +20%
    surge_fee = 0
    if distance_km > 10:
        surge_fee = int(base_fee * 0.2)
        logger.info(f"⚡ Surge pricing applied: +₹{surge_fee} (distance > 10km)")
    
    # Calculate total before discounts
    total_before_discount = base_fee + handling_fee + surge_fee
    
    # Free delivery discount if order value >= ₹500
    discount = 0
    final_fee = total_before_discount
    
    if item_total >= 500:
        discount = total_before_discount
        logger.info(f"🎉 FREE DELIVERY applied (order ₹{item_total} >= ₹500)")
    
    return {
        'deliveryFee': final_fee,
        'breakdown': {
            'baseFee': base_fee,
            'handlingFee': handling_fee,
            'surgeFee': surge_fee,
            'discount': discount
        },
        'freeDeliveryThreshold': 500,
        'isFreeDelivery': final_fee == 0,
        'distance': distance_km
    }


def register_delivery_routes(app):
    """Register delivery fee calculation routes"""
    
    @app.post("/api/v1/delivery/calculate-fee")
    @tracer.capture_method
    def calculate_fee():
        """Calculate delivery fee based on distance, items, and total"""
        try:
            body = app.current_event.json_body
            distance_km = body.get('distanceKm')
            item_count = body.get('itemCount')
            item_total = body.get('itemTotal')
            coupon_code = body.get('couponCode')
            
            if distance_km is None or item_count is None or item_total is None:
                return {
                    "error": "distanceKm, itemCount, and itemTotal are required"
                }, 400
            
            logger.info(f"📦 Delivery fee request: {distance_km}km, {item_count} items, ₹{item_total}")
            
            result = calculate_delivery_fee(
                float(distance_km),
                int(item_count),
                float(item_total)
            )

            # Apply coupon discount on delivery fee if provided
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

                        # Validate coupon date range if present (YYYY-MM-DD)
                        if start_date or end_date:
                            from datetime import datetime
                            today_str = datetime.utcnow().strftime('%Y-%m-%d')
                            if start_date and today_str < start_date:
                                coupon_type = None
                            if end_date and today_str > end_date:
                                coupon_type = None

                        if coupon_type and coupon_value:
                            coupon_value = float(coupon_value)
                            delivery_fee = result['deliveryFee']
                            coupon_discount = 0.0

                            if coupon_type.lower() == 'percentage':
                                coupon_discount = (delivery_fee * coupon_value) / 100.0
                            elif coupon_type.lower() == 'fixed':
                                coupon_discount = coupon_value

                            coupon_discount = max(0.0, min(coupon_discount, delivery_fee))
                            result['deliveryFee'] = round(delivery_fee - coupon_discount, 2)
                            result['breakdown']['couponDiscount'] = round(coupon_discount, 2)
                            result['breakdown']['discount'] = round(
                                result['breakdown'].get('discount', 0) + coupon_discount, 2
                            )
                            coupon_applied = coupon_discount > 0
                except Exception as e:
                    logger.error(f"Error applying coupon {coupon_code}: {str(e)}")

            result['couponApplied'] = coupon_applied
            
            logger.info(f"💰 Calculated delivery fee: ₹{result['deliveryFee']}")
            
            metrics.add_metric(name="DeliveryFeeCalculated", unit="Count", value=1)
            if result['isFreeDelivery']:
                metrics.add_metric(name="FreeDeliveryApplied", unit="Count", value=1)
            
            return result, 200
            
        except Exception as e:
            logger.error("Error calculating delivery fee", exc_info=True)
            metrics.add_metric(name="DeliveryFeeCalculationFailed", unit="Count", value=1)
            return {"error": "Failed to calculate delivery fee", "message": str(e)}, 500
