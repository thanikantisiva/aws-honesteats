"""Delivery fee calculation routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics

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
    base_fee = max(60, int(distance_km * 12))
    
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
        final_fee = 0
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
            
            logger.info(f"💰 Calculated delivery fee: ₹{result['deliveryFee']}")
            
            metrics.add_metric(name="DeliveryFeeCalculated", unit="Count", value=1)
            if result['isFreeDelivery']:
                metrics.add_metric(name="FreeDeliveryApplied", unit="Count", value=1)
            
            return result, 200
            
        except Exception as e:
            logger.error("Error calculating delivery fee", exc_info=True)
            metrics.add_metric(name="DeliveryFeeCalculationFailed", unit="Count", value=1)
            return {"error": "Failed to calculate delivery fee", "message": str(e)}, 500
