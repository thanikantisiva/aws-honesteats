"""Pricing configuration and markup calculations"""

# Category-based markup percentages for platform commission
CATEGORY_MARKUP = {
    'Starters': 0.15,          # 15% markup
    'Main Course': 0.20,       # 20% markup
    'Breads': 0.10,            # 10% markup
    'Rice & Biryani': 0.25,    # 25% markup
    'Desserts': 0.30,          # 30% markup
    'Beverages': 0.40,         # 40% markup
    'Salads': 0.20,            # 20% markup
    'Soups': 0.25,             # 25% markup
    'default': 0.20            # Default 20% markup if category not specified
}


def calculate_customer_price(restaurant_price: float, category: str = None) -> float:
    """
    Calculate customer-facing price from restaurant price with category-based markup
    Rounds UP to nearest multiple of 5 for clean pricing
    
    Args:
        restaurant_price: Price the restaurant charges (base price)
        category: Menu item category (optional)
    
    Returns:
        Customer-facing price with markup applied, rounded up to nearest multiple of 5
    
    Example:
        restaurant_price=200, category="Rice & Biryani" (25% markup)
        → calculated=250 → rounded=250
        
        restaurant_price=197, category="Rice & Biryani" (25% markup)
        → calculated=246.25 → rounded=250 (ceil to nearest 5)
    """
    if restaurant_price <= 0:
        return restaurant_price
    
    # Get markup percentage for category (default if not found)
    markup_percentage = CATEGORY_MARKUP.get(category, CATEGORY_MARKUP['default'])
    
    # Calculate customer price with markup
    customer_price = restaurant_price * (1 + markup_percentage)
    
    # Round UP to nearest multiple of 5
    import math
    rounded_price = math.ceil(customer_price / 5) * 5
    
    return float(rounded_price)


def get_platform_commission(customer_price: float, restaurant_price: float) -> float:
    """
    Calculate platform commission (difference between customer price and restaurant price)
    
    Args:
        customer_price: Price customer pays
        restaurant_price: Price restaurant receives
    
    Returns:
        Platform commission amount
    """
    return round(customer_price - restaurant_price, 2)


def get_markup_percentage(category: str = None) -> float:
    """
    Get markup percentage for a category
    
    Args:
        category: Menu item category
    
    Returns:
        Markup percentage as decimal (e.g., 0.20 for 20%)
    """
    return CATEGORY_MARKUP.get(category, CATEGORY_MARKUP['default'])
