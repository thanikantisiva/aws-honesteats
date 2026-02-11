"""DynamoDB utility functions"""
import os
import boto3
from botocore.exceptions import ClientError


# Initialize DynamoDB client
dynamodb_client = boto3.client('dynamodb')

# Table names from environment variables
TABLES = {
    'USERS': os.environ.get('USERS_TABLE_NAME', 'food-delivery-users'),
    'RESTAURANTS': os.environ.get('RESTAURANTS_TABLE_NAME', 'food-delivery-restaurants'),
    'MENU_ITEMS': os.environ.get('MENU_ITEMS_TABLE_NAME', 'food-delivery-menu-items'),
    'ORDERS': os.environ.get('ORDERS_TABLE_NAME', 'food-delivery-orders'),
    'RIDERS': os.environ.get('RIDERS_TABLE_NAME', 'food-delivery-riders'),
    'ADDRESSES': os.environ.get('ADDRESSES_TABLE_NAME', 'food-delivery-addresses'),
    'PAYMENTS': os.environ.get('PAYMENTS_TABLE_NAME', 'food-delivery-payments'),
    'EARNINGS': os.environ.get('EARNINGS_TABLE_NAME', 'food-delivery-rider-earnings'),
    'RESTAURANT_EARNINGS': os.environ.get('RESTAURANT_EARNINGS_TABLE_NAME', 'food-delivery-restaurant-earnings'),
}


def generate_id(prefix: str) -> str:
    """Generate a unique ID with a prefix
    
    Args:
        prefix: Static prefix for the ID (e.g., 'RES' for restaurants, 'ADD' for addresses)
    
    Returns:
        A unique ID in the format: {PREFIX}-{timestamp}-{random}
    """
    import time
    import random
    return f"{prefix}-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
