"""DynamoDB utility functions"""
import os
import boto3
from botocore.exceptions import ClientError


# Initialize DynamoDB client
dynamodb_client = boto3.client('dynamodb')

# Match template.yaml: TableName = !Sub 'food-delivery-...-${Environment}'
# If *TABLE_NAME is unset, default always includes ENVIRONMENT suffix (never bare names).
_ENV = os.environ.get('ENVIRONMENT', 'dev')


def _default_table(base: str) -> str:
    return f'{base}-{_ENV}'


# Table names: explicit env wins; otherwise suffixed default (same as SAM table names)
TABLES = {
    'USERS': os.environ.get('USERS_TABLE_NAME') or _default_table('food-delivery-users'),
    'RESTAURANTS': os.environ.get('RESTAURANTS_TABLE_NAME') or _default_table('food-delivery-restaurants'),
    'MENU_ITEMS': os.environ.get('MENU_ITEMS_TABLE_NAME') or _default_table('food-delivery-menu-items'),
    'ORDERS': os.environ.get('ORDERS_TABLE_NAME') or _default_table('food-delivery-orders'),
    'RIDERS': os.environ.get('RIDERS_TABLE_NAME') or _default_table('food-delivery-riders'),
    'ADDRESSES': os.environ.get('ADDRESSES_TABLE_NAME') or _default_table('food-delivery-addresses'),
    'PAYMENTS': os.environ.get('PAYMENTS_TABLE_NAME') or _default_table('food-delivery-payments'),
    'EARNINGS': os.environ.get('EARNINGS_TABLE_NAME') or _default_table('food-delivery-rider-earnings'),
    'RESTAURANT_EARNINGS': os.environ.get('RESTAURANT_EARNINGS_TABLE_NAME')
    or _default_table('food-delivery-restaurant-earnings'),
    'RESTAURANT_LOGIN': os.environ.get('RESTAURANT_LOGIN_TABLE_NAME') or _default_table('food-delivery-login'),
    'CONFIG': os.environ.get('CONFIG_TABLE_NAME') or _default_table('food-delivery-config'),
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
