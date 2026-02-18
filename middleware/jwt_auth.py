"""JWT Authentication Middleware"""
import jwt
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Callable
from functools import wraps
from aws_lambda_powertools import Logger
from utils.ssm import get_secret

logger = Logger(child=True)

# JWT Configuration
JWT_SECRET_KEY = get_secret('JWT_SECRET_KEY', 'dev-secret-key-change-in-production')
JWT_ALGORITHM = 'HS256'
JWT_EXPIRY_DAYS = 90  # Long-lived tokens for better UX


def generate_token(phone: str, user_data: Optional[Dict[str, Any]] = None) -> str:
    """
    Generate JWT token for authenticated user
    
    Args:
        phone: User's phone number
        user_data: Optional additional user data to include in token
    
    Returns:
        str: JWT token
    """
    now = datetime.utcnow()
    
    payload = {
        'phone': phone,
        'iat': int(now.timestamp()),
        'exp': int((now + timedelta(days=JWT_EXPIRY_DAYS)).timestamp())
    }
    
    # Add optional user data to payload
    if user_data:
        payload.update(user_data)
    
    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    logger.info(f"Generated JWT token for phone: {phone[:5]}***")
    
    return token


def verify_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Verify JWT token and extract payload
    
    Args:
        token: JWT token string
    
    Returns:
        dict: Token payload if valid, None if invalid/expired
    """
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        logger.info(f"Token verified for phone: {payload.get('phone', '')[:5]}***")
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("Token expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {str(e)}")
        return None


def require_auth(app):
    """
    Decorator factory to require JWT authentication on route handlers
    
    Usage:
        @app.get("/api/v1/protected-route")
        @require_auth(app)
        def protected_route():
            # Access user data from app.current_event.request_context
            user_phone = get_current_user_phone(app)
            return {"message": f"Hello {user_phone}"}
    
    Args:
        app: The APIGatewayRestResolver instance
    
    Returns:
        Decorator function that wraps route handlers
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get Authorization header from current event
            headers = app.current_event.headers or {}
            auth_header = headers.get('authorization') or headers.get('Authorization')
            
            if not auth_header:
                logger.warning("Missing Authorization header")
                return {
                    'error': 'Unauthorized',
                    'message': 'Missing authentication token'
                }, 401
            
            # Check Bearer token format
            if not auth_header.startswith('Bearer '):
                logger.warning("Invalid Authorization header format")
                return {
                    'error': 'Unauthorized',
                    'message': 'Invalid token format'
                }, 401
            
            # Extract token
            token = auth_header.replace('Bearer ', '').strip()
            
            # Verify token
            payload = verify_token(token)
            
            if not payload:
                logger.warning("Token verification failed")
                return {
                    'error': 'Unauthorized',
                    'message': 'Invalid or expired token'
                }, 401
            
            # (Removed) No longer attaching JWT payload to request context
            
            logger.info(f"Authenticated request from: {payload.get('phone', '')[:5]}***")
            
            # Call the actual route handler
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator


def get_current_user_phone(app) -> Optional[str]:
    """
    Helper function to get current authenticated user's phone from request context
    
    Args:
        app: The APIGatewayRestResolver instance
    
    Returns:
        str: User's phone number if authenticated, None otherwise
    """
    if hasattr(app.current_event, 'request_context'):
        return app.current_event.request_context.get('phone')
    return None


def check_ownership(app, resource_phone: str) -> bool:
    """
    Helper function to check if current user owns the resource
    
    Args:
        app: The APIGatewayRestResolver instance
        resource_phone: Phone number associated with the resource
    
    Returns:
        bool: True if current user owns the resource, False otherwise
    """
    current_phone = get_current_user_phone(app)
    if not current_phone:
        return False
    
    return current_phone == resource_phone
