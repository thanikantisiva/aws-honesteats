"""API Key Authentication Middleware"""
from typing import Optional
from aws_lambda_powertools import Logger
from utils.ssm import get_secret

logger = Logger(child=True)

def _api_keys() -> dict:
    return {
        'mobile': get_secret('MOBILE_API_KEY', 'dev-mobile-key-12345'),
        'web': get_secret('WEB_API_KEY', 'dev-web-key-12345'),
        'admin': get_secret('ADMIN_API_KEY', 'dev-admin-key-12345')
    }


class APIKeyAuth:
    """API Key Authentication"""
    
    @staticmethod
    def validate_api_key(api_key: Optional[str]) -> bool:
        """
        Validate API key from request header
        
        Args:
            api_key: API key from X-Api-Key header
        
        Returns:
            bool: True if valid, False otherwise
        """
        if not api_key:
            logger.warning("Missing API key")
            return False
        
        # Check if key matches any of the configured keys
        for key_type, valid_key in _api_keys().items():
            if api_key == valid_key:
                logger.info(f"Valid API key: {key_type}")
                return True
        
        logger.warning(f"Invalid API key: {api_key[:10]}...")
        return False
    
    @staticmethod
    def get_api_key_type(api_key: str) -> Optional[str]:
        """
        Get the type of API key (mobile, web, admin)
        
        Args:
            api_key: API key
        
        Returns:
            str: Key type or None
        """
        for key_type, valid_key in _api_keys().items():
            if api_key == valid_key:
                return key_type
        return None
    
    @staticmethod
    def require_api_key(event: dict) -> tuple[bool, Optional[dict]]:
        """
        Middleware function to require API key
        
        Args:
            event: Lambda event
        
        Returns:
            tuple: (is_valid, error_response)
        """
        headers = event.get('headers', {})
        
        # Check both X-Api-Key and x-api-key (case insensitive)
        api_key = (
            headers.get('X-Api-Key') or 
            headers.get('x-api-key') or
            headers.get('X-API-KEY')
        )
        
        if not APIKeyAuth.validate_api_key(api_key):
            return False, {
                'statusCode': 401,
                'body': {
                    'error': 'Unauthorized',
                    'message': 'Invalid or missing API key'
                }
            }
        
        return True, None
