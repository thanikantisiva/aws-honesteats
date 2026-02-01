"""Rate Limiter Service for OTP requests"""
import os
import time
from typing import Tuple
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from utils.dynamodb import dynamodb_client

logger = Logger(child=True)

RATE_LIMIT_TABLE = os.environ.get('RATE_LIMIT_TABLE', 'rork-honesteats-rate-limits')
OTP_RATE_LIMIT = int(os.environ.get('OTP_RATE_LIMIT', '3'))
OTP_RATE_WINDOW_HOURS = int(os.environ.get('OTP_RATE_WINDOW_HOURS', '1'))
TEST_MODE = os.environ.get('TEST_MODE', 'false').lower() == 'true'


class RateLimiterService:
    """Service for rate limiting OTP requests"""
    
    @staticmethod
    def check_rate_limit(phone: str) -> Tuple[bool, int]:
        """
        Check if phone number has exceeded rate limit
        
        Args:
            phone: Phone number
        
        Returns:
            Tuple[bool, int]: (is_allowed, attempts_remaining)
        """
        try:
            # TEST_MODE: Skip rate limiting
            if TEST_MODE:
                logger.info(f"[TEST_MODE] Skipping rate limit for {phone[:5]}***")
                return True, OTP_RATE_LIMIT
            
            current_time = int(time.time())
            window_start = current_time - (OTP_RATE_WINDOW_HOURS * 3600)
            
            response = dynamodb_client.get_item(
                TableName=RATE_LIMIT_TABLE,
                Key={'phone': {'S': phone}}
            )
            
            if 'Item' not in response:
                # No rate limit record exists
                return True, OTP_RATE_LIMIT
            
            item = response['Item']
            attempts = int(item.get('attempts', {}).get('N', 0))
            item_window_start = int(item.get('windowStart', {}).get('N', 0))
            
            # Check if we're still in the same time window
            if item_window_start < window_start:
                # Old window, reset
                return True, OTP_RATE_LIMIT
            
            attempts_remaining = OTP_RATE_LIMIT - attempts
            
            if attempts >= OTP_RATE_LIMIT:
                logger.warning(f"Rate limit exceeded for {phone[:5]}***")
                return False, 0
            
            return True, max(0, attempts_remaining)
            
        except ClientError as e:
            logger.error(f"Failed to check rate limit: {str(e)}")
            # On error, allow the request (fail open)
            return True, OTP_RATE_LIMIT
        except Exception as e:
            logger.error(f"Unexpected error checking rate limit: {str(e)}", exc_info=True)
            return True, OTP_RATE_LIMIT
    
    @staticmethod
    def increment_attempt(phone: str) -> bool:
        """
        Increment OTP request attempt counter
        
        Args:
            phone: Phone number
        
        Returns:
            bool: True if incremented successfully
        """
        try:
            # TEST_MODE: Skip tracking
            if TEST_MODE:
                return True
            
            current_time = int(time.time())
            window_start = current_time - (OTP_RATE_WINDOW_HOURS * 3600)
            expires_at = current_time + (OTP_RATE_WINDOW_HOURS * 3600)
            
            # Get current attempts
            response = dynamodb_client.get_item(
                TableName=RATE_LIMIT_TABLE,
                Key={'phone': {'S': phone}}
            )
            
            if 'Item' in response:
                item = response['Item']
                item_window_start = int(item.get('windowStart', {}).get('N', 0))
                
                # Check if we need to reset the window
                if item_window_start < window_start:
                    # Reset to new window
                    attempts = 1
                    new_window_start = current_time
                else:
                    # Increment in current window
                    attempts = int(item.get('attempts', {}).get('N', 0)) + 1
                    new_window_start = item_window_start
            else:
                # First attempt
                attempts = 1
                new_window_start = current_time
            
            # Update or create rate limit record
            dynamodb_client.put_item(
                TableName=RATE_LIMIT_TABLE,
                Item={
                    'phone': {'S': phone},
                    'attempts': {'N': str(attempts)},
                    'windowStart': {'N': str(new_window_start)},
                    'expiresAt': {'N': str(expires_at)}
                }
            )
            
            logger.info(f"Rate limit attempt {attempts}/{OTP_RATE_LIMIT} for {phone[:5]}***")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to increment rate limit: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error incrementing attempt: {str(e)}", exc_info=True)
            return False
    
    @staticmethod
    def reset_attempts(phone: str) -> bool:
        """
        Reset rate limit attempts for a phone number
        
        Args:
            phone: Phone number
        
        Returns:
            bool: True if reset successfully
        """
        try:
            dynamodb_client.delete_item(
                TableName=RATE_LIMIT_TABLE,
                Key={'phone': {'S': phone}}
            )
            
            logger.info(f"Rate limit reset for {phone[:5]}***")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to reset rate limit: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error resetting rate limit: {str(e)}", exc_info=True)
            return False

