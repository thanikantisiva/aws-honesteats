"""Authentication routes - Simplified for Firebase SDK"""
import os
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.user_service import UserService
from middleware.api_key_auth import APIKeyAuth
from middleware.jwt_auth import generate_token
from services.messagecentral_service import MessageCentralService
from services.rate_limiter_service import RateLimiterService
from utils.dynamodb import dynamodb_client

logger = Logger()
tracer = Tracer()
metrics = Metrics()

OTP_TABLE_NAME = os.environ.get('OTP_TABLE_NAME', '')


def register_auth_routes(app):
    """Register authentication routes"""
    
    # Note: OTP send/verify now handled by Firebase SDK directly in mobile app
    # These endpoints are kept for backward compatibility or future use

    @app.post("/api/v1/auth/send-otp")
    @tracer.capture_method
    def send_otp():
        """Send OTP code via Message Central"""
        try:
            # Validate API key
            is_valid, error_response = APIKeyAuth.require_api_key(app.current_event.raw_event)
            if not is_valid:
                return error_response['body'], error_response['statusCode']

            body = app.current_event.json_body or {}
            phone = body.get('phone')

            if not phone:
                return {"error": "Phone number is required"}, 400

            logger.info(f"📨 Sending OTP for phone: {str(phone)[:5]}***")

            allowed, _attempts_remaining = RateLimiterService.check_rate_limit(phone)
            if not allowed:
                return {"success": False, "message": "Too many attempts. Please try again later."}, 200

            result = MessageCentralService.send_otp(phone)

            if not result.get('success'):
                metrics.add_metric(name="OTPSendFailed", unit="Count", value=1)
                return {"error": result.get('error', 'Failed to send OTP')}, 400

            verification_id = result.get('verificationId')
            timeout_seconds = int(float(result.get('timeout', 60)))
            if verification_id and OTP_TABLE_NAME:
                import time
                now = int(time.time())
                dynamodb_client.put_item(
                    TableName=OTP_TABLE_NAME,
                    Item={
                        'phone': {'S': phone},
                        'verificationId': {'S': str(verification_id)},
                        'expiresAt': {'N': str(now + timeout_seconds)},
                        'ttl': {'N': str(now + timeout_seconds)}
                    }
                )

            RateLimiterService.increment_attempt(phone)

            metrics.add_metric(name="OTPSent", unit="Count", value=1)
            return {"success": True, "message": "OTP sent successfully"}, 200

        except Exception as e:
            logger.error("Error in send_otp", exc_info=True)
            metrics.add_metric(name="OTPSendError", unit="Count", value=1)
            return {"error": "Failed to send OTP", "message": str(e)}, 500
    
    @app.post("/api/v1/auth/verify-otp")
    @tracer.capture_method
    def verify_otp():
        """Verify OTP code and return Firebase ID token"""
        try:
            # Validate API key
            is_valid, error_response = APIKeyAuth.require_api_key(app.current_event.raw_event)
            if not is_valid:
                return error_response['body'], error_response['statusCode']
            
            body = app.current_event.json_body
            phone = body.get('phone')
            code = body.get('code')
            
            if not phone or not code:
                return {"error": "Phone number and code are required"}, 400
            
            if not code.isdigit():
                return {"error": "Invalid OTP format. Must be numeric"}, 400
            
            logger.info(f"🔐 OTP verification for phone: {phone[:5]}***")
            
            # Verify OTP via Message Central using stored verificationId
            verification_id = None
            if OTP_TABLE_NAME:
                otp_item = dynamodb_client.get_item(
                    TableName=OTP_TABLE_NAME,
                    Key={'phone': {'S': phone}}
                )
                if 'Item' in otp_item:
                    verification_id = otp_item['Item'].get('verificationId', {}).get('S')

            if not verification_id:
                return {"error": "OTP not found or expired"}, 400

            result = MessageCentralService.verify_otp(verification_id, code)
            
            if not result['success']:
                metrics.add_metric(name="OTPVerifyFailed", unit="Count", value=1)
                return {"error": result.get('error', 'Invalid OTP')}, 400
            
            # Check if user exists (try CUSTOMER role first)
            user = UserService.get_user_by_role(phone, "CUSTOMER")
            is_new_user = user is None
            
            metrics.add_metric(name="OTPVerified", unit="Count", value=1)
            if is_new_user:
                metrics.add_metric(name="NewUserOTPLogin", unit="Count", value=1)
            
            # Generate JWT token
            jwt_token = generate_token(phone, {'isNewUser': is_new_user})
            logger.info(f"✅ JWT token generated for: {phone[:5]}***")
            
            return {
                "success": True,
                "token": jwt_token,
                "userId": phone,
                "isNewUser": is_new_user
            }, 200
            
        except Exception as e:
            logger.error("Error in verify_otp", exc_info=True)
            metrics.add_metric(name="OTPVerifyError", unit="Count", value=1)
            return {"error": "Failed to verify OTP", "message": str(e)}, 500
