"""Authentication routes - Simplified for Firebase SDK"""
import os
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.user_service import UserService
from middleware.api_key_auth import APIKeyAuth
from services.msg91_service import MSG91Service

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
        """Send OTP code via MSG91"""
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

            result = MSG91Service.send_otp(phone)

            if not result.get('success'):
                metrics.add_metric(name="OTPSendFailed", unit="Count", value=1)
                return {"error": result.get('error', 'Failed to send OTP')}, 400

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
            
            if len(code) != 6 or not code.isdigit():
                return {"error": "Invalid OTP format. Must be 6 digits"}, 400
            
            logger.info(f"🔐 OTP verification for phone: {phone[:5]}***")
            
            # Verify OTP from DynamoDB (MSG91 OTP)
            result = MSG91Service.verify_otp(phone, code, OTP_TABLE_NAME)
            
            if not result['success']:
                metrics.add_metric(name="OTPVerifyFailed", unit="Count", value=1)
                return {"error": result.get('error', 'Invalid OTP')}, 400
            
            # Check if CUSTOMER user exists
            user = UserService.get_user_by_role(phone, "CUSTOMER")
            is_new_user = user is None
            
            metrics.add_metric(name="OTPVerified", unit="Count", value=1)
            if is_new_user:
                metrics.add_metric(name="NewUserOTPLogin", unit="Count", value=1)
            
            # Generate auth token
            import time
            auth_token = f"token_{phone}_{int(time.time())}"
            
            return {
                "success": True,
                "idToken": auth_token,
                "userId": phone,
                "isNewUser": is_new_user
            }, 200
            
        except Exception as e:
            logger.error("Error in verify_otp", exc_info=True)
            metrics.add_metric(name="OTPVerifyError", unit="Count", value=1)
            return {"error": "Failed to verify OTP", "message": str(e)}, 500
