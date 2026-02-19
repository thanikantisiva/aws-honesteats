"""Message Central OTP service integration"""
import os
import time
import requests
from aws_lambda_powertools import Logger
from utils.ssm import get_secret

logger = Logger()

_token_cache = {
    "token": None,
    "expires_at": 0
}

# Test/Mock phone numbers that use fixed OTP: 1234
TEST_PHONE_NUMBERS = {
    '1999999999', '2999999999', '3999999999', '4999999999',
    '5999999999', '6999999999', '7999999999', '8999999999'
}

TEST_OTP = '1234'


class MessageCentralService:
    """Service for sending and verifying OTP via Message Central"""
    
    @staticmethod
    def _is_test_phone(phone: str) -> bool:
        """Check if phone is a test number"""
        digits = phone.replace('+', '').replace('-', '').replace(' ', '')
        last10 = digits[-10:] if len(digits) >= 10 else digits
        return last10 in TEST_PHONE_NUMBERS

    @staticmethod
    def _config():
        return {
            "customer_id": get_secret("MESSAGE_CENTRAL_CUSTOMER_ID", ""),
            "key": get_secret("MESSAGE_CENTRAL_KEY", ""),
            "email": get_secret("MESSAGE_CENTRAL_EMAIL", ""),
            "country": get_secret("MESSAGE_CENTRAL_COUNTRY_CODE", "91")
        }

    @staticmethod
    def _get_token() -> str:
        cfg = MessageCentralService._config()
        if not cfg["customer_id"] or not cfg["key"] or not cfg["email"]:
            raise Exception("Message Central credentials not configured")

        now = int(time.time())
        if _token_cache["token"] and _token_cache["expires_at"] > now:
            return _token_cache["token"]

        url = (
            "https://cpaas.messagecentral.com/auth/v1/authentication/token"
            f"?customerId={cfg['customer_id']}&key={cfg['key']}&scope=NEW&country={cfg['country']}&email={cfg['email']}"
        )
        response = requests.get(url, headers={"accept": "*/*"}, timeout=10)
        data = response.json() if response.content else {}

        if response.status_code != 200 or data.get("status") != 200:
            logger.error(f"Message Central token error: {data}")
            raise Exception("Failed to generate Message Central token")

        token = data.get("token")
        if not token:
            raise Exception("Message Central token missing")

        # Cache token for 10 minutes by default
        _token_cache["token"] = token
        _token_cache["expires_at"] = now + 600
        return token

    @staticmethod
    def send_otp(phone: str) -> dict:
        """Send OTP to phone via Message Central"""
        # Check if test phone - bypass Message Central
        if MessageCentralService._is_test_phone(phone):
            logger.info(f"🧪 Test phone detected: {phone} - Using test OTP: {TEST_OTP}")
            return {
                "success": True,
                "verificationId": f"test_{phone}_{int(time.time())}",
                "timeout": "300",
                "referenceId": "test_reference"
            }
        
        cfg = MessageCentralService._config()
        token = MessageCentralService._get_token()

        url = "https://cpaas.messagecentral.com/verification/v3/send"
        params = {
            "countryCode": cfg["country"],
            "customerId": cfg["customer_id"],
            "flowType": "SMS",
            "mobileNumber": phone
        }
        response = requests.post(url, params=params, headers={"accept": "*/*", "authToken": token}, timeout=10)
        data = response.json() if response.content else {}

        if response.status_code != 200 or data.get("responseCode") != 200:
            logger.error(f"Message Central send OTP failed: {data}")
            return {"success": False, "error": data.get("message", "OTP send failed")}

        payload = data.get("data", {})
        return {
            "success": True,
            "verificationId": payload.get("verificationId"),
            "timeout": payload.get("timeout", "60"),
            "referenceId": payload.get("referenceId")
        }

    @staticmethod
    def verify_otp(verification_id: str, code: str) -> dict:
        """Verify OTP via Message Central"""
        # Check if test verification ID - bypass Message Central
        if verification_id.startswith("test_"):
            logger.info(f"🧪 Test verification ID detected: {verification_id}")
            if code == TEST_OTP:
                logger.info(f"✅ Test OTP verified: {code}")
                return {"success": True, "message": "Test OTP verified"}
            else:
                logger.warning(f"❌ Invalid test OTP: {code} (expected: {TEST_OTP})")
                return {"success": False, "error": "Invalid OTP"}
        
        verify_url = os.environ.get("MESSAGE_CENTRAL_VERIFY_URL", "").strip()
        if not verify_url:
            return {"success": False, "error": "Message Central verify URL not configured"}

        token = MessageCentralService._get_token()
        params = {
            "verificationId": verification_id,
            "code": code
        }
        response = requests.get(
            verify_url,
            params=params,
            headers={"accept": "*/*", "authToken": token},
            timeout=10
        )
        data = response.json() if response.content else {}

        if response.status_code != 200 or data.get("responseCode") not in [200, "200"]:
            logger.error(f"Message Central verify OTP failed: {data}")
            return {"success": False, "error": data.get("message", "Invalid OTP")}

        return {"success": True, "message": data.get("message", "OTP verified")}
