"""MSG91 OTP service integration"""
import os
import requests
from aws_lambda_powertools import Logger

logger = Logger()


class MSG91Service:
    """Service for sending and verifying OTP via MSG91"""

    @staticmethod
    def _auth_key() -> str:
        return os.environ.get('MSG91_AUTH_KEY', '').strip()

    @staticmethod
    def _template_id() -> str:
        return os.environ.get('MSG91_TEMPLATE_ID', '').strip()

    @staticmethod
    def send_otp(phone: str) -> dict:
        """Send OTP to phone via MSG91"""
        auth_key = MSG91Service._auth_key()
        template_id = MSG91Service._template_id()

        if not auth_key:
            return {"success": False, "error": "MSG91 auth key not configured"}
        if not template_id:
            return {"success": False, "error": "MSG91 template ID not configured"}

        try:
            url = "https://control.msg91.com/api/v5/otp"
            params = {
                "authkey": auth_key,
                "template_id": template_id,
                "mobile": phone
            }
            logger.info(f"MSG91 send OTP request: url={url} mobile={phone} template_id={template_id[:6]}***")
            response = requests.post(url, params=params, timeout=10)
            data = response.json() if response.content else {}
            logger.info(f"MSG91 send OTP response: status={response.status_code} body={data}")

            if response.status_code != 200:
                logger.error(f"MSG91 send OTP failed: {data}")
                return {"success": False, "error": data.get("message", "MSG91 send failed")}

            if data.get("type") == "error":
                return {"success": False, "error": data.get("message", "MSG91 send failed")}

            return {"success": True, "message": data.get("message", "OTP sent")}
        except Exception as e:
            logger.error(f"MSG91 send OTP exception: {str(e)}")
            return {"success": False, "error": "MSG91 send exception"}

    @staticmethod
    def verify_otp(phone: str, code: str, _table_name: str = "") -> dict:
        """Verify OTP via MSG91"""
        auth_key = MSG91Service._auth_key()
        if not auth_key:
            return {"success": False, "error": "MSG91 auth key not configured"}

        try:
            url = "https://control.msg91.com/api/v5/otp/verify"
            params = {
                "authkey": auth_key,
                "mobile": phone,
                "otp": code
            }
            logger.info(f"MSG91 verify OTP request: url={url} mobile={phone}")
            response = requests.post(url, params=params, timeout=10)
            data = response.json() if response.content else {}
            logger.info(f"MSG91 verify OTP response: status={response.status_code} body={data}")

            if response.status_code != 200:
                logger.error(f"MSG91 verify OTP failed: {data}")
                return {"success": False, "error": data.get("message", "MSG91 verify failed")}

            if data.get("type") == "error":
                return {"success": False, "error": data.get("message", "Invalid OTP")}

            return {"success": True, "message": data.get("message", "OTP verified")}
        except Exception as e:
            logger.error(f"MSG91 verify OTP exception: {str(e)}")
            return {"success": False, "error": "MSG91 verify exception"}
