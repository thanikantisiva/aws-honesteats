"""SSM parameter helper with caching and SecureString support."""
import os
import boto3
from aws_lambda_powertools import Logger

logger = Logger()
_ssm_client = boto3.client('ssm')
_cache = {}


def _resolve_ssm(value: str) -> str:
    if not value:
        return value
    if not value.startswith("/rork-honesteats/"):
        return value
    if value in _cache:
        return _cache[value]
    try:
        resp = _ssm_client.get_parameter(Name=value, WithDecryption=True)
        resolved = resp.get("Parameter", {}).get("Value", "")
        _cache[value] = resolved
        return resolved
    except Exception as e:
        logger.error(f"Failed to read SSM parameter {value}: {str(e)}")
        return ""


def get_secret(env_key: str, default: str = "") -> str:
    """Read env var; if it looks like an SSM path, resolve it."""
    raw = os.environ.get(env_key, default)
    return _resolve_ssm(raw)
