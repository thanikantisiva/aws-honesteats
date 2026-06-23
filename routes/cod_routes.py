"""Cash-on-delivery (COD) eligibility decision route.

Stateless rule engine the customer app calls at checkout (before an order
exists) to decide whether to offer COD. Decision precedence:

  1. customer.forceCod      -> allow  (wins over everything)
  2. customer.disableCod    -> deny
  3. global codConfig.disableCod -> deny
  4. codConfig.minAmount <= bill <= codConfig.maxAmount -> allow
  5. otherwise (out of range / config missing) -> deny (safe default)

Per-customer flags live on the USERS row and are set via the admin-only
endpoint POST /api/v1/ops/users/<phone>/cod-toggles. The global codConfig
lives in the dedicated COD config row (CONFIG#COD) and is set via
POST /api/v1/cod-config (see services/cod_config_service.py).
"""
from datetime import datetime
from typing import Optional

from aws_lambda_powertools import Logger, Tracer, Metrics
from services.cod_config_service import fetch_cod_config, parse_hhmm
from services.user_service import UserService
from utils import normalize_phone
from utils.datetime_ist import IST

logger = Logger()
tracer = Tracer()
metrics = Metrics()

# Kept as constants so the wording is trivial to change in one place.
COD_AVAILABLE_MSG = "Cash on delivery available."
COD_UNAVAILABLE_MSG = "Cash on delivery is not available at the moment."
COD_AMOUNT_MSG = "Cash on delivery is not available for this order amount."
COD_HOURS_MSG = "Cash on delivery is not available at this time."


def _to_float(value) -> Optional[float]:
    """Safely parse a numeric value from a request/config payload."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value) -> bool:
    """Coerce JSON / admin-tool values to a real bool (handles "false" strings)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "on"}
    return bool(value)


def _within_cod_hours(cod_config: dict, now_minutes: Optional[int]) -> bool:
    """True if COD is allowed at ``now_minutes`` per the optional availability window.

    ``availableFrom``/``availableTo`` are 24h "HH:MM" strings. No (or incomplete)
    window means no time restriction. Overnight windows are supported by letting
    ``availableFrom`` be greater than ``availableTo`` (e.g. 18:00-02:00); to disable
    COD overnight you set the daytime window, e.g. 06:00-21:00.
    """
    if now_minutes is None:
        return True
    start = parse_hhmm(cod_config.get("availableFrom"))
    end = parse_hhmm(cod_config.get("availableTo"))
    if start is None or end is None or start == end:
        return True  # no/incomplete window -> available all day
    if start < end:
        return start <= now_minutes < end
    # Window wraps past midnight.
    return now_minutes >= start or now_minutes < end


def _decide_cod(
    disable_cod: bool,
    force_cod: bool,
    bill: float,
    cod_config: dict,
    now_minutes: Optional[int] = None,
) -> dict:
    """Pure decision function. Returns {codEnabled, description, reason}."""
    # Rule 2 (takes precedence): explicit per-customer force override.
    if force_cod:
        return {"codEnabled": True, "description": COD_AVAILABLE_MSG, "reason": "FORCE_COD"}

    # Rule 1: per-customer disable.
    if disable_cod:
        return {"codEnabled": False, "description": COD_UNAVAILABLE_MSG, "reason": "CUSTOMER_DISABLED"}

    # Rule 3: global kill-switch.
    if _coerce_bool(cod_config.get("disableCod")):
        return {"codEnabled": False, "description": COD_UNAVAILABLE_MSG, "reason": "GLOBAL_DISABLED"}

    # Rule 3b: outside the configured COD availability window (e.g. disabled overnight).
    if not _within_cod_hours(cod_config, now_minutes):
        return {"codEnabled": False, "description": COD_HOURS_MSG, "reason": "OUTSIDE_COD_HOURS"}

    # Rule 4: bill within the configured COD window.
    min_amount = _to_float(cod_config.get("minAmount"))
    max_amount = _to_float(cod_config.get("maxAmount"))
    if min_amount is None or max_amount is None:
        # Can't evaluate rule 4 without limits — deny by default.
        return {"codEnabled": False, "description": COD_UNAVAILABLE_MSG, "reason": "CONFIG_MISSING"}
    if min_amount <= bill <= max_amount:
        return {"codEnabled": True, "description": COD_AVAILABLE_MSG, "reason": "WITHIN_LIMITS"}

    # Rule 5: out of range.
    return {"codEnabled": False, "description": COD_AMOUNT_MSG, "reason": "AMOUNT_OUT_OF_RANGE"}


def register_cod_routes(app):
    """Register COD eligibility routes."""

    @app.post("/api/v1/cod/eligibility")
    @tracer.capture_method
    def check_cod_eligibility():
        """Decide whether COD can be offered for a checkout.

        Body:
          - billAmount:    required, the order bill the customer would pay
          - customerPhone: optional; when present the per-customer
                           disableCod / forceCod flags are applied. Absent
                           (guest) -> both flags treated as false.

        Always returns 200 with { codEnabled, description, reason } — the
        decision is not an error condition.
        """
        try:
            body = app.current_event.json_body or {}

            bill = _to_float(body.get("billAmount"))
            if bill is None:
                return {"error": "billAmount is required and must be numeric"}, 400

            disable_cod = False
            force_cod = False
            raw_phone = body.get("customerPhone")
            if raw_phone:
                phone = normalize_phone(raw_phone)
                if phone:
                    customer = UserService.get_user_by_role(phone, "CUSTOMER")
                    if customer:
                        disable_cod = bool(customer.disable_cod)
                        force_cod = bool(customer.force_cod)

            cod_config = fetch_cod_config()
            now_ist = datetime.now(IST)
            now_minutes = now_ist.hour * 60 + now_ist.minute
            decision = _decide_cod(disable_cod, force_cod, bill, cod_config, now_minutes)

            logger.info(
                f"COD eligibility: bill={bill} disableCod={disable_cod} "
                f"forceCod={force_cod} timeIST={now_ist:%H:%M} -> {decision['reason']}"
            )
            metrics.add_metric(name="CodEligibilityChecked", unit="Count", value=1)
            return decision, 200
        except Exception as e:
            logger.error("Error checking COD eligibility", exc_info=True)
            return {"error": "Failed to check COD eligibility", "message": str(e)}, 500
