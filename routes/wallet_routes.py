"""Customer wallet (YumCoins) read routes."""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.wallet_service import WalletService
from utils import normalize_phone

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_wallet_routes(app):
    """Register customer wallet read routes."""

    @app.get("/api/v1/users/<phone>/wallet")
    @tracer.capture_method
    def get_wallet(phone: str):
        """Return the customer's YumCoins balance and its real-₹ value."""
        try:
            phone = normalize_phone(phone)
            if not phone:
                return {"error": "Phone number is required"}, 400

            balance = WalletService.get_balance(phone)
            metrics.add_metric(name="WalletBalanceFetched", unit="Count", value=1)
            return {"phone": phone, **balance}, 200
        except Exception as e:
            logger.error("Error fetching wallet balance", exc_info=True)
            return {"error": "Failed to fetch wallet", "message": str(e)}, 500

    @app.get("/api/v1/users/<phone>/wallet/transactions")
    @tracer.capture_method
    def get_wallet_transactions(phone: str):
        """Return the customer's YumCoins transaction history (newest first)."""
        try:
            phone = normalize_phone(phone)
            if not phone:
                return {"error": "Phone number is required"}, 400

            query_params = app.current_event.query_string_parameters or {}
            try:
                limit = int(query_params.get("limit", 50))
            except (TypeError, ValueError):
                limit = 50

            result = WalletService.get_transactions(phone, limit=limit)
            metrics.add_metric(name="WalletTransactionsFetched", unit="Count", value=1)
            return {"phone": phone, **result}, 200
        except Exception as e:
            logger.error("Error fetching wallet transactions", exc_info=True)
            return {"error": "Failed to fetch wallet transactions", "message": str(e)}, 500
