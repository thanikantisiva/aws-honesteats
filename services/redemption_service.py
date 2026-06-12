"""YumCoins redemption — spend coins to reduce an order's food total.

Caps (from CONFIG#GLOBAL.config.walletConfig): max X coins per order
(``maxCoinsPerOrder``) and max Y coin-redeeming orders per day
(``maxRedemptionsPerDay``). The daily cap is tracked on a USERS row
SK=``DAILY_REDEMPTIONS#{IST date}`` holding a string-set of orderIds — its size
is the redemption count today (mirrors the DAILY_COUPONS pattern, TTL'd). Coin
spends and reversals go through the idempotent ``WalletService`` ledger.

The platform absorbs the discount: the order keeps its gross ``food_total`` (so
the restaurant settles on full food) and ``coin_discount`` is a separate
platform-funded line — see services/revenue_service.py.
"""
import math
from datetime import datetime, timedelta
from typing import Any, Dict

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from models.wallet import Wallet
from services.wallet_service import WalletService, _fetch_redemption_config
from utils.datetime_ist import IST
from utils.dynamodb import dynamodb_client, TABLES

logger = Logger()

_DAILY_SK_PREFIX = "DAILY_REDEMPTIONS#"


def _today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _daily_ttl() -> int:
    # Expire ~2 days out (same window as the daily-coupon rows).
    return int((datetime.now(IST) + timedelta(days=2)).timestamp())


def _daily_key(customer_phone: str) -> dict:
    return {"phone": {"S": customer_phone}, "role": {"S": f"{_DAILY_SK_PREFIX}{_today_ist()}"}}


class RedemptionService:
    """Orchestrates coin-redemption caps + wallet debit/credit + daily slots."""

    @staticmethod
    def quote(customer_phone: str, requested_coins, food_total) -> Dict[str, Any]:
        """Pure, read-only: how many coins can actually be applied right now.

        Returns {coinsApplied, coinDiscount, rate, reason}. Used by both the
        calculate-fee preview and the authoritative checkout commit.
        """
        cfg = _fetch_redemption_config()
        rate = cfg["rate"]
        out: Dict[str, Any] = {"coinsApplied": 0, "coinDiscount": 0.0, "rate": rate, "reason": None}

        try:
            requested = int(float(requested_coins or 0))
        except (TypeError, ValueError):
            requested = 0
        food_total = float(food_total or 0)

        if not cfg["enabled"] or cfg["maxCoinsPerOrder"] <= 0 or cfg["maxRedemptionsPerDay"] <= 0:
            out["reason"] = "DISABLED"
            return out
        if not customer_phone or requested <= 0:
            out["reason"] = "NO_COINS"
            return out
        if food_total < cfg["minOrderValue"]:
            out["reason"] = "BELOW_MIN_ORDER"
            return out
        if cfg["maxRedemptionsPerDay"] - RedemptionService.count_today(customer_phone) <= 0:
            out["reason"] = "DAILY_CAP"
            return out

        balance = int(WalletService.get_balance(customer_phone).get("coinsBalance", 0) or 0)
        max_by_food = int(math.floor(food_total / rate)) if rate > 0 else 0
        coins = min(requested, cfg["maxCoinsPerOrder"], balance, max_by_food)
        if coins <= 0:
            out["reason"] = "INSUFFICIENT" if balance <= 0 else "NO_DISCOUNT"
            return out

        out["coinsApplied"] = coins
        out["coinDiscount"] = round(coins * rate, 2)
        return out

    @staticmethod
    def count_today(customer_phone: str) -> int:
        """Number of coin-redeeming orders for this customer today (IST)."""
        try:
            resp = dynamodb_client.get_item(
                TableName=TABLES["USERS"],
                Key=_daily_key(customer_phone),
                ProjectionExpression="redeemedOrders",
            )
            item = resp.get("Item")
            if not item:
                return 0
            return len(item.get("redeemedOrders", {}).get("SS", []))
        except ClientError as e:
            logger.warning(f"count_today failed for {customer_phone}: {e}")
            return 0

    @staticmethod
    def reserve_slot(customer_phone: str, order_id: str) -> bool:
        """Atomically claim a daily redemption slot for this order.

        Adds the orderId to today's set only if the set has < Y entries (or this
        order is already in it — idempotent). Returns False when the daily cap
        is already full.
        """
        max_per_day = _fetch_redemption_config()["maxRedemptionsPerDay"]
        try:
            dynamodb_client.update_item(
                TableName=TABLES["USERS"],
                Key=_daily_key(customer_phone),
                UpdateExpression="ADD redeemedOrders :oidset SET #ttl = :ttl",
                ConditionExpression=(
                    "attribute_not_exists(redeemedOrders) "
                    "OR size(redeemedOrders) < :y "
                    "OR contains(redeemedOrders, :oid)"
                ),
                ExpressionAttributeNames={"#ttl": "ttl"},
                ExpressionAttributeValues={
                    ":oidset": {"SS": [order_id]},
                    ":oid": {"S": order_id},
                    ":y": {"N": str(max_per_day)},
                    ":ttl": {"N": str(_daily_ttl())},
                },
            )
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return False
            raise

    @staticmethod
    def release_slot(customer_phone: str, order_id: str) -> None:
        """Free a previously-claimed daily slot (idempotent)."""
        try:
            dynamodb_client.update_item(
                TableName=TABLES["USERS"],
                Key=_daily_key(customer_phone),
                UpdateExpression="DELETE redeemedOrders :oidset",
                ExpressionAttributeValues={":oidset": {"SS": [order_id]}},
            )
        except ClientError as e:
            logger.warning(f"release_slot failed for {customer_phone}/{order_id}: {e}")

    @staticmethod
    def commit(customer_phone: str, order_id: str, requested_coins, food_total) -> Dict[str, Any]:
        """Authoritative checkout path: validate caps, reserve the daily slot, and
        debit the coins (idempotent per order). Returns {coinsApplied, coinDiscount}.
        """
        q = RedemptionService.quote(customer_phone, requested_coins, food_total)
        coins = q["coinsApplied"]
        if coins <= 0:
            return {"coinsApplied": 0, "coinDiscount": 0.0, "reason": q.get("reason")}

        if not RedemptionService.reserve_slot(customer_phone, order_id):
            return {"coinsApplied": 0, "coinDiscount": 0.0, "reason": "DAILY_CAP"}

        debit = WalletService.debit(
            customer_phone,
            category=Wallet.CATEGORY_REDEMPTION,
            amount=coins,
            reference_id=order_id,
            description=f"Redeemed {coins} YumCoins on order {order_id}",
        )
        if not debit.get("applied"):
            if debit.get("reason") == "ALREADY_APPLIED":
                # Idempotent retry of the same order — treat as applied.
                return {"coinsApplied": coins, "coinDiscount": q["coinDiscount"]}
            RedemptionService.release_slot(customer_phone, order_id)
            return {"coinsApplied": 0, "coinDiscount": 0.0, "reason": debit.get("reason")}

        return {"coinsApplied": coins, "coinDiscount": q["coinDiscount"]}

    @staticmethod
    def reverse(customer_phone: str, order_id: str, coins_spent) -> None:
        """Return redeemed coins + free the daily slot when an order is undone
        (payment failure / cancellation / refund). Idempotent."""
        try:
            coins = int(coins_spent or 0)
        except (TypeError, ValueError):
            coins = 0
        if coins <= 0 or not customer_phone or not order_id:
            return
        try:
            WalletService.credit(
                customer_phone,
                category=Wallet.CATEGORY_REDEMPTION,
                amount=coins,
                reference_id=f"{order_id}#REVERSAL",
                reference_type="ORDER",
                description=f"Reversed {coins} YumCoins for order {order_id}",
            )
        except Exception as e:
            logger.warning(f"coin reversal credit failed for {order_id}: {e}")
        RedemptionService.release_slot(customer_phone, order_id)
