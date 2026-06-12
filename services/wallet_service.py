"""Wallet service — credit/read the customer YumCoins ledger.

Mirrors the RiderEarnings ledger idempotency pattern: each transaction row has a
deterministic sort key (``TXN#<category>#<referenceId>``) and is written with a
conditional put (``attribute_not_exists(entryKey)``) so retries / duplicate
deliveries never double-credit. A per-wallet ``BALANCE`` summary row holds the
running ``coinsBalance`` (atomic ``ADD``) so balance reads are O(1).
"""
from typing import Any, Dict, Optional

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from models.wallet import Wallet
from utils.datetime_ist import now_ist_iso
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python

logger = Logger()

CONFIG_PK = "CONFIG#GLOBAL"
CONFIG_SK = "CONFIG"
DEFAULT_CONVERSION_RATE = 1.0


def _fetch_wallet_conversion_rate() -> float:
    """Read ₹-per-YumCoin from CONFIG#GLOBAL.config.walletConfig.yumConversionRate."""
    try:
        response = dynamodb_client.get_item(
            TableName=TABLES["CONFIG"],
            Key={"partitionkey": {"S": CONFIG_PK}, "sortKey": {"S": CONFIG_SK}},
        )
        item = response.get("Item")
        if not item:
            return DEFAULT_CONVERSION_RATE
        config = dynamodb_to_python(item.get("config", {"NULL": True}))
        wallet_cfg = config.get("walletConfig") if isinstance(config, dict) else None
        if isinstance(wallet_cfg, dict):
            rate = float(wallet_cfg.get("yumConversionRate"))
            if rate > 0:
                return rate
    except (TypeError, ValueError):
        pass
    except Exception as e:
        logger.warning(f"Failed to fetch wallet conversion rate: {e}")
    return DEFAULT_CONVERSION_RATE


class WalletService:
    """Customer YumCoins wallet operations."""

    @staticmethod
    def credit(
        wallet_id: str,
        *,
        category: str,
        amount: float,
        reference_id: str,
        expires_at: Optional[str] = None,
        reference_type: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Idempotently credit YumCoins to a wallet.

        A repeat call with the same (category, reference_id) is a no-op
        (``applied=False``): the deterministic sort key + conditional put
        guarantee a single credit per source event.

        Returns ``{"applied": bool, "coinsBalance": float|None, "entryKey": str}``.
        """
        amount = round(float(amount or 0), 2)
        if amount <= 0:
            return {"applied": False, "reason": "NON_POSITIVE_AMOUNT", "coinsBalance": None, "entryKey": None}

        entry_key = Wallet.txn_key(category, reference_id)
        txn = Wallet(
            wallet_id=wallet_id,
            entry_key=entry_key,
            direction=Wallet.DIRECTION_CREDIT,
            category=category,
            amount=amount,
            expires_at=expires_at,
            reference_id=reference_id,
            reference_type=reference_type,
            description=description,
            created_at=now_ist_iso(),
        )
        try:
            dynamodb_client.put_item(
                TableName=TABLES["WALLET"],
                Item=txn.to_dynamodb_item(),
                ConditionExpression="attribute_not_exists(entryKey)",
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                logger.info(f"[wallet={wallet_id}] credit already applied for {entry_key}; skipping")
                return {"applied": False, "reason": "ALREADY_APPLIED", "coinsBalance": None, "entryKey": entry_key}
            raise Exception(f"Failed to write wallet transaction: {str(e)}")

        # Ledger row committed exactly once → roll the cached balance forward.
        new_balance = WalletService._apply_balance_delta(wallet_id, amount)

        # Best-effort: stamp balanceAfter on the ledger row for display/audit.
        try:
            dynamodb_client.update_item(
                TableName=TABLES["WALLET"],
                Key={"walletId": {"S": wallet_id}, "entryKey": {"S": entry_key}},
                UpdateExpression="SET balanceAfter = :b",
                ExpressionAttributeValues={":b": {"N": str(new_balance)}},
            )
        except ClientError as e:
            logger.warning(f"[wallet={wallet_id}] failed to stamp balanceAfter on {entry_key}: {e}")

        return {"applied": True, "coinsBalance": new_balance, "entryKey": entry_key}

    @staticmethod
    def _apply_balance_delta(wallet_id: str, delta: float) -> float:
        """Atomically add ``delta`` to the BALANCE summary row; return new balance."""
        resp = dynamodb_client.update_item(
            TableName=TABLES["WALLET"],
            Key={"walletId": {"S": wallet_id}, "entryKey": {"S": Wallet.BALANCE_KEY}},
            UpdateExpression="ADD coinsBalance :d SET updatedAt = :t",
            ExpressionAttributeValues={":d": {"N": str(round(delta, 2))}, ":t": {"S": now_ist_iso()}},
            ReturnValues="UPDATED_NEW",
        )
        return float(resp["Attributes"]["coinsBalance"]["N"])

    @staticmethod
    def debit(
        wallet_id: str,
        *,
        category: str,
        amount: float,
        reference_id: str,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Idempotently debit YumCoins (redemption / expiry).

        DESIGN-ONLY — not yet wired to any route. Guards against driving the
        balance negative via a conditional update on the summary row; if the
        balance is insufficient the just-written ledger row is rolled back so
        the debit can be retried later.
        """
        amount = round(float(amount or 0), 2)
        if amount <= 0:
            return {"applied": False, "reason": "NON_POSITIVE_AMOUNT"}

        entry_key = Wallet.txn_key(category, reference_id)
        txn = Wallet(
            wallet_id=wallet_id,
            entry_key=entry_key,
            direction=Wallet.DIRECTION_DEBIT,
            category=category,
            amount=amount,
            reference_id=reference_id,
            description=description,
            created_at=now_ist_iso(),
        )
        try:
            dynamodb_client.put_item(
                TableName=TABLES["WALLET"],
                Item=txn.to_dynamodb_item(),
                ConditionExpression="attribute_not_exists(entryKey)",
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return {"applied": False, "reason": "ALREADY_APPLIED", "entryKey": entry_key}
            raise Exception(f"Failed to write wallet debit: {str(e)}")

        try:
            resp = dynamodb_client.update_item(
                TableName=TABLES["WALLET"],
                Key={"walletId": {"S": wallet_id}, "entryKey": {"S": Wallet.BALANCE_KEY}},
                UpdateExpression="ADD coinsBalance :neg SET updatedAt = :t",
                ConditionExpression="attribute_exists(coinsBalance) AND coinsBalance >= :amt",
                ExpressionAttributeValues={
                    ":neg": {"N": str(-amount)},
                    ":amt": {"N": str(amount)},
                    ":t": {"S": now_ist_iso()},
                },
                ReturnValues="UPDATED_NEW",
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                try:
                    dynamodb_client.delete_item(
                        TableName=TABLES["WALLET"],
                        Key={"walletId": {"S": wallet_id}, "entryKey": {"S": entry_key}},
                    )
                except ClientError:
                    pass
                return {"applied": False, "reason": "INSUFFICIENT_BALANCE", "entryKey": entry_key}
            raise Exception(f"Failed to debit wallet balance: {str(e)}")

        return {
            "applied": True,
            "coinsBalance": float(resp["Attributes"]["coinsBalance"]["N"]),
            "entryKey": entry_key,
        }

    @staticmethod
    def get_balance(wallet_id: str) -> Dict[str, Any]:
        """Return the wallet's coin balance plus its real-₹ value."""
        coins = 0.0
        try:
            response = dynamodb_client.get_item(
                TableName=TABLES["WALLET"],
                Key={"walletId": {"S": wallet_id}, "entryKey": {"S": Wallet.BALANCE_KEY}},
            )
            item = response.get("Item")
            if item:
                coins = float(item.get("coinsBalance", {}).get("N", "0"))
        except ClientError as e:
            raise Exception(f"Failed to read wallet balance: {str(e)}")

        rate = _fetch_wallet_conversion_rate()
        return {
            "coinsBalance": round(coins, 2),
            "conversionRate": rate,
            "cashValue": round(coins * rate, 2),
        }

    @staticmethod
    def get_transactions(wallet_id: str, limit: int = 50) -> Dict[str, Any]:
        """Return the wallet's transaction history, newest-first.

        Transaction sort keys are deterministic (not time-ordered), so — like
        ``EarningsService.get_earnings_for_date_range`` — rows are sorted by
        ``createdAt`` in memory and truncated to ``limit``.
        """
        try:
            response = dynamodb_client.query(
                TableName=TABLES["WALLET"],
                KeyConditionExpression="walletId = :w AND begins_with(entryKey, :p)",
                ExpressionAttributeValues={
                    ":w": {"S": wallet_id},
                    ":p": {"S": "TXN#"},
                },
            )
        except ClientError as e:
            raise Exception(f"Failed to read wallet transactions: {str(e)}")

        txns = [Wallet.from_dynamodb_item(it) for it in response.get("Items", [])]
        txns.sort(key=lambda t: t.created_at or "", reverse=True)
        if limit and limit > 0:
            txns = txns[:limit]
        return {"transactions": [t.to_dict() for t in txns]}
