"""Wallet model — customer YumCoins ledger.

Two row shapes share this class, distinguished by ``entry_key``:
  * Summary row : ``entry_key == "BALANCE"`` — holds the running ``coins_balance``.
  * Transaction : ``entry_key == "TXN#<category>#<referenceId>"`` — one immutable
                  ledger row per source event. The sort key is deterministic so a
                  conditional ``attribute_not_exists`` write makes every credit
                  idempotent (mirrors the RiderEarnings milestone-bonus pattern).
"""
from typing import Optional
from utils.datetime_ist import now_ist_iso


class Wallet:
    """Customer YumCoins wallet ledger model."""

    # Sort key of the per-wallet running-balance summary row.
    BALANCE_KEY = "BALANCE"

    # Transaction categories.
    CATEGORY_ORDER_CASHBACK = "ORDER_CASHBACK"
    CATEGORY_REFERRAL = "REFERRAL"                # credit to the referrer
    CATEGORY_REFERRAL_SIGNUP = "REFERRAL_SIGNUP"  # credit to the referred user
    CATEGORY_REDEMPTION = "REDEMPTION"            # future DEBIT (spend at checkout)
    CATEGORY_EXPIRY = "EXPIRY"                     # future DEBIT (expiry sweep)

    # Directions.
    DIRECTION_CREDIT = "CREDIT"
    DIRECTION_DEBIT = "DEBIT"

    @staticmethod
    def txn_key(category: str, reference_id: str) -> str:
        """Deterministic sort key for a transaction row (one per source event)."""
        return f"TXN#{category}#{reference_id}"

    def __init__(
        self,
        wallet_id: str,
        entry_key: str,
        direction: Optional[str] = None,
        category: Optional[str] = None,
        amount: float = 0.0,
        balance_after: Optional[float] = None,
        expires_at: Optional[str] = None,
        reference_id: Optional[str] = None,
        reference_type: Optional[str] = None,
        description: Optional[str] = None,
        created_at: Optional[str] = None,
        # Summary-row fields (entry_key == BALANCE_KEY)
        coins_balance: float = 0.0,
        updated_at: Optional[str] = None,
    ):
        self.wallet_id = wallet_id
        self.entry_key = entry_key
        self.direction = direction
        self.category = category
        self.amount = amount
        self.balance_after = balance_after
        self.expires_at = expires_at
        self.reference_id = reference_id
        self.reference_type = reference_type
        self.description = description
        self.created_at = created_at or now_ist_iso()
        self.coins_balance = coins_balance
        self.updated_at = updated_at

    @property
    def is_summary(self) -> bool:
        return self.entry_key == self.BALANCE_KEY

    def to_dict(self) -> dict:
        """Convert to dictionary (camelCase)."""
        if self.is_summary:
            return {
                "walletId": self.wallet_id,
                "entryKey": self.entry_key,
                "coinsBalance": self.coins_balance,
                "updatedAt": self.updated_at,
            }
        result = {
            "walletId": self.wallet_id,
            "entryKey": self.entry_key,
            "direction": self.direction,
            "category": self.category,
            "amount": self.amount,
            "createdAt": self.created_at,
        }
        if self.balance_after is not None:
            result["balanceAfter"] = self.balance_after
        if self.expires_at:
            result["expiresAt"] = self.expires_at
        if self.reference_id:
            result["referenceId"] = self.reference_id
        if self.reference_type:
            result["referenceType"] = self.reference_type
        if self.description:
            result["description"] = self.description
        return result

    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "Wallet":
        """Create Wallet from DynamoDB item."""
        return cls(
            wallet_id=item.get("walletId", {}).get("S", ""),
            entry_key=item.get("entryKey", {}).get("S", ""),
            direction=item.get("direction", {}).get("S") if "direction" in item else None,
            category=item.get("category", {}).get("S") if "category" in item else None,
            amount=float(item.get("amount", {}).get("N", "0")) if "amount" in item else 0.0,
            balance_after=float(item.get("balanceAfter", {}).get("N")) if "balanceAfter" in item else None,
            expires_at=item.get("expiresAt", {}).get("S") if "expiresAt" in item else None,
            reference_id=item.get("referenceId", {}).get("S") if "referenceId" in item else None,
            reference_type=item.get("referenceType", {}).get("S") if "referenceType" in item else None,
            description=item.get("description", {}).get("S") if "description" in item else None,
            created_at=item.get("createdAt", {}).get("S", ""),
            coins_balance=float(item.get("coinsBalance", {}).get("N", "0")) if "coinsBalance" in item else 0.0,
            updated_at=item.get("updatedAt", {}).get("S") if "updatedAt" in item else None,
        )

    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format."""
        if self.is_summary:
            item = {
                "walletId": {"S": self.wallet_id},
                "entryKey": {"S": self.entry_key},
                "coinsBalance": {"N": str(self.coins_balance)},
            }
            if self.updated_at:
                item["updatedAt"] = {"S": self.updated_at}
            return item

        item = {
            "walletId": {"S": self.wallet_id},
            "entryKey": {"S": self.entry_key},
            "amount": {"N": str(self.amount)},
            "createdAt": {"S": self.created_at},
        }
        if self.direction:
            item["direction"] = {"S": self.direction}
        if self.category:
            item["category"] = {"S": self.category}
        if self.balance_after is not None:
            item["balanceAfter"] = {"N": str(self.balance_after)}
        if self.expires_at:
            item["expiresAt"] = {"S": self.expires_at}
        if self.reference_id:
            item["referenceId"] = {"S": self.reference_id}
        if self.reference_type:
            item["referenceType"] = {"S": self.reference_type}
        if self.description:
            item["description"] = {"S": self.description}
        return item
