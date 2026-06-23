"""Refer-and-earn + order-cashback service.

Responsibilities:
  * Generate / look up a per-customer shareable referral code, stored as a
    ``REFERRALCODE#<code>`` row in the CONFIG table (mirrors the ``COUPON#``
    precedent — no extra index).
  * At signup: reward both the referrer and the new (referred) user
    (``credit_signup_referral``).
  * On order delivery: credit the buyer's own order-cashback YumCoins.

All YumCoin movements go through the idempotent ``WalletService.credit`` and all
tunables (referral rewards, cashback %) live in the dedicated YumCoins config row
(``CONFIG#YUMCOINS``); see services/yumcoins_config_service.py.
"""
import math
import random
from typing import Optional

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from models.wallet import Wallet
from services.user_service import UserService
from services.wallet_service import WalletService
from services.yumcoins_config_service import fetch_yumcoins_config
from utils.datetime_ist import now_ist_iso
from utils.dynamodb import dynamodb_client, TABLES

logger = Logger()

CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # no ambiguous 0/O/1/I
CODE_LEN = 6
CODE_PK_PREFIX = "REFERRALCODE#"
CODE_SK = "DETAILS"


class ReferralService:
    """Refer-and-earn + order cashback."""

    # ------------------------------------------------------------------ config
    @staticmethod
    def _fetch_global_config() -> dict:
        """YumCoins config map ({referralConfig, orderCashbackConfig, ...})."""
        return fetch_yumcoins_config()

    # --------------------------------------------------- referral code lifecycle
    @staticmethod
    def _gen_code() -> str:
        return "".join(random.choice(CODE_ALPHABET) for _ in range(CODE_LEN))

    @staticmethod
    def mint_code_for(phone: str, attempts: int = 5) -> Optional[str]:
        """Generate a unique referral code and persist its reverse-lookup row.

        Writes a CONFIG row ``PK=REFERRALCODE#<code>`` / ``SK=DETAILS`` carrying
        the referrer phone, guarded by ``attribute_not_exists`` so a colliding
        code is retried. Returns the code, or None on failure (non-fatal for
        signup — the customer simply has no code yet and can backfill later).
        """
        for _ in range(max(1, attempts)):
            code = ReferralService._gen_code()
            try:
                dynamodb_client.put_item(
                    TableName=TABLES["CONFIG"],
                    Item={
                        "partitionkey": {"S": f"{CODE_PK_PREFIX}{code}"},
                        "sortKey": {"S": CODE_SK},
                        "referrerPhone": {"S": phone},
                        "createdAt": {"S": now_ist_iso()},
                    },
                    ConditionExpression="attribute_not_exists(partitionkey)",
                )
                return code
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                    continue  # collision — try another code
                logger.warning(f"Failed to mint referral code for {phone}: {e}")
                return None
        logger.warning(f"Exhausted referral code attempts for {phone}")
        return None

    @staticmethod
    def resolve_referrer(code: str) -> Optional[str]:
        """Map a referral code to the referrer's phone, or None if unknown."""
        if not code:
            return None
        try:
            response = dynamodb_client.get_item(
                TableName=TABLES["CONFIG"],
                Key={
                    "partitionkey": {"S": f"{CODE_PK_PREFIX}{code.strip().upper()}"},
                    "sortKey": {"S": CODE_SK},
                },
            )
            item = response.get("Item")
            if not item:
                return None
            return item.get("referrerPhone", {}).get("S") or None
        except ClientError as e:
            logger.warning(f"Failed to resolve referral code {code}: {e}")
            return None

    @staticmethod
    def ensure_code(user) -> Optional[str]:
        """Return the user's referral code, minting + persisting one if absent."""
        existing = getattr(user, "referral_code", None)
        if existing:
            return existing
        code = ReferralService.mint_code_for(user.phone)
        if code:
            UserService.update_user(user.phone, "CUSTOMER", {"referralCode": code})
        return code

    @staticmethod
    def credit_signup_referral(referrer_phone: str, referee_phone: str) -> None:
        """Reward both parties immediately when ``referee_phone`` signs up with
        ``referrer_phone``'s code.

        The referrer gets ``referrerReward`` and the referee gets
        ``refereeReward`` YumCoins (both from global config). Idempotent: each
        credit is keyed by the referee phone, so a retried signup cannot
        double-pay. Self-referral is a no-op for the referrer leg.
        """
        cfg = ReferralService._fetch_global_config().get("referralConfig") or {}
        if not cfg.get("enabled"):
            return

        referrer_reward = ReferralService._as_int(cfg.get("referrerReward"))
        referee_reward = ReferralService._as_int(cfg.get("refereeReward"))

        # Reward the referrer (the existing user whose code was used).
        if referrer_phone and referrer_phone != referee_phone and referrer_reward > 0:
            WalletService.credit(
                referrer_phone,
                category=Wallet.CATEGORY_REFERRAL,
                amount=referrer_reward,
                reference_id=referee_phone,  # uniquely identifies this referral
                reference_type="REFERRAL",
                description=f"Referral reward — {referee_phone} signed up with your code",
            )

        # Reward the new (referred) user.
        if referee_reward > 0:
            WalletService.credit(
                referee_phone,
                category=Wallet.CATEGORY_REFERRAL_SIGNUP,
                amount=referee_reward,
                reference_id=referee_phone,
                reference_type="REFERRAL",
                description=f"Welcome referral reward (referred by {referrer_phone})",
            )

    # --------------------------------------------------- order-delivered hook
    @staticmethod
    def on_order_delivered(order) -> None:
        """Credit the buyer's own order cashback for a delivered order.

        Referral rewards are NOT paid here — they are credited up front at
        signup (see ``credit_signup_referral``). The caller wraps this so a
        cashback failure can never block delivery completion.
        """
        config = ReferralService._fetch_global_config()
        order_id = getattr(order, "order_id", "?")

        try:
            ReferralService._credit_order_cashback(order, config.get("orderCashbackConfig") or {})
        except Exception as e:
            logger.warning(f"[orderId={order_id}] cashback credit failed: {e}", exc_info=True)

    # ------------------------------------------------------------- cashback
    @staticmethod
    def _credit_order_cashback(order, cfg: dict) -> None:
        if not cfg.get("enabled"):
            return
        try:
            pct = float(cfg.get("percentage", 0) or 0)
        except (TypeError, ValueError):
            pct = 0.0
        if pct <= 0:
            return

        basis = float(getattr(order, "grand_total", 0) or 0)
        coins = math.floor(basis * pct / 100.0)

        cap = cfg.get("maxCoinsPerOrder")
        if cap is not None:
            try:
                coins = min(coins, int(float(cap)))
            except (TypeError, ValueError):
                pass
        if coins <= 0:
            return

        WalletService.credit(
            order.customer_phone,
            category=Wallet.CATEGORY_ORDER_CASHBACK,
            amount=coins,
            reference_id=order.order_id,
            reference_type="ORDER",
            description=f"Cashback for order {order.order_id}",
        )

    @staticmethod
    def _as_int(value) -> int:
        try:
            return int(float(value or 0))
        except (TypeError, ValueError):
            return 0
