
from typing import Dict, List, Optional
from botocore.exceptions import ClientError
from models.rider_earnings import RiderEarnings
from utils.dynamodb import dynamodb_client, TABLES
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from models.rider_earnings import RiderEarnings
from utils.datetime_ist import IST, now_ist_iso
from utils.dynamodb import TABLES, dynamodb_client
from utils.dynamodb_helpers import dynamodb_to_python

logger = Logger()

CONFIG_PK = "CONFIG#GLOBAL"
CONFIG_SK = "CONFIG"


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_cod_row(e: "RiderEarnings") -> bool:
    """COD cash-collection rows are tracked separately and must NOT roll up
    into ``totalEarnings`` (they're a liability against the rider's pocket,
    not an earned payout)."""
    return (e.entry_type or "") == "COD_AMOUNT_COLLECTED"


def _aggregate_totals(earnings_list: List[RiderEarnings]) -> Dict[str, float]:
    """Compute the period-level rollup values from a list of earnings rows.

    ``totalEarnings`` is the GROSS payout owed to the rider (delivery fees +
    tips + incentives + bonuses).  COD cash-collected rows are reported
    separately in ``totalCashCollected`` and are NOT subtracted from
    ``totalEarnings`` — clients render them as a distinct line item.
    """
    return {
        "totalDeliveries": sum(e.total_deliveries for e in earnings_list),
        "totalEarnings": sum(e.total_earnings for e in earnings_list if not _is_cod_row(e)),
        "totalTips": sum(e.tips for e in earnings_list),
        "totalIncentives": sum(e.incentives for e in earnings_list),
        "totalCashCollected": sum(e.cash_collected for e in earnings_list),
    }


class EarningsService:
    """Service for rider earnings operations."""

    ENTRY_TYPE_ORDER_EARNING = "ORDER_EARNING"
    ENTRY_TYPE_MILESTONE_BONUS = "MILESTONE_BONUS"
    ENTRY_TYPE_COD_AMOUNT_COLLECTED = "COD_AMOUNT_COLLECTED"
    BONUS_TYPE_RIDER_TARGET = "RIDER_TARGET"

    @staticmethod
    def _fetch_global_config() -> dict:
        response = dynamodb_client.get_item(
            TableName=TABLES["CONFIG"],
            Key={
                "partitionkey": {"S": CONFIG_PK},
                "sortKey": {"S": CONFIG_SK},
            },
        )
        item = response.get("Item")
        if not item:
            return {}
        payload = dynamodb_to_python(item.get("config", {"NULL": True}))
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def get_bonus_campaign(reference_time: Optional[datetime] = None) -> Optional[dict]:
        """Return the configured rider bonus campaign, if valid."""
        config = EarningsService._fetch_global_config()
        rider_bonus = config.get("riderBonusConfig")
        if not isinstance(rider_bonus, dict):
            return None
        if not rider_bonus.get("enabled", False):
            return None

        start_at = _parse_iso_datetime(rider_bonus.get("startDate"))
        end_at = _parse_iso_datetime(rider_bonus.get("endDate"))
        if not start_at or not end_at or start_at > end_at:
            return None

        milestones: List[dict] = []
        for milestone in rider_bonus.get("milestones", []):
            if not isinstance(milestone, dict):
                continue
            stops = _to_int(milestone.get("stops"))
            amount = _to_float(milestone.get("amount"))
            if stops is None or amount is None or stops <= 0 or amount < 0:
                continue
            milestones.append({"stops": stops, "amount": round(amount, 2)})
        milestones.sort(key=lambda item: item["stops"])
        if not milestones:
            return None

        target_stops = _to_int(rider_bonus.get("targetStops"))
        if target_stops is None or target_stops <= 0:
            target_stops = milestones[-1]["stops"]
        target_stops = max(target_stops, milestones[-1]["stops"])

        now = reference_time or datetime.now(IST)
        now = now if now.tzinfo else now.replace(tzinfo=IST)
        status = "upcoming"
        if start_at <= now <= end_at:
            status = "active"
        elif now > end_at:
            status = "ended"

        return {
            "enabled": True,
            "title": rider_bonus.get("title") or "Rider Bonus",
            "description": rider_bonus.get("description") or "",
            "startDate": start_at.isoformat(),
            "endDate": end_at.isoformat(),
            "targetStops": target_stops,
            "milestones": milestones,
            "status": status,
            "_startAt": start_at,
            "_endAt": end_at,
        }

    @staticmethod
    def _is_campaign_match(earning: RiderEarnings, campaign: dict) -> bool:
        return (
            earning.campaign_start_date == campaign["startDate"]
            and earning.campaign_end_date == campaign["endDate"]
        )

    @staticmethod
    def _is_bonus_entry(earning: RiderEarnings) -> bool:
        return (
            (earning.entry_type or EarningsService.ENTRY_TYPE_ORDER_EARNING)
            == EarningsService.ENTRY_TYPE_MILESTONE_BONUS
        )

    @staticmethod
    def _is_order_entry(earning: RiderEarnings) -> bool:
        return not EarningsService._is_bonus_entry(earning)

    @staticmethod
    def _earning_timestamp(earning: RiderEarnings) -> Optional[datetime]:
        created_at = _parse_iso_datetime(earning.created_at)
        if created_at:
            return created_at
        date_prefix = (earning.date or "").split("#")[0]
        if not date_prefix:
            return None
        try:
            fallback = datetime.strptime(date_prefix, "%Y-%m-%d")
        except ValueError:
            return None
        return fallback.replace(tzinfo=IST)

    @staticmethod
    def _eligible_campaign_deliveries(
        earnings_list: List[RiderEarnings], campaign: dict
    ) -> List[RiderEarnings]:
        start_at = campaign["_startAt"]
        end_at = campaign["_endAt"]
        eligible: List[RiderEarnings] = []
        for earning in earnings_list:
            if not EarningsService._is_order_entry(earning):
                continue
            occurred_at = EarningsService._earning_timestamp(earning)
            if not occurred_at:
                continue
            if start_at <= occurred_at <= end_at:
                eligible.append(earning)
        return eligible

    @staticmethod
    def get_bonus_progress(rider_id: str, reference_time: Optional[datetime] = None) -> dict:
        """Return bonus campaign details and the rider's current progress."""
        campaign = EarningsService.get_bonus_campaign(reference_time=reference_time)
        if not campaign:
            return {"campaign": None, "progress": None}

        start_date = campaign["startDate"][:10]
        end_date = campaign["endDate"][:10]
        earnings_list = EarningsService.get_earnings_for_date_range(rider_id, start_date, end_date)
        eligible_deliveries = EarningsService._eligible_campaign_deliveries(earnings_list, campaign)
        completed_stops = sum(max(0, earning.total_deliveries) for earning in eligible_deliveries)

        credited_entries = [
            earning
            for earning in earnings_list
            if EarningsService._is_bonus_entry(earning)
            and EarningsService._is_campaign_match(earning, campaign)
        ]
        credited_by_stops = {
            earning.milestone_stops: earning
            for earning in credited_entries
            if earning.milestone_stops is not None
        }

        reached_milestones: List[dict] = []
        next_milestone: Optional[dict] = None
        for milestone in campaign["milestones"]:
            stops = milestone["stops"]
            if completed_stops >= stops:
                credited = credited_by_stops.get(stops)
                reached_milestones.append(
                    {
                        "stops": stops,
                        "amount": milestone["amount"],
                        "credited": credited is not None,
                        "creditedAt": credited.created_at if credited else None,
                    }
                )
            elif next_milestone is None:
                next_milestone = {
                    "stops": stops,
                    "amount": milestone["amount"],
                    "remainingStops": max(0, stops - completed_stops),
                }

        total_bonus_earned = round(sum(entry.total_earnings for entry in credited_entries), 2)
        progress = {
            "completedStops": completed_stops,
            "remainingStops": max(0, campaign["targetStops"] - completed_stops),
            "reachedMilestones": reached_milestones,
            "nextMilestone": next_milestone,
            "totalBonusEarned": total_bonus_earned,
        }
        campaign_payload = {
            key: value
            for key, value in campaign.items()
            if not key.startswith("_")
        }
        return {"campaign": campaign_payload, "progress": progress}

    @staticmethod
    def summarize_earnings(earnings_list: List[RiderEarnings]) -> dict:
        total_deliveries = sum(e.total_deliveries for e in earnings_list)
        # Exclude COD adjustment rows: ``totalEarnings`` is the gross payout
        # owed to the rider.  Cash held by the rider is reported separately
        # via ``totalCashCollected``.
        total_earnings = round(
            sum(e.total_earnings for e in earnings_list if not _is_cod_row(e)),
            2,
        )
        total_tips = round(sum(e.tips for e in earnings_list), 2)
        total_incentives = round(sum(e.incentives for e in earnings_list), 2)
        total_bonus_earnings = round(
            sum(e.total_earnings for e in earnings_list if EarningsService._is_bonus_entry(e)),
            2,
        )
        delivery_earnings = round(
            sum(e.delivery_fees for e in earnings_list if EarningsService._is_order_entry(e)),
            2,
        )
        total_cash_collected = round(
            sum(e.cash_collected for e in earnings_list),
            2,
        )
        return {
            "totalDeliveries": total_deliveries,
            "totalEarnings": total_earnings,
            "totalTips": total_tips,
            "totalIncentives": total_incentives,
            "totalBonusEarnings": total_bonus_earnings,
            "deliveryEarnings": delivery_earnings,
            "totalCashCollected": total_cash_collected,
            "dailyBreakdown": [e.to_dict() for e in earnings_list],
        }

    @staticmethod
    def get_or_create_daily_earnings(rider_id: str, date: str) -> RiderEarnings:
        """Get aggregated earnings summary for a specific date."""
        try:
            earnings_list = EarningsService.get_earnings_for_date_range(rider_id, date, date)
            summary = EarningsService.summarize_earnings(earnings_list)
            return RiderEarnings(
                rider_id=rider_id,
                date=date,
                total_deliveries=summary["totalDeliveries"],
                total_earnings=summary["totalEarnings"],
                delivery_fees=summary["deliveryEarnings"],
                tips=summary["totalTips"],
                incentives=summary["totalIncentives"],
            )
        except ClientError as e:
            raise Exception(f"Failed to get earnings: {str(e)}")

    @staticmethod
    def add_delivery(
        rider_id: str,
        order_id: str,
        delivery_fee: float,
        tip: float = 0.0,
        incentives: float = 0.0,
        delivery_duration_minutes: int = 0,
        date_override: Optional[str] = None,
    ):
        """Add a delivery to rider's earnings.

        `incentives` rolls up any per-order bonuses (e.g. longDistanceBonus).
        Total earnings = delivery_fee + tip + incentives.

        `date_override` (YYYY-MM-DD) pins the sort-key prefix so multiple
        call sites for the same order (e.g. cash-collected safety net +
        DELIVERED finalize) overwrite the same row instead of creating
        duplicates across a UTC midnight boundary. Defaults to today UTC.
        """
        try:
            date_str = date_override or datetime.utcnow().strftime('%Y-%m-%d')
            earnings = RiderEarnings(
                rider_id=rider_id,
                date=f"{date_str}#{order_id}",
                total_deliveries=1,
                total_earnings=delivery_fee + tip + incentives,
                delivery_fees=delivery_fee,
                tips=tip,
                incentives=incentives,
                delivery_duration_minutes=delivery_duration_minutes,
                order_id=order_id,
                settled=False,
                settled_at=None,
                entry_type=EarningsService.ENTRY_TYPE_ORDER_EARNING,
            )

            dynamodb_client.put_item(
                TableName=TABLES["EARNINGS"],
                Item=earnings.to_dynamodb_item(),
            )
        except ClientError as e:
            raise Exception(f"Failed to add delivery: {str(e)}")

    @staticmethod
    def add_milestone_bonus(
        rider_id: str,
        campaign: dict,
        milestone_stops: int,
        amount: float,
    ) -> bool:
        """Add a milestone bonus entry if it has not already been credited."""
        credited_at = now_ist_iso()
        date_key = f"{credited_at[:10]}#BONUS#{campaign['startDate'][:10]}#{milestone_stops}"
        bonus_label = f"Milestone Bonus - {milestone_stops} stops"
        # Milestone bonus rows store the credited amount in ``total_earnings``.
        # ``incentives`` is intentionally NOT populated — that bucket is
        # reserved for per-order incentives (e.g. longDistanceBonus) so the
        # Incentives mini-stat doesn't double-count milestone payouts.
        # Aggregation surfaces milestone amounts via ``summarize_earnings`` →
        # ``totalBonusEarnings`` (entryType-filtered, reads total_earnings).
        earning = RiderEarnings(
            rider_id=rider_id,
            date=date_key,
            total_deliveries=0,
            total_earnings=round(amount, 2),
            delivery_fees=0.0,
            tips=0.0,
            incentives=0.0,
            order_id=None,
            settled=False,
            settled_at=None,
            created_at=credited_at,
            entry_type=EarningsService.ENTRY_TYPE_MILESTONE_BONUS,
            bonus_type=EarningsService.BONUS_TYPE_RIDER_TARGET,
            milestone_stops=milestone_stops,
            campaign_start_date=campaign["startDate"],
            campaign_end_date=campaign["endDate"],
            bonus_label=bonus_label,
        )
        try:
            dynamodb_client.put_item(
                TableName=TABLES["EARNINGS"],
                Item=earning.to_dynamodb_item(),
                ConditionExpression="attribute_not_exists(#date)",
                ExpressionAttributeNames={"#date": "date"},
            )
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return False
            raise Exception(f"Failed to add milestone bonus: {str(e)}")

    @staticmethod
    def apply_milestone_bonuses(rider_id: str) -> List[dict]:
        """Credit all newly reached milestones for the active campaign."""
        bonus_state = EarningsService.get_bonus_progress(rider_id)
        campaign = bonus_state.get("campaign")
        progress = bonus_state.get("progress")
        if not campaign or not progress or campaign.get("status") != "active":
            return []

        credited_now: List[dict] = []
        credited_stops = {
            milestone["stops"]
            for milestone in progress.get("reachedMilestones", [])
            if milestone.get("credited")
        }
        for milestone in campaign.get("milestones", []):
            stops = milestone["stops"]
            if progress["completedStops"] < stops or stops in credited_stops:
                continue
            inserted = EarningsService.add_milestone_bonus(
                rider_id=rider_id,
                campaign=campaign,
                milestone_stops=stops,
                amount=milestone["amount"],
            )
            if inserted:
                credited_now.append(
                    {
                        "stops": stops,
                        "amount": milestone["amount"],
                    }
                )
        return credited_now

    @staticmethod
    def get_earnings_for_date_range(
        rider_id: str, start_date: str, end_date: str
    ) -> List[RiderEarnings]:
        """Get earnings for a date range."""
    def record_cash_collected(
        rider_id: str,
        order_id: str,
        cash_amount: float,
        date_override: Optional[str] = None,
    ) -> None:
        """Record a COD cash-collection event as a distinct earnings row.

        Sort key: ``<YYYY-MM-DD>#COD#<orderId>`` (intentionally distinct from
        the delivery row at ``<YYYY-MM-DD>#<orderId>`` so the two events live
        side by side in the same query window).

        The row is tagged ``entryType=COD_AMOUNT_COLLECTED`` and is EXCLUDED
        from the ``totalEarnings`` rollup; the cash amount is surfaced via
        the separate ``totalCashCollected`` aggregate so clients can render
        it as its own line item (it represents a liability of the rider, not
        an earned payout).

        Component fields (deliveryFees / tips / incentives) stay 0.
        ``cashCollected`` carries the gross amount for display /
        reconciliation. ``totalEarnings`` is stored as ``-cash_amount`` for
        backward compatibility with older aggregates that summed the field
        directly; new code paths use the explicit COD filter.
        """
        try:
            cash_amount = float(cash_amount or 0)
            date_str = date_override or datetime.utcnow().strftime('%Y-%m-%d')
            earnings = RiderEarnings(
                rider_id=rider_id,
                date=f"{date_str}#COD#{order_id}",
                total_deliveries=0,
                total_earnings=-cash_amount,
                delivery_fees=0.0,
                tips=0.0,
                incentives=0.0,
                delivery_duration_minutes=0,
                order_id=order_id,
                settled=False,
                settled_at=None,
                entry_type=EarningsService.ENTRY_TYPE_COD_AMOUNT_COLLECTED,
                payment_channel="COD",
                cash_collected=cash_amount,
            )

            dynamodb_client.put_item(
                TableName=TABLES['EARNINGS'],
                Item=earnings.to_dynamodb_item()
            )
        except ClientError as e:
            raise Exception(f"Failed to record cash collected: {str(e)}")

    @staticmethod
    def get_earnings_for_date_range(rider_id: str, start_date: str, end_date: str) -> List[RiderEarnings]:
        """Get earnings for a date range"""
        try:
            response = dynamodb_client.query(
                TableName=TABLES["EARNINGS"],
                KeyConditionExpression="riderId = :riderId AND #date BETWEEN :start AND :end",
                ExpressionAttributeNames={
                    "#date": "date",
                },
                ExpressionAttributeValues={
                    ":riderId": {"S": rider_id},
                    ":start": {"S": f"{start_date}#"},
                    ":end": {"S": f"{end_date}#\uffff"},
                },
            )

            earnings_list = [
                RiderEarnings.from_dynamodb_item(item)
                for item in response.get("Items", [])
            ]
            earnings_list.sort(
                key=lambda earning: (earning.date or "", earning.created_at or ""),
                reverse=True,
            )
            return earnings_list
        except ClientError as e:
            raise Exception(f"Failed to get earnings: {str(e)}")

    @staticmethod
    def get_today_earnings(rider_id: str) -> RiderEarnings:
        """Get today's earnings."""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        return EarningsService.get_or_create_daily_earnings(rider_id, today)

    @staticmethod
    def get_weekly_earnings(rider_id: str) -> dict:
        """Get this week's earnings summary."""
        today = datetime.utcnow()
        start_of_week = today - timedelta(days=today.weekday())

        start_date = start_of_week.strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")

        earnings_list = EarningsService.get_earnings_for_date_range(rider_id, start_date, end_date)
        totals = _aggregate_totals(earnings_list)

        return {
            "period": "week",
            "startDate": start_date,
            "endDate": end_date,
            **EarningsService.summarize_earnings(earnings_list),
            **totals,
            "dailyBreakdown": [e.to_dict() for e in earnings_list]
        }

    @staticmethod
    def get_monthly_earnings(rider_id: str) -> dict:
        """Get this month's earnings summary."""
        today = datetime.utcnow()
        start_of_month = today.replace(day=1)

        start_date = start_of_month.strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")

        earnings_list = EarningsService.get_earnings_for_date_range(rider_id, start_date, end_date)
        totals = _aggregate_totals(earnings_list)

        return {
            "period": "month",
            "startDate": start_date,
            "endDate": end_date,
            **EarningsService.summarize_earnings(earnings_list),
            **totals,
            "dailyBreakdown": [e.to_dict() for e in earnings_list]
        }

    @staticmethod
    def settle_earnings_for_orders(
        rider_id: str,
        order_ids: List[str],
        start_date: str,
        end_date: str,
        settlement_id: str,
    ) -> List[str]:
        """Mark earnings rows as settled for matching order IDs in date range."""
        try:
            earnings_list = EarningsService.get_earnings_for_date_range(
                rider_id, start_date, end_date
            )

            settled_at = datetime.utcnow().isoformat()
            updated_order_ids: List[str] = []

            for earning in earnings_list:
                if not earning.order_id:
                    continue
                if earning.order_id not in order_ids:
                    continue
                # COD rows are cash physically collected by the rider on behalf
                # of the platform — they are not part of the rider's payout and
                # must never be flagged as "settled". Skip them so settling an
                # order only touches its ORDER_EARNING (and any bonus) rows.
                if _is_cod_row(earning):
                    logger.info(
                        f"Skipping COD row during settle: riderId={rider_id}, "
                        f"date={earning.date}, orderId={earning.order_id}"
                    )
                    continue

                dynamodb_client.update_item(
                    TableName=TABLES["EARNINGS"],
                    Key={
                        "riderId": {"S": rider_id},
                        "date": {"S": earning.date},
                    },
                    UpdateExpression="SET settled = :settled, settledAt = :settledAt, settlementId = :settlementId",
                    ExpressionAttributeValues={
                        ":settled": {"BOOL": True},
                        ":settledAt": {"S": settled_at},
                        ":settlementId": {"S": settlement_id},
                    },
                )
                updated_order_ids.append(earning.order_id)

            return updated_order_ids
        except ClientError as e:
            raise Exception(f"Failed to settle earnings: {str(e)}")

    @staticmethod
    def settle_cod_for_orders(
        rider_id: str,
        order_ids: List[str],
        start_date: str,
        end_date: str,
        settlement_id: str,
    ) -> List[str]:
        """Mark COD cash-collected rows as returned/settled for matching orders."""
        try:
            earnings_list = EarningsService.get_earnings_for_date_range(
                rider_id, start_date, end_date
            )

            settled_at = datetime.utcnow().isoformat()
            updated_order_ids: List[str] = []

            for earning in earnings_list:
                if not earning.order_id:
                    continue
                if earning.order_id not in order_ids:
                    continue
                if not _is_cod_row(earning):
                    continue

                dynamodb_client.update_item(
                    TableName=TABLES["EARNINGS"],
                    Key={
                        "riderId": {"S": rider_id},
                        "date": {"S": earning.date},
                    },
                    UpdateExpression="SET settled = :settled, settledAt = :settledAt, settlementId = :settlementId",
                    ExpressionAttributeValues={
                        ":settled": {"BOOL": True},
                        ":settledAt": {"S": settled_at},
                        ":settlementId": {"S": settlement_id},
                    },
                )
                updated_order_ids.append(earning.order_id)

            return updated_order_ids
        except ClientError as e:
            raise Exception(f"Failed to settle COD cash: {str(e)}")
