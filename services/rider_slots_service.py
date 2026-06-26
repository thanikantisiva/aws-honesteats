"""Rider slots service — Zomato-style committed shift booking + compliance.

Slot *settings* live in the dedicated rider config row (``CONFIG#RIDER`` / ``CONFIG``)
alongside ``riderBonusConfig`` (see services/rider_config_service.py; reads fall back
to the legacy ``CONFIG#GLOBAL`` keys until migrated):

    config.riderSlotsSettings  -> { complianceThresholdPct, maxRejectionsAllowed,
                                    noShowPenalty, staleSeconds, noShowBanThreshold,
                                    noShowBanWindowDays, noShowBanDurationDays }

Live booking state is kept in SEPARATE items in the SAME config table so the admin
full-replace of the config map can never clobber it:

    RIDER_SLOT#<slotId>          / BOOKINGS           -> { bookedCount, capacity, riderIds(SS) }
    RIDER_SLOT_BOOKINGS#<rider>  / <date>#<slotId>    -> per-rider booking + compliance state
    RIDER_SLOT_BOOKINGS#<rider>  / PROFILE            -> { noShowEvents, bookingBanUntil }

Rider online sessions (login/logout) used for compliance live at
``RIDER_SESSIONS#<rider>`` / ``<date>`` (written by RiderService.set_active_status).
"""
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

from services.rider_config_service import RIDER_CONFIG_PK, fetch_rider_config
from utils.dynamodb import dynamodb_client, TABLES, generate_id
from utils.dynamodb_helpers import python_to_dynamodb, dynamodb_to_python
from utils.datetime_ist import IST, now_ist_iso

logger = Logger()

# Rider config (settings/bonus) lives in its own row now; slots are separate items.
CONFIG_PK = RIDER_CONFIG_PK
CONFIG_SK = "CONFIG"
SETTINGS_KEY = "riderSlotsSettings"

# Each slot is its OWN item (so the CONFIG#GLOBAL row never bloats past 400KB):
#   PK = RIDER_SLOT#<slotId>, SK = "DEF"   (shares the partition with the seat counter)
# A date GSI (slotBucket / slotDateSk) answers the T-7..T+7 range query in one shot.
SLOT_DEF_SK = "DEF"
SLOT_GSI = "slotDate-index"
SLOT_GSI_BUCKET = "SLOT"
SLOT_RANGE_PAST_DAYS = 7
SLOT_RANGE_FUTURE_DAYS = 7

DEFAULT_SETTINGS = {
    "complianceThresholdPct": 0.8,
    "maxRejectionsAllowed": 1,
    "noShowPenalty": 50,
    "staleSeconds": 90,
    "noShowBanThreshold": 2,
    "noShowBanWindowDays": 14,
    "noShowBanDurationDays": 7,
    # How many days before a slot's date it opens for booking. A slot dated D
    # becomes bookable at 00:00 IST of (D - releaseDaysInAdvance). Riders SEE the
    # slot earlier (blocked in the UI) so they can plan ahead. An explicit
    # per-slot `releaseAt` overrides this window.
    "releaseDaysInAdvance": 1,
    # Riders may cancel a booked slot only up to this many hours before the slot
    # starts (0 = allowed any time before start).
    "cancelCutoffHours": 2,
}

# Per-rider booking partition + sort-key helpers
def _bookings_pk(rider_id: str) -> str:
    return f"RIDER_SLOT_BOOKINGS#{rider_id}"


def _booking_sk(date: str, slot_id: str) -> str:
    return f"{date}#{slot_id}"


def _counter_pk(slot_id: str) -> str:
    return f"RIDER_SLOT#{slot_id}"


def _slot_date_sk(date: str, slot_id: str) -> str:
    return f"{date}#{slot_id}"


def _sessions_pk(rider_id: str) -> str:
    return f"RIDER_SESSIONS#{rider_id}"


class SlotError(Exception):
    """Domain error for slot operations; carries an HTTP status + machine code."""

    def __init__(self, code: str, message: str, http_status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


# ----------------------------------------------------------------------------- datetime utils
def _now() -> datetime:
    return datetime.now(IST)


def _slot_dt(date_str: str, hhmm: str) -> datetime:
    """Build an IST-aware datetime from 'YYYY-MM-DD' + 'HH:MM'."""
    y, mo, d = (int(x) for x in date_str.split("-"))
    h, mi = (int(x) for x in hhmm.split(":"))
    return datetime(y, mo, d, h, mi, tzinfo=IST)


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=IST)


def _duration_minutes(start: str, end: str) -> int:
    sh, sm = (int(x) for x in start.split(":"))
    eh, em = (int(x) for x in end.split(":"))
    mins = (eh * 60 + em) - (sh * 60 + sm)
    return mins if mins > 0 else 0


def _overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> float:
    """Return overlap in seconds between [a_start,a_end] and [b_start,b_end]."""
    latest_start = max(a_start, b_start)
    earliest_end = min(a_end, b_end)
    delta = (earliest_end - latest_start).total_seconds()
    return delta if delta > 0 else 0.0


def _order_rider_payout(order) -> float:
    """Rider delivery payout for an order: revenue.riderRevenue.finalPayout, else delivery_fee."""
    rev = getattr(order, "revenue", None)
    if isinstance(rev, dict):
        rr = rev.get("riderRevenue") or {}
        try:
            v = rr.get("finalPayout")
            if v is not None:
                return float(v)
        except (TypeError, ValueError):
            pass
    try:
        return float(getattr(order, "delivery_fee", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


class RiderSlotsService:
    """Admin-managed slot config + rider booking + compliance."""

    # ------------------------------------------------------------------ config row helpers
    @staticmethod
    def _get_config_map() -> dict:
        # Reads the dedicated rider config row, falling back to legacy CONFIG#GLOBAL.
        return fetch_rider_config()

    @staticmethod
    def _write_config_key(key: str, value) -> None:
        """Targeted update of a single key inside the config map (siblings untouched)."""
        names = {"#config": "config", "#k": key}
        try:
            dynamodb_client.update_item(
                TableName=TABLES["CONFIG"],
                Key={"partitionkey": {"S": CONFIG_PK}, "sortKey": {"S": CONFIG_SK}},
                UpdateExpression="SET #config.#k = :v, updatedAt = :u",
                ExpressionAttributeNames=names,
                ExpressionAttributeValues={
                    ":v": python_to_dynamodb(value),
                    ":u": {"S": now_ist_iso()},
                },
                ConditionExpression="attribute_exists(#config)",
            )
        except ClientError:
            # config attribute (or the row) doesn't exist yet — merge the whole map.
            cfg = RiderSlotsService._get_config_map()
            cfg[key] = value
            dynamodb_client.update_item(
                TableName=TABLES["CONFIG"],
                Key={"partitionkey": {"S": CONFIG_PK}, "sortKey": {"S": CONFIG_SK}},
                UpdateExpression="SET #config = :cfg, updatedAt = :u",
                ExpressionAttributeNames={"#config": "config"},
                ExpressionAttributeValues={
                    ":cfg": python_to_dynamodb(cfg),
                    ":u": {"S": now_ist_iso()},
                },
            )

    @staticmethod
    def get_settings() -> dict:
        cfg = RiderSlotsService._get_config_map()
        merged = dict(DEFAULT_SETTINGS)
        stored = cfg.get(SETTINGS_KEY)
        if isinstance(stored, dict):
            merged.update({k: v for k, v in stored.items() if v is not None})
        return merged

    # Fields stored as floats; everything else in DEFAULT_SETTINGS is a non-negative int.
    _FLOAT_SETTINGS = {"complianceThresholdPct", "noShowPenalty"}

    @staticmethod
    def update_settings(partial: dict) -> dict:
        """Validate + persist rider slot settings (riderSlotsSettings in CONFIG#RIDER).

        Accepts a partial dict (unknown keys ignored); merges over current values.
        """
        if not isinstance(partial, dict):
            raise SlotError("InvalidSettings", "settings must be an object")

        merged = RiderSlotsService.get_settings()
        for key in DEFAULT_SETTINGS:
            if key not in partial or partial[key] is None:
                continue
            try:
                merged[key] = (
                    float(partial[key]) if key in RiderSlotsService._FLOAT_SETTINGS
                    else int(float(partial[key]))
                )
            except (TypeError, ValueError):
                raise SlotError("InvalidSettings", f"{key} must be a number")

        if not (0.0 <= float(merged["complianceThresholdPct"]) <= 1.0):
            raise SlotError("InvalidSettings", "complianceThresholdPct must be between 0 and 1")
        for key in DEFAULT_SETTINGS:
            if float(merged[key]) < 0:
                raise SlotError("InvalidSettings", f"{key} must be >= 0")

        RiderSlotsService._write_config_key(SETTINGS_KEY, merged)
        return RiderSlotsService.get_settings()

    # ------------------------------------------------------------------ rider bonus config
    @staticmethod
    def get_bonus_config() -> dict:
        """Raw riderBonusConfig from CONFIG#RIDER (empty dict if unset)."""
        bonus = RiderSlotsService._get_config_map().get("riderBonusConfig")
        return bonus if isinstance(bonus, dict) else {}

    @staticmethod
    def update_bonus_config(data: dict) -> dict:
        """Validate + persist the rider bonus campaign (riderBonusConfig in CONFIG#RIDER)."""
        if not isinstance(data, dict):
            raise SlotError("InvalidBonus", "bonusConfig must be an object")

        clean: dict = {"enabled": bool(data.get("enabled", False))}

        try:
            clean["targetStops"] = int(float(data.get("targetStops") or 0))
        except (TypeError, ValueError):
            raise SlotError("InvalidBonus", "targetStops must be a number")
        if clean["targetStops"] < 0:
            raise SlotError("InvalidBonus", "targetStops must be >= 0")

        for key in ("title", "description", "startDate", "endDate"):
            value = str(data.get(key) or "").strip()
            if value:
                clean[key] = value

        milestones = []
        raw = data.get("milestones")
        if raw is not None:
            if not isinstance(raw, list):
                raise SlotError("InvalidBonus", "milestones must be a list")
            for entry in raw:
                if not isinstance(entry, dict):
                    continue
                try:
                    stops = int(float(entry.get("stops")))
                    amount = float(entry.get("amount"))
                except (TypeError, ValueError):
                    raise SlotError("InvalidBonus", "each milestone needs numeric stops and amount")
                if stops <= 0 or amount < 0:
                    raise SlotError("InvalidBonus", "milestone stops must be > 0 and amount >= 0")
                milestones.append({"stops": stops, "amount": round(amount, 2)})
            milestones.sort(key=lambda m: m["stops"])
        clean["milestones"] = milestones

        if clean["enabled"] and (not clean.get("startDate") or not clean.get("endDate")):
            raise SlotError("InvalidBonus", "startDate and endDate are required when the campaign is enabled")

        RiderSlotsService._write_config_key("riderBonusConfig", clean)
        return RiderSlotsService.get_bonus_config()

    @staticmethod
    def _write_slot(slot: dict) -> None:
        """Upsert a slot as its own item (PK=RIDER_SLOT#<id>, SK=DEF) + GSI keys."""
        dynamodb_client.put_item(
            TableName=TABLES["CONFIG"],
            Item={
                "partitionkey": {"S": _counter_pk(slot["slotId"])},
                "sortKey": {"S": SLOT_DEF_SK},
                "slotBucket": {"S": SLOT_GSI_BUCKET},
                "slotDateSk": {"S": _slot_date_sk(slot["date"], slot["slotId"])},
                "slot": python_to_dynamodb(slot),
            },
        )

    @staticmethod
    def get_slot(slot_id: str) -> Optional[dict]:
        """O(1) fetch by id (GetItem on the slot's own item)."""
        resp = dynamodb_client.get_item(
            TableName=TABLES["CONFIG"],
            Key={"partitionkey": {"S": _counter_pk(slot_id)}, "sortKey": {"S": SLOT_DEF_SK}},
        )
        item = resp.get("Item")
        if not item or "slot" not in item:
            return None
        slot = dynamodb_to_python(item["slot"])
        return slot if isinstance(slot, dict) else None

    @staticmethod
    def _query_slot_gsi(key_cond: str, values: dict) -> List[dict]:
        out: List[dict] = []
        last_key = None
        while True:
            kwargs = {
                "TableName": TABLES["CONFIG"],
                "IndexName": SLOT_GSI,
                "KeyConditionExpression": key_cond,
                "ExpressionAttributeValues": values,
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = dynamodb_client.query(**kwargs)
            for item in resp.get("Items", []):
                slot = dynamodb_to_python(item.get("slot", {"M": {}}))
                if isinstance(slot, dict) and slot.get("slotId"):
                    out.append(slot)
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
        out.sort(key=lambda s: (s.get("date") or "", s.get("startTime") or ""))
        return out

    @staticmethod
    def list_slots_in_range(from_date: str, to_date: str) -> List[dict]:
        """All slots dated in [from_date, to_date] (inclusive) — one GSI query."""
        return RiderSlotsService._query_slot_gsi(
            "slotBucket = :b AND slotDateSk BETWEEN :from AND :to",
            {
                ":b": {"S": SLOT_GSI_BUCKET},
                ":from": {"S": f"{from_date}#"},
                ":to": {"S": f"{to_date}#￿"},
            },
        )

    @staticmethod
    def list_all_slots() -> List[dict]:
        """Every slot (admin view)."""
        return RiderSlotsService._query_slot_gsi(
            "slotBucket = :b", {":b": {"S": SLOT_GSI_BUCKET}}
        )

    # ------------------------------------------------------------------ admin CRUD
    @staticmethod
    def _validate_slot_input(data: dict) -> None:
        for field in ("date", "startTime", "endTime"):
            if not data.get(field):
                raise SlotError("InvalidSlot", f"{field} is required")
        if _duration_minutes(data["startTime"], data["endTime"]) <= 0:
            raise SlotError("InvalidSlot", "endTime must be after startTime")
        try:
            if float(data.get("price", 0)) < 0:
                raise SlotError("InvalidSlot", "price must be >= 0")
            if int(data.get("totalSeats", 0)) <= 0:
                raise SlotError("InvalidSlot", "totalSeats must be > 0")
        except (TypeError, ValueError):
            raise SlotError("InvalidSlot", "price/totalSeats must be numeric")

    @staticmethod
    def create_slot(data: dict) -> dict:
        RiderSlotsService._validate_slot_input(data)
        now = now_ist_iso()
        slot = {
            "slotId": generate_id("SLOT"),
            "label": data.get("label") or "",
            "date": data["date"],
            "startTime": data["startTime"],
            "endTime": data["endTime"],
            "durationMinutes": _duration_minutes(data["startTime"], data["endTime"]),
            "price": float(data.get("price", 0)),
            "totalSeats": int(data["totalSeats"]),
            "released": bool(data.get("released", False)),
            "releaseAt": data.get("releaseAt"),
            "createdAt": now,
            "updatedAt": now,
        }
        RiderSlotsService._write_slot(slot)
        if slot["released"]:
            RiderSlotsService._ensure_counter(slot)
            RiderSlotsService._schedule_compliance(slot)
        return slot

    @staticmethod
    def update_slot(slot_id: str, data: dict) -> dict:
        slot = RiderSlotsService.get_slot(slot_id)
        if not slot:
            raise SlotError("SlotNotFound", "Slot not found", 404)

        for field in ("label", "date", "startTime", "endTime", "releaseAt"):
            if field in data and data[field] is not None:
                slot[field] = data[field]
        if "price" in data and data["price"] is not None:
            slot["price"] = float(data["price"])
        if "totalSeats" in data and data["totalSeats"] is not None:
            new_seats = int(data["totalSeats"])
            booked = RiderSlotsService._booked_count(slot_id)
            if new_seats < booked:
                raise SlotError(
                    "SeatsBelowBooked",
                    f"totalSeats ({new_seats}) cannot be below already-booked ({booked})",
                )
            slot["totalSeats"] = new_seats
        slot["durationMinutes"] = _duration_minutes(slot["startTime"], slot["endTime"])
        slot["updatedAt"] = now_ist_iso()

        RiderSlotsService._write_slot(slot)
        if slot.get("released"):
            RiderSlotsService._ensure_counter(slot)
            # End time / date may have changed — (re)schedule the compliance check.
            RiderSlotsService._schedule_compliance(slot, reschedule=True)
        return slot

    @staticmethod
    def delete_slot(slot_id: str) -> None:
        slot = RiderSlotsService.get_slot(slot_id)
        if not slot:
            raise SlotError("SlotNotFound", "Slot not found", 404)

        # Cancel existing bookings (best-effort) before dropping the definition.
        for rider_id in RiderSlotsService._counter_rider_ids(slot_id):
            try:
                dynamodb_client.delete_item(
                    TableName=TABLES["CONFIG"],
                    Key={
                        "partitionkey": {"S": _bookings_pk(rider_id)},
                        "sortKey": {"S": _booking_sk(slot["date"], slot_id)},
                    },
                )
            except ClientError:
                logger.warning(f"[slotId={slot_id}] failed to delete booking for {rider_id}")

        # Drop the live counter + the pending compliance schedule.
        try:
            dynamodb_client.delete_item(
                TableName=TABLES["CONFIG"],
                Key={"partitionkey": {"S": _counter_pk(slot_id)}, "sortKey": {"S": "BOOKINGS"}},
            )
        except ClientError:
            pass
        RiderSlotsService._delete_schedule(slot_id)

        # Drop the slot definition item itself.
        try:
            dynamodb_client.delete_item(
                TableName=TABLES["CONFIG"],
                Key={"partitionkey": {"S": _counter_pk(slot_id)}, "sortKey": {"S": SLOT_DEF_SK}},
            )
        except ClientError:
            pass

    @staticmethod
    def release_slot(slot_id: str, release_at: Optional[str] = None) -> dict:
        slot = RiderSlotsService.get_slot(slot_id)
        if not slot:
            raise SlotError("SlotNotFound", "Slot not found", 404)
        slot["released"] = True
        slot["releaseAt"] = release_at or now_ist_iso()
        slot["updatedAt"] = now_ist_iso()
        RiderSlotsService._write_slot(slot)
        RiderSlotsService._ensure_counter(slot)
        RiderSlotsService._schedule_compliance(slot, reschedule=True)
        return slot

    # ------------------------------------------------------------------ live counter
    @staticmethod
    def _ensure_counter(slot: dict) -> None:
        """Create/refresh the per-slot seat counter without resetting bookedCount."""
        try:
            dynamodb_client.update_item(
                TableName=TABLES["CONFIG"],
                Key={"partitionkey": {"S": _counter_pk(slot["slotId"])}, "sortKey": {"S": "BOOKINGS"}},
                # `capacity` is a DynamoDB reserved word — must be aliased.
                UpdateExpression="SET #capacity = :cap, bookedCount = if_not_exists(bookedCount, :zero)",
                ExpressionAttributeNames={"#capacity": "capacity"},
                ExpressionAttributeValues={
                    ":cap": {"N": str(int(slot["totalSeats"]))},
                    ":zero": {"N": "0"},
                },
            )
        except ClientError as e:
            logger.error(f"[slotId={slot.get('slotId')}] _ensure_counter failed: {e}")

    @staticmethod
    def _get_counter(slot_id: str) -> Optional[dict]:
        resp = dynamodb_client.get_item(
            TableName=TABLES["CONFIG"],
            Key={"partitionkey": {"S": _counter_pk(slot_id)}, "sortKey": {"S": "BOOKINGS"}},
        )
        return resp.get("Item")

    @staticmethod
    def _booked_count(slot_id: str) -> int:
        item = RiderSlotsService._get_counter(slot_id)
        if not item:
            return 0
        return int(item.get("bookedCount", {}).get("N", "0"))

    @staticmethod
    def _counter_rider_ids(slot_id: str) -> List[str]:
        item = RiderSlotsService._get_counter(slot_id)
        if not item:
            return []
        return list(item.get("riderIds", {}).get("SS", []))

    @staticmethod
    def _opens_at(slot: dict, settings: dict) -> datetime:
        """When a slot becomes bookable. Explicit per-slot releaseAt overrides the
        ``releaseDaysInAdvance`` window."""
        explicit = _parse_iso(slot.get("releaseAt"))
        if explicit:
            return explicit
        days = int(settings.get("releaseDaysInAdvance", 1))
        return _slot_dt(slot["date"], "00:00") - timedelta(days=days)

    # ------------------------------------------------------------------ rider: list available
    @staticmethod
    def list_available_for_rider(
        rider_id: str, from_date: Optional[str] = None, to_date: Optional[str] = None
    ) -> List[dict]:
        """All released slots in [from_date, to_date] (default T-7..T+7), each annotated
        with ``bookable`` + ``blockReason`` (OPENS_LATER / FULL / CLOSED) so the UI can
        render open / blocked / past slots. Fetched in one GSI range query."""
        now = _now()
        today = now.date()
        if not from_date:
            from_date = (today - timedelta(days=SLOT_RANGE_PAST_DAYS)).isoformat()
        if not to_date:
            to_date = (today + timedelta(days=SLOT_RANGE_FUTURE_DAYS)).isoformat()

        settings = RiderSlotsService.get_settings()
        out: List[dict] = []
        for slot in RiderSlotsService.list_slots_in_range(from_date, to_date):
            if not slot.get("released"):
                continue  # admin draft — not published to riders

            start_dt = _slot_dt(slot["date"], slot["startTime"])
            opens_at = RiderSlotsService._opens_at(slot, settings)

            counter = RiderSlotsService._get_counter(slot["slotId"])
            booked = int(counter.get("bookedCount", {}).get("N", "0")) if counter else 0
            rider_ids = list(counter.get("riderIds", {}).get("SS", [])) if counter else []
            total = int(slot.get("totalSeats", 0))
            available = max(0, total - booked)
            booked_by_me = rider_id in rider_ids

            if now >= start_dt:
                bookable, block_reason = False, "CLOSED"   # past
            elif booked_by_me:
                bookable, block_reason = False, None
            elif available <= 0:
                bookable, block_reason = False, "FULL"
            elif now < opens_at:
                bookable, block_reason = False, "OPENS_LATER"
            else:
                bookable, block_reason = True, None

            out.append({
                **_public_slot(slot),
                "availableSeats": available,
                "bookedSeats": booked,
                "bookedByMe": booked_by_me,
                "status": "FULL" if available <= 0 else "AVAILABLE",
                "bookable": bookable,
                "blockReason": block_reason,
                "opensAt": opens_at.isoformat(),
            })
        out.sort(key=lambda s: (s["date"], s["startTime"]))
        return out

    # ------------------------------------------------------------------ rider: book
    @staticmethod
    def book_slot(rider_id: str, slot_id: str) -> dict:
        slot = RiderSlotsService.get_slot(slot_id)
        if not slot:
            raise SlotError("SlotNotFound", "Slot not found", 404)
        if not slot.get("released"):
            raise SlotError("SlotNotReleased", "This slot is not open for booking yet")

        now = _now()
        start_dt = _slot_dt(slot["date"], slot["startTime"])
        end_dt = _slot_dt(slot["date"], slot["endTime"])
        if now >= start_dt:
            raise SlotError("SlotClosed", "Booking for this slot has closed")
        opens_at = RiderSlotsService._opens_at(slot, RiderSlotsService.get_settings())
        if now < opens_at:
            raise SlotError(
                "SlotNotOpen",
                f"This slot opens on {opens_at.strftime('%d %b, %I:%M %p')}",
            )

        # Ban gate
        profile = RiderSlotsService.get_rider_slots_profile(rider_id)
        ban_until = _parse_iso(profile.get("bookingBanUntil"))
        if ban_until and now < ban_until:
            raise SlotError(
                "BookingBanned",
                f"Booking paused until {ban_until.date().isoformat()} due to repeated no-shows",
                403,
            )

        # Overlap with the rider's other (non-missed) bookings
        for b in RiderSlotsService.list_rider_bookings(rider_id):
            if b.get("status") in ("MISSED",) or b.get("slotId") == slot_id:
                continue
            b_start = _parse_iso(b.get("startAt"))
            b_end = _parse_iso(b.get("endAt"))
            if b_start and b_end and _overlaps(start_dt, end_dt, b_start, b_end) > 0:
                raise SlotError("SlotOverlap", "You already have a slot booked in this window")

        # Lazily ensure the seat counter exists before the conditional booking
        # transaction. Covers slots released before the counter existed (or whose
        # config was hand-edited). Idempotent: syncs capacity, preserves bookedCount.
        RiderSlotsService._ensure_counter(slot)

        booking_item = {
            "partitionkey": {"S": _bookings_pk(rider_id)},
            "sortKey": {"S": _booking_sk(slot["date"], slot_id)},
            "riderId": {"S": rider_id},
            "slotId": {"S": slot_id},
            "date": {"S": slot["date"]},
            "label": {"S": slot.get("label") or ""},
            "startTime": {"S": slot["startTime"]},
            "endTime": {"S": slot["endTime"]},
            "startAt": {"S": start_dt.isoformat()},
            "endAt": {"S": end_dt.isoformat()},
            "price": {"N": str(float(slot.get("price", 0)))},
            "status": {"S": "BOOKED"},
            "onlineSecondsInSlot": {"N": "0"},
            "offersReceived": {"N": "0"},
            "offersAccepted": {"N": "0"},
            "offersRejected": {"N": "0"},
            "settled": {"BOOL": False},
            "bookedAt": {"S": now_ist_iso()},
        }

        try:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": TABLES["CONFIG"],
                            "Key": {
                                "partitionkey": {"S": _counter_pk(slot_id)},
                                "sortKey": {"S": "BOOKINGS"},
                            },
                            # `capacity` is a DynamoDB reserved word — must be aliased.
                            "UpdateExpression": "ADD bookedCount :one, riderIds :rset",
                            "ConditionExpression": (
                                "attribute_exists(#capacity) AND bookedCount < #capacity "
                                "AND (attribute_not_exists(riderIds) OR NOT contains(riderIds, :rid))"
                            ),
                            "ExpressionAttributeNames": {"#capacity": "capacity"},
                            "ExpressionAttributeValues": {
                                ":one": {"N": "1"},
                                ":rset": {"SS": [rider_id]},
                                ":rid": {"S": rider_id},
                            },
                        }
                    },
                    {
                        "Put": {
                            "TableName": TABLES["CONFIG"],
                            "Item": booking_item,
                            "ConditionExpression": "attribute_not_exists(sortKey)",
                        }
                    },
                ]
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "TransactionCanceledException":
                # Either the slot is full or the rider already booked it.
                if rider_id in RiderSlotsService._counter_rider_ids(slot_id):
                    raise SlotError("AlreadyBooked", "You have already booked this slot")
                raise SlotError("SlotFull", "This slot is full")
            raise SlotError("BookFailed", f"Failed to book slot: {e}", 500)

        return _booking_to_dict(booking_item)

    @staticmethod
    def cancel_booking(rider_id: str, slot_id: str) -> None:
        slot = RiderSlotsService.get_slot(slot_id)
        if not slot:
            raise SlotError("SlotNotFound", "Slot not found", 404)
        # Cancellation is allowed only up to `cancelCutoffHours` before the slot starts.
        cutoff_hours = float(RiderSlotsService.get_settings().get("cancelCutoffHours", 0))
        start_dt = _slot_dt(slot["date"], slot["startTime"])
        cancel_by = start_dt - timedelta(hours=cutoff_hours)
        if _now() > cancel_by:
            raise SlotError(
                "CancelWindowClosed",
                f"Cancellations must be made at least {cutoff_hours:g} hour(s) before the slot — "
                f"by {cancel_by.strftime('%d %b, %I:%M %p')}",
            )
        try:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": TABLES["CONFIG"],
                            "Key": {"partitionkey": {"S": _counter_pk(slot_id)}, "sortKey": {"S": "BOOKINGS"}},
                            "UpdateExpression": "ADD bookedCount :neg DELETE riderIds :rset",
                            "ConditionExpression": "contains(riderIds, :rid)",
                            "ExpressionAttributeValues": {
                                ":neg": {"N": "-1"},
                                ":rset": {"SS": [rider_id]},
                                ":rid": {"S": rider_id},
                            },
                        }
                    },
                    {
                        "Delete": {
                            "TableName": TABLES["CONFIG"],
                            "Key": {
                                "partitionkey": {"S": _bookings_pk(rider_id)},
                                "sortKey": {"S": _booking_sk(slot["date"], slot_id)},
                            },
                        }
                    },
                ]
            )
        except ClientError as e:
            raise SlotError("CancelFailed", f"Failed to cancel: {e}", 500)

    # ------------------------------------------------------------------ rider: my bookings + profile
    @staticmethod
    def list_rider_bookings(rider_id: str) -> List[dict]:
        out: List[dict] = []
        last_key = None
        while True:
            kwargs = {
                "TableName": TABLES["CONFIG"],
                "KeyConditionExpression": "partitionkey = :pk AND begins_with(sortKey, :pfx)",
                "ExpressionAttributeValues": {
                    ":pk": {"S": _bookings_pk(rider_id)},
                    ":pfx": {"S": "20"},  # date-prefixed booking rows (YYYY-...), excludes PROFILE
                },
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = dynamodb_client.query(**kwargs)
            for item in resp.get("Items", []):
                out.append(_booking_to_dict(item))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
        out.sort(key=lambda b: b.get("startAt") or "", reverse=True)

        # Annotate cancellability so the app can show/hide the Cancel action.
        cutoff_hours = float(RiderSlotsService.get_settings().get("cancelCutoffHours", 0))
        now = _now()
        for b in out:
            start = _parse_iso(b.get("startAt"))
            if b.get("status") == "BOOKED" and start:
                cancel_by = start - timedelta(hours=cutoff_hours)
                b["cancellable"] = now <= cancel_by
                b["cancelByAt"] = cancel_by.isoformat()
            else:
                b["cancellable"] = False
                b["cancelByAt"] = None
        return out

    @staticmethod
    def get_rider_slots_profile(rider_id: str) -> dict:
        resp = dynamodb_client.get_item(
            TableName=TABLES["CONFIG"],
            Key={"partitionkey": {"S": _bookings_pk(rider_id)}, "sortKey": {"S": "PROFILE"}},
        )
        item = resp.get("Item")
        if not item:
            return {"noShowEvents": [], "bookingBanUntil": None}
        return {
            "noShowEvents": list(item.get("noShowEvents", {}).get("SS", [])),
            "bookingBanUntil": item.get("bookingBanUntil", {}).get("S") if "bookingBanUntil" in item else None,
        }

    # ------------------------------------------------------------------ offer counters
    @staticmethod
    def bump_offer_counter(rider_id: str, kind: str) -> None:
        """Best-effort: bump offer/accept/reject on the rider's booking active right now.

        Must NEVER raise into the order flow.
        """
        field = {
            "offer": "offersReceived",
            "accept": "offersAccepted",
            "reject": "offersRejected",
        }.get(kind)
        if not field or not rider_id:
            return
        try:
            now = _now()
            for b in RiderSlotsService.list_rider_bookings(rider_id):
                if b.get("status") != "BOOKED":
                    continue
                start = _parse_iso(b.get("startAt"))
                end = _parse_iso(b.get("endAt"))
                if start and end and start <= now <= end:
                    dynamodb_client.update_item(
                        TableName=TABLES["CONFIG"],
                        Key={
                            "partitionkey": {"S": _bookings_pk(rider_id)},
                            "sortKey": {"S": _booking_sk(b["date"], b["slotId"])},
                        },
                        UpdateExpression=f"ADD {field} :one",
                        ExpressionAttributeValues={":one": {"N": "1"}},
                    )
                    return
        except Exception as e:  # best-effort
            logger.warning(f"[riderId={rider_id}] bump_offer_counter({kind}) failed: {e}")

    # ------------------------------------------------------------------ compliance: coverage
    @staticmethod
    def compute_slot_coverage(rider_id: str, slot: dict, settings: dict) -> Tuple[float, float]:
        """Return (online_seconds_in_window, coverage_pct) for the rider's slot window."""
        start_dt = _slot_dt(slot["date"], slot["startTime"])
        end_dt = _slot_dt(slot["date"], slot["endTime"])
        window = (end_dt - start_dt).total_seconds()
        if window <= 0:
            return 0.0, 0.0
        stale = int(settings.get("staleSeconds", 90))

        intervals: List[Tuple[datetime, datetime]] = []

        # Closed sessions logged for the slot's date.
        resp = dynamodb_client.get_item(
            TableName=TABLES["CONFIG"],
            Key={"partitionkey": {"S": _sessions_pk(rider_id)}, "sortKey": {"S": slot["date"]}},
        )
        item = resp.get("Item")
        if item:
            sessions = dynamodb_to_python(item.get("sessions", {"L": []})) or []
            for s in sessions:
                lo = _parse_iso(s.get("loginAt"))
                hi = _parse_iso(s.get("logoutAt"))
                if lo and hi:
                    intervals.append((lo, hi))

        # Open session (rider still online) — bound by lastSeen + stale window.
        rresp = dynamodb_client.get_item(
            TableName=TABLES["RIDERS"], Key={"riderId": {"S": rider_id}}
        )
        ritem = rresp.get("Item")
        if ritem and "currentSessionStart" in ritem:
            lo = _parse_iso(ritem.get("currentSessionStart", {}).get("S"))
            last_seen = _parse_iso(ritem.get("lastSeen", {}).get("S"))
            if lo:
                hi = end_dt
                if last_seen:
                    hi = min(end_dt, last_seen + timedelta(seconds=stale))
                if hi > lo:
                    intervals.append((lo, hi))

        coverage = sum(_overlaps(start_dt, end_dt, s, e) for s, e in intervals)
        coverage = min(coverage, window)
        return coverage, coverage / window

    @staticmethod
    def _slot_delivery_earnings(rider_id: str, slot: dict) -> float:
        """Sum the rider's delivery payout for orders ACCEPTED during the slot window.

        Used for the guarantee top-up — the guarantee is a floor on slot earnings,
        not a bonus on top. Attribution = order accepted (riderAssignedAt) within
        [slotStart, slotEnd]; cancelled orders are excluded.
        """
        from services.order_service import OrderService
        from models.order import Order

        start_dt = _slot_dt(slot["date"], slot["startTime"])
        end_dt = _slot_dt(slot["date"], slot["endTime"])
        try:
            orders = OrderService.get_orders_by_rider(rider_id, limit=100)
        except Exception as e:
            logger.warning(f"[riderId={rider_id}] slot earnings lookup failed: {e}")
            return 0.0

        total = 0.0
        for o in orders:
            if (getattr(o, "status", "") or "") == Order.STATUS_CANCELLED:
                continue
            accepted = _parse_iso(getattr(o, "rider_assigned_at", None))
            if accepted and start_dt <= accepted <= end_dt:
                total += _order_rider_payout(o)
        return round(total, 2)

    # ------------------------------------------------------------------ compliance: no-show ban
    @staticmethod
    def record_no_show(rider_id: str, settings: dict) -> None:
        now = _now()
        profile = RiderSlotsService.get_rider_slots_profile(rider_id)
        window_days = int(settings.get("noShowBanWindowDays", 14))
        cutoff = now - timedelta(days=window_days)
        events = [e for e in profile.get("noShowEvents", []) if (_parse_iso(e) or now) >= cutoff]
        events.append(now.isoformat())

        update_expr = "SET noShowEvents = :ev"
        values = {":ev": {"SS": events}}
        if len(events) >= int(settings.get("noShowBanThreshold", 2)):
            ban_until = now + timedelta(days=int(settings.get("noShowBanDurationDays", 7)))
            update_expr += ", bookingBanUntil = :ban"
            values[":ban"] = {"S": ban_until.isoformat()}

        dynamodb_client.update_item(
            TableName=TABLES["CONFIG"],
            Key={"partitionkey": {"S": _bookings_pk(rider_id)}, "sortKey": {"S": "PROFILE"}},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=values,
        )

    # ------------------------------------------------------------------ compliance: evaluate (slot end)
    @staticmethod
    def _booking_key(rider_id: str, date: str, slot_id: str) -> dict:
        return {
            "partitionkey": {"S": _bookings_pk(rider_id)},
            "sortKey": {"S": _booking_sk(date, slot_id)},
        }

    @staticmethod
    def evaluate_slot_compliance(slot_id: str) -> dict:
        """At slot end: evaluate presence/responsiveness for every booking and stamp
        the verdict on the booking. Does NOT credit the guarantee (deferred to the EOD
        batch — see ``settle_slot_guarantee``) and applies NO monetary penalty. Records
        a no-show (booking-ban tracking) for NO_SHOW riders. Idempotent: a booking that
        is already settled or already compliance-evaluated is skipped.
        """
        slot = RiderSlotsService.get_slot(slot_id)
        if not slot:
            logger.warning(f"[slotId={slot_id}] compliance: slot not found")
            return {"slotId": slot_id, "evaluated": 0, "skipped": 0, "reason": "slot_not_found"}

        settings = RiderSlotsService.get_settings()
        threshold = float(settings.get("complianceThresholdPct", 0.8))
        max_rejects = int(settings.get("maxRejectionsAllowed", 1))

        results = {"slotId": slot_id, "eligible": 0, "ineligible": 0, "skipped": 0, "details": []}
        for rider_id in RiderSlotsService._counter_rider_ids(slot_id):
            resp = dynamodb_client.get_item(
                TableName=TABLES["CONFIG"],
                Key=RiderSlotsService._booking_key(rider_id, slot["date"], slot_id),
            )
            item = resp.get("Item")
            if not item:
                results["skipped"] += 1
                continue
            if item.get("settled", {}).get("BOOL", False) or "complianceEvaluatedAt" in item:
                results["skipped"] += 1
                continue

            offers_received = int(item.get("offersReceived", {}).get("N", "0"))
            offers_rejected = int(item.get("offersRejected", {}).get("N", "0"))
            coverage_secs, coverage_pct = RiderSlotsService.compute_slot_coverage(rider_id, slot, settings)

            presence_ok = coverage_pct >= threshold
            responsiveness_ok = (offers_received == 0) or (offers_rejected <= max_rejects)
            compliant = presence_ok and responsiveness_ok

            if compliant:
                status, reason = "COMPLETED", None
                results["eligible"] += 1
            else:
                if coverage_secs <= 0:
                    reason = "NO_SHOW"
                elif not presence_ok:
                    reason = "SHORT_PRESENCE"
                else:
                    reason = "UNRESPONSIVE"
                status = "MISSED"
                results["ineligible"] += 1
                if reason == "NO_SHOW":
                    RiderSlotsService.record_no_show(rider_id, settings)

            # Stamp the compliance verdict; the guarantee credit happens at EOD.
            set_expr = (
                "SET #s = :status, guaranteeEligible = :elig, complianceEvaluatedAt = :now, "
                "onlineSecondsInSlot = :secs, coveragePct = :pct"
            )
            values = {
                ":status": {"S": status},
                ":elig": {"BOOL": compliant},
                ":now": {"S": now_ist_iso()},
                ":secs": {"N": str(int(coverage_secs))},
                ":pct": {"N": str(round(coverage_pct, 4))},
            }
            if reason:
                set_expr += ", missReason = :reason"
                values[":reason"] = {"S": reason}
            try:
                dynamodb_client.update_item(
                    TableName=TABLES["CONFIG"],
                    Key=RiderSlotsService._booking_key(rider_id, slot["date"], slot_id),
                    UpdateExpression=set_expr,
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues=values,
                    ConditionExpression="attribute_not_exists(complianceEvaluatedAt)",
                )
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                    raise
            results["details"].append({"riderId": rider_id, "status": status, "reason": reason})

        logger.info(
            f"[slotId={slot_id}] compliance evaluated: {results['eligible']} eligible, {results['ineligible']} ineligible"
        )
        return results

    # ------------------------------------------------------------------ settlement (EOD)
    @staticmethod
    def settle_slot_guarantee(slot_id: str) -> dict:
        """EOD: credit the guarantee top-up for every compliant booking on a slot, once
        all orders accepted during the window have been delivered (so payouts are final).

        Top-up = max(0, guarantee - slot delivery earnings); the guarantee is a FLOOR,
        so a rider who earned >= the guarantee gets nothing extra. Non-compliant bookings
        forfeit the guarantee with NO penalty. Idempotent per booking (settled guard +
        idempotent ``credit_slot_guarantee``).
        """
        from services.earnings_service import EarningsService

        slot = RiderSlotsService.get_slot(slot_id)
        if not slot:
            logger.warning(f"[slotId={slot_id}] settlement: slot not found")
            return {"slotId": slot_id, "credited": 0, "forfeited": 0, "skipped": 0, "reason": "slot_not_found"}

        guarantee = float(slot.get("price", 0) or 0)
        results = {"slotId": slot_id, "credited": 0, "forfeited": 0, "skipped": 0}
        for rider_id in RiderSlotsService._counter_rider_ids(slot_id):
            key = RiderSlotsService._booking_key(rider_id, slot["date"], slot_id)
            item = dynamodb_client.get_item(TableName=TABLES["CONFIG"], Key=key).get("Item")
            if not item:
                results["skipped"] += 1
                continue
            if item.get("settled", {}).get("BOOL", False):
                results["skipped"] += 1
                continue
            # Fallback: if the slot-end compliance check never ran, evaluate now.
            if "complianceEvaluatedAt" not in item:
                RiderSlotsService.evaluate_slot_compliance(slot_id)
                item = dynamodb_client.get_item(TableName=TABLES["CONFIG"], Key=key).get("Item") or item

            eligible = item.get("guaranteeEligible", {}).get("BOOL", False)
            # Compute the rider's slot earnings for EVERY settled booking (so the
            # app can show "you earned" even for forfeited slots); the guarantee
            # top-up is only credited when the booking is eligible.
            slot_delivery_earnings = RiderSlotsService._slot_delivery_earnings(rider_id, slot)
            if eligible:
                topup = round(max(0.0, guarantee - slot_delivery_earnings), 2)
                if topup > 0:
                    EarningsService.credit_slot_guarantee(rider_id, slot, topup)
                outcome_amount = topup
                results["credited"] += 1
            else:
                outcome_amount = 0.0  # forfeit, no penalty
                results["forfeited"] += 1

            set_expr = "SET settled = :true, settledAt = :now, outcomeAmount = :amt"
            values = {
                ":true": {"BOOL": True},
                ":now": {"S": now_ist_iso()},
                ":amt": {"N": str(round(outcome_amount, 2))},
                ":false": {"BOOL": False},
            }
            if slot_delivery_earnings is not None:
                set_expr += ", slotDeliveryEarnings = :earned"
                values[":earned"] = {"N": str(round(slot_delivery_earnings, 2))}
            try:
                dynamodb_client.update_item(
                    TableName=TABLES["CONFIG"],
                    Key=key,
                    UpdateExpression=set_expr,
                    ExpressionAttributeValues=values,
                    ConditionExpression="attribute_not_exists(settled) OR settled = :false",
                )
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                    raise
        logger.info(
            f"[slotId={slot_id}] EOD settled: {results['credited']} credited, {results['forfeited']} forfeited"
        )
        return results

    @staticmethod
    def settle_day(date: str) -> dict:
        """EOD orchestrator: settle the guarantee for every slot dated ``date`` (YYYY-MM-DD).

        Run after the day has fully ended (so all in-window orders are delivered).
        Idempotent — safe to re-run / backfill.
        """
        slots = RiderSlotsService.list_slots_in_range(date, date)
        summary = {"date": date, "slots": len(slots), "credited": 0, "forfeited": 0, "skipped": 0}
        for slot in slots:
            r = RiderSlotsService.settle_slot_guarantee(slot["slotId"])
            summary["credited"] += r.get("credited", 0)
            summary["forfeited"] += r.get("forfeited", 0)
            summary["skipped"] += r.get("skipped", 0)
        logger.info(f"[date={date}] EOD slot settlement: {summary}")
        return summary

    # ------------------------------------------------------------------ scheduling
    @staticmethod
    def _schedule_name(slot_id: str) -> str:
        return "slot-compliance-" + re.sub(r"[^a-zA-Z0-9_-]", "-", slot_id)[:48]

    @staticmethod
    def _schedule_compliance(slot: dict, reschedule: bool = False) -> None:
        handler_arn = (os.environ.get("SLOT_COMPLIANCE_HANDLER_ARN") or "").strip()
        role_arn = (os.environ.get("SLOT_COMPLIANCE_HANDLER_ROLE_ARN") or "").strip()
        if not handler_arn or not role_arn:
            logger.warning("SLOT_COMPLIANCE_HANDLER_ARN/_ROLE_ARN not set — skipping schedule")
            return
        end_dt = _slot_dt(slot["date"], slot["endTime"])
        run_at_utc = end_dt.astimezone(timezone.utc)
        name = RiderSlotsService._schedule_name(slot["slotId"])
        try:
            scheduler = boto3.client("scheduler")
            if reschedule:
                RiderSlotsService._delete_schedule(slot["slotId"], scheduler)
            scheduler.create_schedule(
                Name=name,
                ScheduleExpression=f"at({run_at_utc.strftime('%Y-%m-%dT%H:%M:%S')})",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={
                    "Arn": handler_arn,
                    "RoleArn": role_arn,
                    "Input": json.dumps({"slotId": slot["slotId"]}),
                },
                ActionAfterCompletion="DELETE",
            )
            logger.info(f"[slotId={slot['slotId']}] compliance scheduled at {run_at_utc.isoformat()}")
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConflictException":
                logger.info(f"[slotId={slot['slotId']}] compliance schedule already exists")
            else:
                logger.error(f"[slotId={slot['slotId']}] failed to schedule compliance: {e}")
        except Exception as e:
            logger.error(f"[slotId={slot['slotId']}] failed to schedule compliance: {e}")

    @staticmethod
    def _delete_schedule(slot_id: str, scheduler=None) -> None:
        try:
            scheduler = scheduler or boto3.client("scheduler")
            scheduler.delete_schedule(Name=RiderSlotsService._schedule_name(slot_id))
        except Exception:
            pass  # not found / already deleted


# ----------------------------------------------------------------------------- module helpers
def _parse_date(date_str: str):
    y, mo, d = (int(x) for x in date_str.split("-"))
    return datetime(y, mo, d, tzinfo=IST).date()


def _public_slot(slot: dict) -> dict:
    """Rider-facing projection of a slot definition (no internal/admin-only churn)."""
    return {
        "slotId": slot.get("slotId"),
        "label": slot.get("label") or "",
        "date": slot.get("date"),
        "startTime": slot.get("startTime"),
        "endTime": slot.get("endTime"),
        "durationMinutes": slot.get("durationMinutes"),
        "price": float(slot.get("price", 0)),
        "totalSeats": int(slot.get("totalSeats", 0)),
    }


def _booking_to_dict(item: dict) -> dict:
    """DynamoDB booking item -> rider-facing dict."""
    def s(k):
        return item.get(k, {}).get("S")

    def n(k, default=0):
        try:
            return float(item.get(k, {}).get("N", str(default)))
        except (TypeError, ValueError):
            return default

    return {
        "slotId": s("slotId"),
        "date": s("date"),
        "label": s("label") or "",
        "startTime": s("startTime"),
        "endTime": s("endTime"),
        "startAt": s("startAt"),
        "endAt": s("endAt"),
        "price": n("price"),
        "status": s("status") or "BOOKED",
        "missReason": s("missReason"),
        "onlineSecondsInSlot": int(n("onlineSecondsInSlot")),
        "coveragePct": n("coveragePct"),
        "offersReceived": int(n("offersReceived")),
        "offersAccepted": int(n("offersAccepted")),
        "offersRejected": int(n("offersRejected")),
        "settled": item.get("settled", {}).get("BOOL", False),
        "outcomeAmount": n("outcomeAmount") if "outcomeAmount" in item else None,
        "slotDeliveryEarnings": n("slotDeliveryEarnings") if "slotDeliveryEarnings" in item else None,
        "bookedAt": s("bookedAt"),
    }
