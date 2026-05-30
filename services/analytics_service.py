"""Analytics service — dashboard metrics for a date range.

Aggregates four DynamoDB tables in a single response using only Query calls
(no Scans). Strategy:

  PHASE 1 — parallel fan-out, no input dependencies
    • Orders:   loop over every order status, Query `status-createdAtIso-index`
                with KeyConditionExpression on createdAt BETWEEN start AND end.
    • Payments: loop over every payment method, Query
                `paymentMethod-createdAtIso-index` similarly.

  PHASE 2 — parallel fan-out, depends on order results
    • Restaurant earnings: collect unique restaurantIds from orders, Query the
                main table by restaurantId with date BETWEEN start AND end.
    • Rider earnings:      collect unique riderIds from orders, Query the main
                table by riderId similarly (captures delivery + COD rows).

Every Query is billed only for items it returns (Query reads physically
adjacent items in the GSI partition, unlike Scan which reads every item in
the table).
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from models.order import Order
from models.payment import Payment
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import dynamodb_to_python

logger = Logger()


# ---------------------------------------------------------------------------
# Status / method enumerations (the values we iterate to avoid Scans)
# ---------------------------------------------------------------------------
# Includes theatre-only FAILED_INVENTORY which Order.get_all_statuses() omits.
_ALL_ORDER_STATUSES: List[str] = list(dict.fromkeys(
    Order.get_all_statuses() + [Order.STATUS_FAILED_INVENTORY]
))

_ALL_PAYMENT_METHODS: List[str] = [
    Payment.METHOD_UPI,
    Payment.METHOD_CARD,
    Payment.METHOD_WALLET,
    Payment.METHOD_NETBANKING,
    Payment.METHOD_COD,
    Payment.METHOD_SODEXO,
]


# ---------------------------------------------------------------------------
# Date normalization
# ---------------------------------------------------------------------------
def _normalize_date(value: str, end_of_day: bool = False) -> str:
    """Accept `YYYY-MM-DD` or a full ISO timestamp and return an IST ISO bound."""
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Date value is required")
    if len(raw) == 10:
        suffix = "T23:59:59+05:30" if end_of_day else "T00:00:00+05:30"
        return f"{raw}{suffix}"
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    parsed = datetime.fromisoformat(raw)
    iso = parsed.isoformat()
    if len(iso) == 19:
        iso = f"{iso}+05:30"
    return iso


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
def _unmarshal(item: dict) -> dict:
    """Convert one DynamoDB item (top-level AttributeValue map) to a plain dict."""
    return {k: dynamodb_to_python(v) for k, v in item.items()}


def _query_all(params: dict) -> List[dict]:
    """Paginated Query. Returns plain-Python items."""
    items: List[dict] = []
    while True:
        response = dynamodb_client.query(**params)
        for raw in response.get("Items", []):
            items.append(_unmarshal(raw))
        if "LastEvaluatedKey" not in response:
            break
        params["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    return items


def _fetch_orders_by_status(status: str, start_iso: str, end_iso: str) -> List[dict]:
    """Query orders via status-createdAtIso-index for one status, date-bounded."""
    return _query_all({
        "TableName": TABLES["ORDERS"],
        "IndexName": "status-createdAtIso-index",
        # `status` is a reserved word in DynamoDB — alias it.
        "KeyConditionExpression": "#st = :s AND createdAt BETWEEN :start AND :end",
        "ExpressionAttributeNames": {"#st": "status"},
        "ExpressionAttributeValues": {
            ":s": {"S": status},
            ":start": {"S": start_iso},
            ":end": {"S": end_iso},
        },
    })


def _fetch_payments_by_method(method: str, start_iso: str, end_iso: str) -> List[dict]:
    """Query payments via paymentMethod-createdAtIso-index for one method, date-bounded."""
    return _query_all({
        "TableName": TABLES["PAYMENTS"],
        "IndexName": "paymentMethod-createdAtIso-index",
        "KeyConditionExpression": "paymentMethod = :m AND createdAtIso BETWEEN :start AND :end",
        "ExpressionAttributeValues": {
            ":m": {"S": method},
            ":start": {"S": start_iso},
            ":end": {"S": end_iso},
        },
    })


def _fetch_restaurant_earnings(restaurant_id: str, start_only: str, end_prefix: str) -> List[dict]:
    """Query restaurant earnings by restaurantId on the main table (PK=restaurantId, SK=date)."""
    return _query_all({
        "TableName": TABLES["RESTAURANT_EARNINGS"],
        "KeyConditionExpression": "restaurantId = :r AND #d BETWEEN :start AND :end",
        "ExpressionAttributeNames": {"#d": "date"},
        "ExpressionAttributeValues": {
            ":r": {"S": restaurant_id},
            ":start": {"S": start_only},
            ":end": {"S": end_prefix},
        },
    })


def _fetch_rider_earnings(rider_id: str, start_only: str, end_prefix: str) -> List[dict]:
    """Query rider earnings by riderId on the main table (PK=riderId, SK=date).
    Captures both delivery rows (YYYY-MM-DD#orderId) and COD rows (YYYY-MM-DD#COD#orderId).
    """
    return _query_all({
        "TableName": TABLES["EARNINGS"],
        "KeyConditionExpression": "riderId = :r AND #d BETWEEN :start AND :end",
        "ExpressionAttributeNames": {"#d": "date"},
        "ExpressionAttributeValues": {
            ":r": {"S": rider_id},
            ":start": {"S": start_only},
            ":end": {"S": end_prefix},
        },
    })


# ---------------------------------------------------------------------------
# Small numerics + bucket helpers
# ---------------------------------------------------------------------------
def _num(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _day_bucket(value) -> str:
    """Extract YYYY-MM-DD from an IST ISO string (or epoch ms int)."""
    if isinstance(value, int):
        from utils.datetime_ist import epoch_ms_to_ist_iso
        value = epoch_ms_to_ist_iso(value)
    s = str(value or "")
    return s[:10] if len(s) >= 10 else s


def _hour_bucket(value) -> int:
    if isinstance(value, int):
        from utils.datetime_ist import epoch_ms_to_ist_iso
        value = epoch_ms_to_ist_iso(value)
    s = str(value or "")
    if len(s) >= 13:
        try:
            return int(s[11:13])
        except ValueError:
            return 0
    return 0


def _r2(x: float) -> float:
    return round(x, 2)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def generate_dashboard_metrics(start_date: str, end_date: str) -> Dict[str, Any]:
    """Build a full dashboard payload for an inclusive date range.

    Args:
        start_date: 'YYYY-MM-DD' or full ISO timestamp
        end_date:   'YYYY-MM-DD' or full ISO timestamp

    Returns: dict with `range`, `summary`, and `charts` sections.
    """
    start_iso = _normalize_date(start_date, end_of_day=False)
    end_iso = _normalize_date(end_date, end_of_day=True)
    start_only = start_iso[:10]
    end_only = end_iso[:10]
    # Sort keys on the earnings tables look like "YYYY-MM-DD#orderId" (and
    # "YYYY-MM-DD#COD#orderId" on the rider table). ASCII '~' (0x7E) is greater
    # than every character that can appear in those suffixes, so an upper bound
    # of "<end>#~" captures every row on end_only without bleeding into the
    # following day (because '#' < '0'..'9' < '~' < '0' is irrelevant —
    # the next day "YYYY-MM-<dd+1>" sorts above "YYYY-MM-<dd>#~").
    end_prefix = f"{end_only}#~"

    start_day = datetime.fromisoformat(start_only).date()
    end_day = datetime.fromisoformat(end_only).date()
    days = max(1, (end_day - start_day).days + 1)

    logger.info(
        f"[analytics] {start_only}..{end_only} ({days}d) "
        f"phase1: {len(_ALL_ORDER_STATUSES)} status queries + "
        f"{len(_ALL_PAYMENT_METHODS)} payment-method queries"
    )

    # ---------- Phase 1: orders (per status) + payments (per method) ---------
    orders: List[dict] = []
    payments: List[dict] = []
    try:
        with ThreadPoolExecutor(max_workers=20) as ex:
            order_futures = [
                ex.submit(_fetch_orders_by_status, s, start_iso, end_iso)
                for s in _ALL_ORDER_STATUSES
            ]
            payment_futures = [
                ex.submit(_fetch_payments_by_method, m, start_iso, end_iso)
                for m in _ALL_PAYMENT_METHODS
            ]
            for f in as_completed(order_futures):
                orders.extend(f.result())
            for f in as_completed(payment_futures):
                payments.extend(f.result())
    except ClientError as e:
        raise Exception(f"Failed Phase 1 GSI queries: {e}")

    # De-dupe defensively (each orderId/paymentId only appears under one
    # status/method partition, but a status transition mid-query could in
    # principle expose an order under two partitions).
    orders = list({o.get("orderId"): o for o in orders if o.get("orderId")}.values())
    payments = list({p.get("paymentId"): p for p in payments if p.get("paymentId")}.values())

    # ---------- Phase 2: earnings, fanned out across active actors ----------
    restaurant_ids = sorted({
        o["restaurantId"] for o in orders if o.get("restaurantId")
    })
    rider_ids = sorted({
        o["riderId"] for o in orders if o.get("riderId")
    })

    logger.info(
        f"[analytics] phase2: {len(restaurant_ids)} restaurant queries + "
        f"{len(rider_ids)} rider queries"
    )

    rest_earn: List[dict] = []
    rider_earn: List[dict] = []
    try:
        with ThreadPoolExecutor(max_workers=20) as ex:
            rest_futures = [
                ex.submit(_fetch_restaurant_earnings, rid, start_only, end_prefix)
                for rid in restaurant_ids
            ]
            rider_futures = [
                ex.submit(_fetch_rider_earnings, rid, start_only, end_prefix)
                for rid in rider_ids
            ]
            for f in as_completed(rest_futures):
                rest_earn.extend(f.result())
            for f in as_completed(rider_futures):
                rider_earn.extend(f.result())
    except ClientError as e:
        raise Exception(f"Failed Phase 2 earnings queries: {e}")

    logger.info(
        f"[analytics] fetched orders={len(orders)} payments={len(payments)} "
        f"restEarn={len(rest_earn)} riderEarn={len(rider_earn)}"
    )

    return _build_metrics(
        orders, payments, rest_earn, rider_earn, start_day, end_day, days
    )


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
def _build_metrics(
    orders: List[dict],
    payments: List[dict],
    rest_earn: List[dict],
    rider_earn: List[dict],
    start_day,
    end_day,
    days: int,
) -> Dict[str, Any]:
    # Daily axis (so empty days still appear in charts)
    daily_keys: List[str] = []
    d = start_day
    while d <= end_day:
        daily_keys.append(d.isoformat())
        d += timedelta(days=1)

    orders_by_day = {
        dk: {"date": dk, "orders": 0, "delivered": 0, "cancelled": 0, "gmv": 0.0}
        for dk in daily_keys
    }
    revenue_by_day = {
        dk: {"date": dk, "platform": 0.0, "restaurant": 0.0, "rider": 0.0, "govt": 0.0}
        for dk in daily_keys
    }

    status_breakdown = defaultdict(int)
    hourly_dist = [0] * 24
    customer_orders = defaultdict(int)
    restaurant_agg: Dict[str, dict] = {}
    coupon_agg: Dict[str, dict] = {}

    gmv = 0.0
    delivered = cancelled = 0
    coupons_used = 0
    coupon_discount_total = 0.0
    platform_final = restaurant_final_from_orders = rider_final_from_orders = govt_final = 0.0
    gst_on_food = gst_on_delivery = gst_on_platform = 0.0
    food_commission_total = platform_fee_total = rider_subsidy_total = 0.0
    excess_from_restaurant_rev = delivery_fee_discount_total = 0.0

    # ---- Orders ----------------------------------------------------------
    for o in orders:
        day = _day_bucket(o.get("createdAt"))
        if day not in orders_by_day:
            orders_by_day[day] = {"date": day, "orders": 0, "delivered": 0, "cancelled": 0, "gmv": 0.0}
            revenue_by_day[day] = {"date": day, "platform": 0.0, "restaurant": 0.0, "rider": 0.0, "govt": 0.0}

        status = o.get("status") or "UNKNOWN"
        status_breakdown[status] += 1
        orders_by_day[day]["orders"] += 1
        hourly_dist[_hour_bucket(o.get("createdAt"))] += 1

        phone = o.get("customerPhone")
        if phone:
            customer_orders[phone] += 1

        rest_id = o.get("restaurantId") or "UNKNOWN"
        ra = restaurant_agg.setdefault(
            rest_id,
            {
                "restaurantId": rest_id,
                "name": o.get("restaurantName") or rest_id,
                # Order volume
                "orders": 0,
                "delivered": 0,
                "cancelled": 0,
                # Money flow attributable to this restaurant
                "gmv": 0.0,
                "restaurantPayout": 0.0,       # rev.restaurantRevenue.finalPayout
                "platformEarnings": 0.0,       # rev.platformRevenue.finalPayout
                "platformFee": 0.0,
                "foodCommission": 0.0,
                "riderPayout": 0.0,            # rev.riderRevenue.finalPayout
                "govtGstTotal": 0.0,           # rev.govtRevenue.finalPayout
                "gstOnFood": 0.0,
                "gstOnDelivery": 0.0,
                "gstOnPlatform": 0.0,
                # COD share
                "codCount": 0,
                "codValue": 0.0,
                # Coupons applied on this restaurant's orders
                "couponsUsed": 0,
                "couponDiscount": 0.0,
                # Customers (derived after loop)
                "uniqueCustomers": 0,
                "repeatCustomers": 0,
                "_customerOrders": {},         # phone -> order count in window
                # Item-level (derived after loop)
                "itemsSold": 0,
                "_itemAgg": {},                # itemId -> {name, quantity, revenue}
                # Settlement ledger (from rest_earn rows)
                "settledEarnings": 0.0,
                "unsettledEarnings": 0.0,
                "earningsRows": 0,
            },
        )
        ra["orders"] += 1

        if phone:
            ra["_customerOrders"][phone] = ra["_customerOrders"].get(phone, 0) + 1

        order_gmv = _num(o.get("grandTotal"))
        gmv += order_gmv
        orders_by_day[day]["gmv"] += order_gmv
        ra["gmv"] += order_gmv

        if status == "DELIVERED":
            delivered += 1
            orders_by_day[day]["delivered"] += 1
            ra["delivered"] += 1
        if status == "CANCELLED":
            cancelled += 1
            orders_by_day[day]["cancelled"] += 1
            ra["cancelled"] += 1

        rev = o.get("revenue") or {}
        pr = rev.get("platformRevenue") or {}
        rr = rev.get("restaurantRevenue") or {}
        rdr = rev.get("riderRevenue") or {}
        gr = rev.get("govtRevenue") or {}

        # Pull each leaf once so we can roll up both globally and per-restaurant
        pr_final  = _num(pr.get("finalPayout"))
        rr_final  = _num(rr.get("finalPayout"))
        rdr_final = _num(rdr.get("finalPayout"))
        gr_final  = _num(gr.get("finalPayout"))
        pr_food_comm  = _num(pr.get("foodCommission"))
        pr_plat_fee   = _num(pr.get("platformFee"))
        pr_sub        = _num(pr.get("riderDeliverySubsidy"))
        pr_excess     = _num(pr.get("excessFromRestaurantRevenue"))
        pr_del_disc   = _num(pr.get("deliveryFeeDiscount"))
        gst_food_o    = _num(gr.get("gstOnFood"))
        gst_del_o     = _num(gr.get("gstOnDeliveryFee"))
        gst_plat_o    = _num(gr.get("gstOnPlatformFee"))

        platform_final += pr_final
        restaurant_final_from_orders += rr_final
        rider_final_from_orders += rdr_final
        govt_final += gr_final

        revenue_by_day[day]["platform"] += pr_final
        revenue_by_day[day]["restaurant"] += rr_final
        revenue_by_day[day]["rider"] += rdr_final
        revenue_by_day[day]["govt"] += gr_final

        ra["restaurantPayout"] += rr_final
        ra["platformEarnings"] += pr_final
        ra["platformFee"] += pr_plat_fee
        ra["foodCommission"] += pr_food_comm
        ra["riderPayout"] += rdr_final
        ra["govtGstTotal"] += gr_final
        ra["gstOnFood"] += gst_food_o
        ra["gstOnDelivery"] += gst_del_o
        ra["gstOnPlatform"] += gst_plat_o

        food_commission_total += pr_food_comm
        platform_fee_total += pr_plat_fee
        rider_subsidy_total += pr_sub
        excess_from_restaurant_rev += pr_excess
        delivery_fee_discount_total += pr_del_disc
        gst_on_food += gst_food_o
        gst_on_delivery += gst_del_o
        gst_on_platform += gst_plat_o

        # Per-restaurant COD attribution (orders.paymentMethod is the source
        # of truth — payments table is keyed by paymentId and would require
        # an O(orders) extra lookup).
        order_pm = (o.get("paymentMethod") or "").upper()
        if order_pm == "COD" and status != "CANCELLED":
            ra["codCount"] += 1
            ra["codValue"] += order_gmv

        if rev.get("couponApplied") and rev.get("couponCode"):
            coupons_used += 1
            disc = _num(rev.get("totalDiscount"))
            coupon_discount_total += disc
            ra["couponsUsed"] += 1
            ra["couponDiscount"] += disc
            code = rev["couponCode"]
            ca = coupon_agg.setdefault(
                code,
                {"code": code, "issuedBy": rev.get("couponIssuedBy"), "uses": 0, "discount": 0.0},
            )
            ca["uses"] += 1
            ca["discount"] += disc

        # Per-restaurant item rollup (quantity + revenue contribution).
        for it in (o.get("items") or []):
            iid = it.get("itemId") or it.get("name") or "UNKNOWN"
            qty = int(_num(it.get("quantity")))
            line_rev = _num(it.get("price")) * qty + _num(it.get("addOnTotal"))
            agg = ra["_itemAgg"].setdefault(
                iid,
                {"itemId": iid, "name": it.get("name") or iid, "quantity": 0, "revenue": 0.0},
            )
            agg["quantity"] += qty
            agg["revenue"] += line_rev
            ra["itemsSold"] += qty

    aov = _r2(gmv / len(orders)) if orders else 0.0

    # ---- Payments --------------------------------------------------------
    payment_methods: Dict[str, dict] = {}
    cod_count = 0
    cod_value = 0.0
    succ_payments = fail_payments = 0

    for p in payments:
        m = (p.get("paymentMethod") or "UNKNOWN").upper()
        amt = _num(p.get("amount"))
        pm = payment_methods.setdefault(m, {"method": m, "count": 0, "value": 0.0})
        pm["count"] += 1
        pm["value"] += amt

        if p.get("paymentStatus") == "SUCCESS":
            succ_payments += 1
        if p.get("paymentStatus") == "FAILED":
            fail_payments += 1

        is_cod = m == "COD" or p.get("paymentChannel") == "COD_AT_DELIVERY"
        if is_cod and p.get("paymentStatus") != "FAILED":
            cod_count += 1
            cod_value += amt

    payments_examined = sum(pm["count"] for pm in payment_methods.values())

    # ---- Customer cohort ------------------------------------------------
    unique_customers = len(customer_orders)
    repeat_customers = sum(1 for c in customer_orders.values() if c > 1)
    cohort_buckets = {"1 order": 0, "2-3": 0, "4-9": 0, "10+": 0}
    for cnt in customer_orders.values():
        if cnt == 1:
            cohort_buckets["1 order"] += 1
        elif cnt <= 3:
            cohort_buckets["2-3"] += 1
        elif cnt <= 9:
            cohort_buckets["4-9"] += 1
        else:
            cohort_buckets["10+"] += 1

    # ---- Restaurant earnings (settlement ledger) -------------------------
    rest_earn_total = rest_earn_settled = rest_earn_unsettled = 0.0
    for row in rest_earn:
        amt = _num(row.get("totalEarnings"))
        rest_earn_total += amt
        rid = row.get("restaurantId")
        ra_e = restaurant_agg.get(rid) if rid else None
        if ra_e is not None:
            ra_e["earningsRows"] += 1
        if row.get("settled"):
            rest_earn_settled += amt
            if ra_e is not None:
                ra_e["settledEarnings"] += amt
        else:
            rest_earn_unsettled += amt
            if ra_e is not None:
                ra_e["unsettledEarnings"] += amt

    # Finalize per-restaurant derived fields (round + top item + cleanup).
    restaurant_stats: List[dict] = []
    for ra_f in restaurant_agg.values():
        cust_orders = ra_f.pop("_customerOrders")
        ra_f["uniqueCustomers"] = len(cust_orders)
        ra_f["repeatCustomers"] = sum(1 for c in cust_orders.values() if c > 1)
        ra_f["repeatRatePct"] = (
            _r2(ra_f["repeatCustomers"] / ra_f["uniqueCustomers"] * 100)
            if ra_f["uniqueCustomers"] else 0.0
        )

        items = list(ra_f.pop("_itemAgg").values())
        items.sort(key=lambda x: (-x["quantity"], -x["revenue"]))
        top = items[0] if items else None
        ra_f["topItem"] = (
            {
                "itemId": top["itemId"],
                "name": top["name"],
                "quantity": top["quantity"],
                "revenue": _r2(top["revenue"]),
            }
            if top
            else None
        )
        ra_f["topItems"] = [
            {"itemId": i["itemId"], "name": i["name"], "quantity": i["quantity"], "revenue": _r2(i["revenue"])}
            for i in items[:5]
        ]
        ra_f["aov"] = _r2(ra_f["gmv"] / ra_f["orders"]) if ra_f["orders"] else 0.0
        ra_f["conversionPct"] = (
            _r2(ra_f["delivered"] / ra_f["orders"] * 100) if ra_f["orders"] else 0.0
        )
        for k in (
            "gmv", "restaurantPayout", "platformEarnings", "platformFee",
            "foodCommission", "riderPayout", "govtGstTotal", "gstOnFood",
            "gstOnDelivery", "gstOnPlatform", "codValue", "couponDiscount",
            "settledEarnings", "unsettledEarnings",
        ):
            ra_f[k] = _r2(ra_f[k])
        restaurant_stats.append(ra_f)
    restaurant_stats.sort(key=lambda x: -x["gmv"])

    # ---- Rider earnings --------------------------------------------------
    rider_agg: Dict[str, dict] = {}
    rider_earn_total = 0.0
    rider_deliveries_total = 0
    rider_cod_held = 0.0
    rider_tips = rider_incentives = 0.0

    for row in rider_earn:
        amt = _num(row.get("totalEarnings"))
        rider_earn_total += amt
        rider_deliveries_total += int(_num(row.get("totalDeliveries")))
        rider_tips += _num(row.get("tips"))
        rider_incentives += _num(row.get("incentives"))

        sk = row.get("date") or ""
        is_cod_row = "#COD#" in sk
        if is_cod_row:
            cash = (
                _num(row.get("cashCollected"))
                if row.get("cashCollected") is not None
                else abs(amt)
            )
            rider_cod_held += cash

        rid = row.get("riderId") or "UNKNOWN"
        r = rider_agg.setdefault(
            rid,
            {
                "riderId": rid,
                "deliveries": 0,
                "earnings": 0.0,
                "codHeld": 0.0,
                "tips": 0.0,
                "incentives": 0.0,
            },
        )
        r["earnings"] += amt
        r["deliveries"] += int(_num(row.get("totalDeliveries")))
        r["tips"] += _num(row.get("tips"))
        r["incentives"] += _num(row.get("incentives"))
        if is_cod_row:
            r["codHeld"] += (
                _num(row.get("cashCollected"))
                if row.get("cashCollected") is not None
                else abs(amt)
            )

    # ---- Pack response ---------------------------------------------------
    return {
        "range": {
            "startDate": start_day.isoformat(),
            "endDate": end_day.isoformat(),
            "days": days,
        },
        "summary": {
            "orders": {
                "total": len(orders),
                "delivered": delivered,
                "cancelled": cancelled,
                "conversionPct": _r2(delivered / len(orders) * 100) if orders else 0,
                "gmv": _r2(gmv),
                "aov": aov,
                "ordersPerDay": _r2(len(orders) / days),
            },
            "cod": {
                "count": cod_count,
                "value": _r2(cod_value),
                "sharePct": _r2(cod_count / payments_examined * 100) if payments_examined else 0,
            },
            "payments": {
                "totalAttempts": len(payments),
                "successful": succ_payments,
                "failed": fail_payments,
                "successRatePct": _r2(succ_payments / len(payments) * 100) if payments else 0,
            },
            "customers": {
                "unique": unique_customers,
                "repeatInWindow": repeat_customers,
                "repeatRatePct": _r2(repeat_customers / unique_customers * 100) if unique_customers else 0,
                "avgOrdersPerCustomer": _r2(len(orders) / unique_customers) if unique_customers else 0,
            },
            "coupons": {
                "ordersUsed": coupons_used,
                "uniqueCodes": len(coupon_agg),
                "discountValue": _r2(coupon_discount_total),
                "penetrationPct": _r2(coupons_used / len(orders) * 100) if orders else 0,
            },
            "platformRevenue": {
                "finalPayout": _r2(platform_final),
                "foodCommission": _r2(food_commission_total),
                "platformFee": _r2(platform_fee_total),
                "excessFromRestaurantRevenue": _r2(excess_from_restaurant_rev),
                "riderDeliverySubsidy": _r2(rider_subsidy_total),
                "deliveryFeeDiscount": _r2(delivery_fee_discount_total),
            },
            "restaurantRevenueFromOrders": {"finalPayout": _r2(restaurant_final_from_orders)},
            "riderRevenueFromOrders": {"finalPayout": _r2(rider_final_from_orders)},
            "govtRevenue": {
                "gstOnFood": _r2(gst_on_food),
                "gstOnDelivery": _r2(gst_on_delivery),
                "gstOnPlatform": _r2(gst_on_platform),
                "totalGst": _r2(gst_on_food + gst_on_delivery + gst_on_platform),
            },
            "restaurantEarningsTable": {
                "rows": len(rest_earn),
                "totalEarnings": _r2(rest_earn_total),
                "settled": _r2(rest_earn_settled),
                "unsettled": _r2(rest_earn_unsettled),
            },
            "riderEarningsTable": {
                "rows": len(rider_earn),
                "totalEarnings": _r2(rider_earn_total),
                "totalDeliveries": rider_deliveries_total,
                "codHeldByRiders": _r2(rider_cod_held),
                "tips": _r2(rider_tips),
                "incentives": _r2(rider_incentives),
            },
        },
        "charts": {
            "ordersByDay": [orders_by_day[dk] for dk in daily_keys],
            "revenueByDay": [revenue_by_day[dk] for dk in daily_keys],
            "statusBreakdown": sorted(
                [{"status": k, "count": v} for k, v in status_breakdown.items()],
                key=lambda x: -x["count"],
            ),
            "paymentMethodBreakdown": sorted(
                list(payment_methods.values()), key=lambda x: -x["count"]
            ),
            "customerCohort": [{"bucket": k, "customers": v} for k, v in cohort_buckets.items()],
            "restaurantStats": restaurant_stats,
            "topRestaurants": restaurant_stats[:10],
            "topRiders": sorted(rider_agg.values(), key=lambda x: -x["earnings"])[:10],
            "couponUsage": sorted(coupon_agg.values(), key=lambda x: -x["uses"]),
            "hourlyDistribution": [{"hour": h, "orders": c} for h, c in enumerate(hourly_dist)],
            "revenueComposition": [
                {"source": "Platform", "amount": _r2(platform_final)},
                {"source": "Restaurant payout", "amount": _r2(restaurant_final_from_orders)},
                {"source": "Rider payout", "amount": _r2(rider_final_from_orders)},
                {"source": "Govt (GST)", "amount": _r2(govt_final)},
            ],
        },
    }
