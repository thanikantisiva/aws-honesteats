"""Publish best-effort new-order alerts to a public ntfy.sh topic.

Used by the order DynamoDB stream → `restaurant_notification_handler` so
operators get an instant push on their phones / desktops the moment an
order transitions to `CONFIRMED`. Only fires in **production**
(`ENVIRONMENT == 'prod'`); a no-op in dev / local.

Equivalent of:

    curl -X POST "https://ntfy.sh/yumdudeneworders" \
         -H "Title: 🚨 NEW ORDER" \
         -H "Priority: max" \
         -H "Tags: rotating_light,shopping_cart" \
         -H "Click: https://www.yumdude.com/dashboard/orders" \
         -d "Order #ORD-123 • Paradise Biryani • ₹450"

The `Click` deep link opens the restaurant app's orders tab when the
notification is tapped: installed apps open it natively via Android App Links
/ iOS Universal Links, everyone else falls back to the web orders page.

Environment variables:
    ENVIRONMENT             — must be 'prod' for the call to actually fire
    NTFY_TOPIC_URL          — full topic URL (default: https://ntfy.sh/yumdudeneworders)
    NTFY_NEW_ORDER_ENABLED  — 'true'/'false' kill switch (default: true)
    NTFY_CLICK_URL          — tap target (default: https://www.yumdude.com/dashboard/orders; empty disables)
    NTFY_TIMEOUT_SECONDS    — HTTP timeout (default: 3)
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

from aws_lambda_powertools import Logger

logger = Logger()

_DEFAULT_TOPIC_URL = "https://ntfy.sh/yumdudeneworders"
_DEFAULT_TITLE = "🚨 NEW ORDER"
# ntfy JSON publish API requires numeric priority (1=min … 5=max).
# https://docs.ntfy.sh/publish/#message-priority
_DEFAULT_PRIORITY = 5
# Tags MUST be a list for the ntfy JSON publish API.
_DEFAULT_TAGS: list[str] = ["rotating_light", "shopping_cart"]
# Deep link opened when the notification is tapped. Points at the restaurant
# app's orders tab; installed apps open it natively via Android App Links /
# iOS Universal Links, everyone else falls back to the web orders page.
# https://docs.ntfy.sh/publish/#click-action
_DEFAULT_CLICK_URL = "https://www.yumdude.com/dashboard/orders"


def _is_prod() -> bool:
    return (os.environ.get("ENVIRONMENT", "dev") or "").strip().lower() == "prod"


def _is_enabled() -> bool:
    raw = (os.environ.get("NTFY_NEW_ORDER_ENABLED", "true") or "").strip().lower()
    return raw in ("true", "1", "yes", "on")


def _resolve_click_url() -> Optional[str]:
    """URL opened when the notification is tapped, or None to omit it.

    Defaults to the orders deep link; an explicit empty NTFY_CLICK_URL disables
    the tap action. Only http(s) is accepted — App / Universal Links require an
    https URL, and a malformed value is dropped rather than sent.
    """
    raw = os.environ.get("NTFY_CLICK_URL")
    raw = (_DEFAULT_CLICK_URL if raw is None else raw).strip()
    if not raw:
        return None
    parsed = urllib.parse.urlsplit(raw)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        logger.warning(f"ntfy: ignoring invalid NTFY_CLICK_URL '{raw}'")
        return None
    return raw


def _format_amount(value: Optional[float]) -> str:
    if value is None:
        return ""
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return ""
    if amount <= 0:
        return ""
    return f"₹{int(amount)}" if amount == int(amount) else f"₹{amount:.2f}"


def _split_topic_url(topic_url: str) -> tuple[str, str]:
    """Split a per-topic ntfy URL into (publish_url, topic).

    `https://ntfy.sh/yumdudeneworders` →
        publish_url='https://ntfy.sh/', topic='yumdudeneworders'

    The JSON publish API is POSTed to the server root with the topic carried
    inside the JSON body. The path may contain only the topic (single segment);
    anything else is rejected so misconfiguration fails loudly instead of
    silently posting to the wrong place.
    """
    parsed = urllib.parse.urlsplit(topic_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("missing scheme or host")
    topic = parsed.path.strip("/")
    if not topic or "/" in topic:
        raise ValueError("expected a single-segment topic in the path")
    publish_url = f"{parsed.scheme}://{parsed.netloc}/"
    return publish_url, topic


def _build_message(
    order_id: Optional[str],
    restaurant_name: Optional[str],
    amount: Optional[float],
) -> str:
    """Format a one-liner body: 'Order #<id> • <restaurant> • <amount>'."""
    parts: list[str] = []

    oid = (order_id or "").strip()
    if oid:
        parts.append(f"Order #{oid}")

    rname = (restaurant_name or "").strip()
    if rname:
        parts.append(rname)

    formatted = _format_amount(amount)
    if formatted:
        parts.append(formatted)

    return " • ".join(parts) if parts else "New order received"


def publish_new_order_alert(
    order_id: Optional[str] = None,
    restaurant_name: Optional[str] = None,
    amount: Optional[float] = None,
) -> bool:
    """Fire-and-forget ntfy push for a freshly-confirmed order.

    Best-effort: never raises. Returns True only when the HTTP request
    actually succeeded (2xx). Skipped entirely outside prod or when the
    kill-switch is off.
    """
    if not _is_prod():
        logger.debug("ntfy: skipped (ENVIRONMENT is not prod)")
        return False
    if not _is_enabled():
        logger.info("ntfy: skipped (NTFY_NEW_ORDER_ENABLED=false)")
        return False

    topic_url = (os.environ.get("NTFY_TOPIC_URL") or _DEFAULT_TOPIC_URL).strip()
    if not topic_url:
        logger.warning("ntfy: NTFY_TOPIC_URL is empty, skipping")
        return False

    try:
        publish_url, topic = _split_topic_url(topic_url)
    except ValueError as parse_err:
        logger.warning(f"ntfy: invalid NTFY_TOPIC_URL '{topic_url}': {parse_err}")
        return False

    try:
        timeout = float(os.environ.get("NTFY_TIMEOUT_SECONDS", "3") or "3")
    except ValueError:
        timeout = 3.0

    message = _build_message(order_id, restaurant_name, amount)

    # Use the ntfy JSON publish API so the emoji-bearing title travels in a
    # UTF-8 JSON body instead of an HTTP header (Python's urllib rejects
    # non-Latin-1 chars in headers — ordinal not in range(256)).
    # https://docs.ntfy.sh/publish/#publish-as-json
    payload = {
        "topic": topic,
        "title": _DEFAULT_TITLE,
        "message": message,
        "priority": _DEFAULT_PRIORITY,
        "tags": _DEFAULT_TAGS,
    }
    click_url = _resolve_click_url()
    if click_url:
        payload["click"] = click_url
    req = urllib.request.Request(
        publish_url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", None) or resp.getcode()
            if 200 <= status < 300:
                logger.info(f"ntfy: alert sent for orderId={order_id or ''} status={status}")
                return True
            logger.warning(f"ntfy: non-2xx response status={status}")
            return False
    except urllib.error.HTTPError as e:
        # Surface ntfy's response body so 4xx misconfigurations are easy to diagnose.
        body = ""
        try:
            body = (e.read() or b"").decode("utf-8", errors="replace")[:300]
        except Exception:  # noqa: BLE001
            pass
        logger.warning(f"ntfy: HTTP {e.code} sending alert: {body or e.reason}")
        return False
    except urllib.error.URLError as e:
        logger.warning(f"ntfy: network error sending alert: {e}")
        return False
    except Exception as e:  # noqa: BLE001 — best-effort, must never crash the caller
        logger.warning(f"ntfy: unexpected error sending alert: {e}")
        return False
