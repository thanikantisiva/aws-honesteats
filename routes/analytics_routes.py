"""Analytics routes — dashboard metrics for ops/finance/Retool."""
from datetime import datetime, timezone

from aws_lambda_powertools import Logger, Tracer, Metrics

from services.analytics_service import generate_dashboard_metrics
from services.restaurant_service import RestaurantService

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_analytics_routes(app):
    """Register analytics routes on the resolver."""

    @app.get("/api/v1/analytics/dashboard")
    @tracer.capture_method
    def get_dashboard():
        """Build dashboard metrics for an inclusive [startDate, endDate] window.

        Query params:
          startDate  YYYY-MM-DD (required)
          endDate    YYYY-MM-DD (required)
        """
        try:
            params = app.current_event.query_string_parameters or {}
            start_date = params.get("startDate")
            end_date = params.get("endDate")

            if not start_date or not end_date:
                return {
                    "error": "Missing query params",
                    "message": "startDate and endDate are required (YYYY-MM-DD)",
                }, 400

            logger.info(
                f"[analytics] dashboard request startDate={start_date} endDate={end_date}"
            )
            result = generate_dashboard_metrics(start_date, end_date)
            metrics.add_metric(name="AnalyticsDashboardRequested", unit="Count", value=1)
            return result, 200
        except ValueError as e:
            return {"error": "Invalid date", "message": str(e)}, 400
        except Exception as e:
            logger.error("Failed to generate analytics dashboard", exc_info=True)
            return {
                "error": "Failed to generate analytics",
                "message": str(e),
            }, 500

    @app.get("/api/v1/admin/restaurants/notification-health")
    @tracer.capture_method
    def get_notification_health():
        """Per-restaurant FCM token health snapshot.

        Surfaces 'who can actually be notified vs who can't' so ops can chase
        restaurants whose tokens are missing or stale before they miss orders.

        Each row: restaurantId, name, tokenCount, hasLegacyToken, lastUpdated,
        ageHours, status (HEALTHY / STALE / MISSING). 'STALE' = token last
        updated > 7 days ago — FCM frequently rotates tokens after that.
        """
        try:
            restaurants = RestaurantService.list_restaurants()

            STALE_THRESHOLD_HOURS = 24 * 7
            now = datetime.now(timezone.utc)

            rows = []
            healthy = stale = missing = 0
            for r in restaurants:
                tokens = list(r.fcm_tokens or [])
                legacy = r.fcm_token or ""
                has_legacy = bool(legacy)
                token_count = len(set(tokens + ([legacy] if has_legacy else [])))

                age_hours = None
                last_updated = r.fcm_token_updated_at or ""
                if last_updated:
                    try:
                        parsed = datetime.fromisoformat(last_updated)
                        if parsed.tzinfo is None:
                            parsed = parsed.replace(tzinfo=timezone.utc)
                        age_hours = round((now - parsed).total_seconds() / 3600, 1)
                    except (ValueError, TypeError):
                        age_hours = None

                if token_count == 0:
                    status = "MISSING"
                    missing += 1
                elif age_hours is None or age_hours > STALE_THRESHOLD_HOURS:
                    status = "STALE"
                    stale += 1
                else:
                    status = "HEALTHY"
                    healthy += 1

                rows.append({
                    "restaurantId": r.restaurant_id,
                    "name": r.name or r.restaurant_id,
                    "tokenCount": token_count,
                    "hasLegacyToken": has_legacy,
                    "lastUpdated": last_updated or None,
                    "ageHours": age_hours,
                    "status": status,
                })

            # Worst first so ops sees who to chase
            order = {"MISSING": 0, "STALE": 1, "HEALTHY": 2}
            rows.sort(key=lambda x: (order.get(x["status"], 9), -(x.get("ageHours") or 0)))

            metrics.add_metric(name="NotificationHealthRequested", unit="Count", value=1)
            return {
                "summary": {
                    "total": len(rows),
                    "healthy": healthy,
                    "stale": stale,
                    "missing": missing,
                    "staleThresholdHours": STALE_THRESHOLD_HOURS,
                },
                "restaurants": rows,
            }, 200
        except Exception as e:
            logger.error("Failed to compute notification health", exc_info=True)
            return {
                "error": "Failed to compute notification health",
                "message": str(e),
            }, 500
