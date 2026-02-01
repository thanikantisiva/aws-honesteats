"""Rider earnings routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from services.earnings_service import EarningsService

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_earnings_routes(app):
    """Register rider earnings routes"""
    
    @app.get("/api/v1/riders/<rider_id>/earnings")
    @tracer.capture_method
    def get_rider_earnings(rider_id: str):
        """
        Get rider earnings summary
        
        Query params:
        - period: today | week | month (default: today)
        """
        try:
            query_params = app.current_event.query_string_parameters or {}
            period = query_params.get('period', 'today')
            
            logger.info(f"Getting earnings for rider: {rider_id}, period: {period}")
            
            if period == 'today':
                earnings = EarningsService.get_today_earnings(rider_id)
                # Convert to match the same format as week/month
                result = {
                    "period": "today",
                    "totalDeliveries": earnings.total_deliveries,
                    "totalEarnings": earnings.total_earnings,
                    "totalTips": earnings.tips,
                    "dailyBreakdown": [earnings.to_dict()]
                }
            elif period == 'week':
                result = EarningsService.get_weekly_earnings(rider_id)
            elif period == 'month':
                result = EarningsService.get_monthly_earnings(rider_id)
            else:
                return {"error": "Invalid period. Use: today, week, or month"}, 400
            
            metrics.add_metric(name="RiderEarningsRetrieved", unit="Count", value=1)
            
            return result, 200
            
        except Exception as e:
            logger.error("Error getting rider earnings", exc_info=True)
            return {"error": "Failed to get earnings", "message": str(e)}, 500
    
    @app.get("/api/v1/riders/<rider_id>/earnings/history")
    @tracer.capture_method
    def get_earnings_history(rider_id: str):
        """
        Get rider earnings history
        
        Query params:
        - startDate: YYYY-MM-DD
        - endDate: YYYY-MM-DD
        """
        try:
            query_params = app.current_event.query_string_parameters or {}
            start_date = query_params.get('startDate')
            end_date = query_params.get('endDate')
            
            if not start_date or not end_date:
                return {"error": "startDate and endDate required"}, 400
            
            logger.info(f"Getting earnings history for rider: {rider_id}, {start_date} to {end_date}")
            
            earnings_list = EarningsService.get_earnings_for_date_range(rider_id, start_date, end_date)
            
            total_deliveries = sum(e.total_deliveries for e in earnings_list)
            total_earnings = sum(e.total_earnings for e in earnings_list)
            
            metrics.add_metric(name="RiderEarningsHistoryRetrieved", unit="Count", value=1)
            
            return {
                "startDate": start_date,
                "endDate": end_date,
                "totalDeliveries": total_deliveries,
                "totalEarnings": total_earnings,
                "history": [e.to_dict() for e in earnings_list]
            }, 200
            
        except Exception as e:
            logger.error("Error getting earnings history", exc_info=True)
            return {"error": "Failed to get earnings history", "message": str(e)}, 500
