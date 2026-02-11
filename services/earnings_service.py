"""Earnings service for rider earnings tracking"""
from typing import List, Optional
from botocore.exceptions import ClientError
from models.rider_earnings import RiderEarnings
from utils.dynamodb import dynamodb_client, TABLES
from datetime import datetime, timedelta


class EarningsService:
    """Service for rider earnings operations"""
    
    @staticmethod
    def get_or_create_daily_earnings(rider_id: str, date: str) -> RiderEarnings:
        """Get aggregated earnings summary for a specific date"""
        try:
            earnings_list = EarningsService.get_earnings_for_date_range(
                rider_id,
                date,
                date
            )
            total_deliveries = sum(e.total_deliveries for e in earnings_list)
            total_earnings = sum(e.total_earnings for e in earnings_list)
            total_fees = sum(e.delivery_fees for e in earnings_list)
            total_tips = sum(e.tips for e in earnings_list)
            total_incentives = sum(e.incentives for e in earnings_list)

            return RiderEarnings(
                rider_id=rider_id,
                date=date,
                total_deliveries=total_deliveries,
                total_earnings=total_earnings,
                delivery_fees=total_fees,
                tips=total_tips,
                incentives=total_incentives
            )
        except ClientError as e:
            raise Exception(f"Failed to get earnings: {str(e)}")
    
    @staticmethod
    def add_delivery(rider_id: str, order_id: str, delivery_fee: float, tip: float = 0.0):
        """Add a delivery to rider's earnings"""
        try:
            today = datetime.utcnow().strftime('%Y-%m-%d')
            earnings = RiderEarnings(
                rider_id=rider_id,
                date=f"{today}#{order_id}",
                total_deliveries=1,
                total_earnings=delivery_fee + tip,
                delivery_fees=delivery_fee,
                tips=tip,
                incentives=0.0,
                order_id=order_id,
                settled=False,
                settled_at=None
            )

            dynamodb_client.put_item(
                TableName=TABLES['EARNINGS'],
                Item=earnings.to_dynamodb_item()
            )
        except ClientError as e:
            raise Exception(f"Failed to add delivery: {str(e)}")
    
    @staticmethod
    def get_earnings_for_date_range(rider_id: str, start_date: str, end_date: str) -> List[RiderEarnings]:
        """Get earnings for a date range"""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['EARNINGS'],
                KeyConditionExpression='riderId = :riderId AND #date BETWEEN :start AND :end',
                ExpressionAttributeNames={
                    '#date': 'date'
                },
                ExpressionAttributeValues={
                    ':riderId': {'S': rider_id},
                    ':start': {'S': f'{start_date}#'},
                    ':end': {'S': f'{end_date}#\uffff'}
                }
            )
            
            earnings_list = []
            for item in response.get('Items', []):
                earnings_list.append(RiderEarnings.from_dynamodb_item(item))
            
            return earnings_list
        except ClientError as e:
            raise Exception(f"Failed to get earnings: {str(e)}")
    
    @staticmethod
    def get_today_earnings(rider_id: str) -> RiderEarnings:
        """Get today's earnings"""
        today = datetime.utcnow().strftime('%Y-%m-%d')
        return EarningsService.get_or_create_daily_earnings(rider_id, today)
    
    @staticmethod
    def get_weekly_earnings(rider_id: str) -> dict:
        """Get this week's earnings summary"""
        today = datetime.utcnow()
        start_of_week = today - timedelta(days=today.weekday())
        
        start_date = start_of_week.strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
        
        earnings_list = EarningsService.get_earnings_for_date_range(rider_id, start_date, end_date)
        
        total_deliveries = sum(e.total_deliveries for e in earnings_list)
        total_earnings = sum(e.total_earnings for e in earnings_list)
        total_tips = sum(e.tips for e in earnings_list)
        
        return {
            "period": "week",
            "startDate": start_date,
            "endDate": end_date,
            "totalDeliveries": total_deliveries,
            "totalEarnings": total_earnings,
            "totalTips": total_tips,
            "dailyBreakdown": [e.to_dict() for e in earnings_list]
        }
    
    @staticmethod
    def get_monthly_earnings(rider_id: str) -> dict:
        """Get this month's earnings summary"""
        today = datetime.utcnow()
        start_of_month = today.replace(day=1)
        
        start_date = start_of_month.strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
        
        earnings_list = EarningsService.get_earnings_for_date_range(rider_id, start_date, end_date)
        
        total_deliveries = sum(e.total_deliveries for e in earnings_list)
        total_earnings = sum(e.total_earnings for e in earnings_list)
        total_tips = sum(e.tips for e in earnings_list)
        
        return {
            "period": "month",
            "startDate": start_date,
            "endDate": end_date,
            "totalDeliveries": total_deliveries,
            "totalEarnings": total_earnings,
            "totalTips": total_tips,
            "dailyBreakdown": [e.to_dict() for e in earnings_list]
        }

    @staticmethod
    def settle_earnings_for_orders(
        rider_id: str,
        order_ids: List[str],
        start_date: str,
        end_date: str,
        settlement_id: str
    ) -> List[str]:
        """Mark earnings rows as settled for matching orderIds in date range"""
        try:
            earnings_list = EarningsService.get_earnings_for_date_range(
                rider_id,
                start_date,
                end_date
            )

            settled_at = datetime.utcnow().isoformat()
            updated_order_ids: List[str] = []

            for earning in earnings_list:
                if not earning.order_id:
                    continue
                if earning.order_id not in order_ids:
                    continue

                dynamodb_client.update_item(
                    TableName=TABLES['EARNINGS'],
                    Key={
                        'riderId': {'S': rider_id},
                        'date': {'S': earning.date}
                    },
                    UpdateExpression='SET settled = :settled, settledAt = :settledAt, settlementId = :settlementId',
                    ExpressionAttributeValues={
                        ':settled': {'BOOL': True},
                        ':settledAt': {'S': settled_at},
                        ':settlementId': {'S': settlement_id}
                    }
                )
                updated_order_ids.append(earning.order_id)

            return updated_order_ids
        except ClientError as e:
            raise Exception(f"Failed to settle earnings: {str(e)}")
