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
        """Get or create earnings record for a specific date"""
        try:
            response = dynamodb_client.get_item(
                TableName=TABLES['EARNINGS'],
                Key={
                    'riderId': {'S': rider_id},
                    'date': {'S': date}
                }
            )
            
            if 'Item' in response:
                return RiderEarnings.from_dynamodb_item(response['Item'])
            
            # Create new earnings record
            earnings = RiderEarnings(
                rider_id=rider_id,
                date=date
            )
            
            dynamodb_client.put_item(
                TableName=TABLES['EARNINGS'],
                Item=earnings.to_dynamodb_item()
            )
            
            return earnings
        except ClientError as e:
            raise Exception(f"Failed to get/create earnings: {str(e)}")
    
    @staticmethod
    def add_delivery(rider_id: str, delivery_fee: float, tip: float = 0.0):
        """Add a delivery to rider's earnings"""
        try:
            today = datetime.utcnow().strftime('%Y-%m-%d')
            
            dynamodb_client.update_item(
                TableName=TABLES['EARNINGS'],
                Key={
                    'riderId': {'S': rider_id},
                    'date': {'S': today}
                },
                UpdateExpression='ADD totalDeliveries :one, deliveryFees :fee, tips :tip, totalEarnings :total',
                ExpressionAttributeValues={
                    ':one': {'N': '1'},
                    ':fee': {'N': str(delivery_fee)},
                    ':tip': {'N': str(tip)},
                    ':total': {'N': str(delivery_fee + tip)}
                }
            )
        except ClientError as e:
            # If item doesn't exist, create it first
            if e.response['Error']['Code'] == 'ValidationException':
                earnings = EarningsService.get_or_create_daily_earnings(rider_id, today)
                EarningsService.add_delivery(rider_id, delivery_fee, tip)
            else:
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
                    ':start': {'S': start_date},
                    ':end': {'S': end_date}
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
