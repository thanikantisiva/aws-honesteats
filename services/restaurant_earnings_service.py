"""Restaurant earnings service"""
from typing import List
from botocore.exceptions import ClientError
from datetime import datetime
from models.restaurant_earnings import RestaurantEarnings
from utils.dynamodb import dynamodb_client, TABLES


class RestaurantEarningsService:
    """Service for restaurant earnings operations"""

    @staticmethod
    def add_order_earning(restaurant_id: str, order_id: str, amount: float):
        """Add a restaurant earning entry per order"""
        try:
            today = datetime.utcnow().strftime('%Y-%m-%d')
            earnings = RestaurantEarnings(
                restaurant_id=restaurant_id,
                date=f"{today}#{order_id}",
                total_orders=1,
                total_earnings=amount,
                order_id=order_id,
                settled=False,
                settled_at=None,
                settlement_id=None
            )

            dynamodb_client.put_item(
                TableName=TABLES['RESTAURANT_EARNINGS'],
                Item=earnings.to_dynamodb_item()
            )
        except ClientError as e:
            raise Exception(f"Failed to add restaurant earning: {str(e)}")

    @staticmethod
    def add_refund_adjustment(
        restaurant_id: str,
        order_id: str,
        refund_id: str,
        amount: float,
        created_at_epoch: int = None
    ):
        """Add a negative restaurant earning row for refund adjustments."""
        try:
            if created_at_epoch:
                date_prefix = datetime.utcfromtimestamp(int(created_at_epoch)).strftime('%Y-%m-%d')
            else:
                date_prefix = datetime.utcnow().strftime('%Y-%m-%d')

            earnings = RestaurantEarnings(
                restaurant_id=restaurant_id,
                date=f"{date_prefix}#{order_id}#REFUND#{refund_id}",
                total_orders=0,
                total_earnings=-abs(float(amount)),
                order_id=order_id,
                settled=False,
                settled_at=None,
                settlement_id=None
            )

            dynamodb_client.put_item(
                TableName=TABLES['RESTAURANT_EARNINGS'],
                Item=earnings.to_dynamodb_item(),
                ConditionExpression='attribute_not_exists(restaurantId) AND attribute_not_exists(#date)',
                ExpressionAttributeNames={
                    '#date': 'date'
                }
            )
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                return
            raise Exception(f"Failed to add restaurant refund adjustment: {str(e)}")

    @staticmethod
    def get_earnings_for_date_range(restaurant_id: str, start_date: str, end_date: str) -> List[RestaurantEarnings]:
        """Get restaurant earnings for a date range"""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['RESTAURANT_EARNINGS'],
                KeyConditionExpression='restaurantId = :restaurantId AND #date BETWEEN :start AND :end',
                ExpressionAttributeNames={
                    '#date': 'date'
                },
                ExpressionAttributeValues={
                    ':restaurantId': {'S': restaurant_id},
                    ':start': {'S': f'{start_date}#'},
                    ':end': {'S': f'{end_date}#\uffff'}
                }
            )

            earnings_list = []
            for item in response.get('Items', []):
                earnings_list.append(RestaurantEarnings.from_dynamodb_item(item))

            return earnings_list
        except ClientError as e:
            raise Exception(f"Failed to get restaurant earnings: {str(e)}")

    @staticmethod
    def settle_earnings_for_orders(
        restaurant_id: str,
        order_ids: List[str],
        start_date: str,
        end_date: str,
        settlement_id: str
    ) -> List[str]:
        """Mark restaurant earnings rows as settled for matching orderIds in date range"""
        try:
            earnings_list = RestaurantEarningsService.get_earnings_for_date_range(
                restaurant_id,
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
                    TableName=TABLES['RESTAURANT_EARNINGS'],
                    Key={
                        'restaurantId': {'S': restaurant_id},
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
            raise Exception(f"Failed to settle restaurant earnings: {str(e)}")
