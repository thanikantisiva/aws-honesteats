"""Restaurant earnings service"""
from typing import List
from botocore.exceptions import ClientError
from datetime import datetime
from models.restaurant_earnings import RestaurantEarnings
from utils.dynamodb import dynamodb_client, TABLES, generate_id


class RestaurantEarningsService:
    """Service for restaurant earnings operations"""

    @staticmethod
    def add_order_earning(restaurant_id: str, order_id: str, amount: float, date_override: str = None):
        """Add a restaurant earning entry per order"""
        try:
            today = date_override or datetime.utcnow().strftime('%Y-%m-%d')
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
    def add_item_adjustment(
        restaurant_id: str,
        order_id: str,
        adjustment_id: str,
        delta_amount: float,
    ):
        """Add a signed restaurant earnings row for an ops item-adjustment.

        `delta_amount` is `newRestaurantPayout - oldRestaurantPayout`:
          - negative when items were removed / replaced with cheaper ones (restaurant earns less)
          - positive when replacement items are more expensive (restaurant earns more)

        Row key is `YYYY-MM-DD#orderId#ADJUSTMENT#adjustmentId` so multiple
        adjustments on the same order can co-exist without overwriting each
        other, and a retry of the same adjustment is idempotent via the
        attribute_not_exists ConditionExpression.
        """
        try:
            date_prefix = datetime.utcnow().strftime('%Y-%m-%d')
            earnings = RestaurantEarnings(
                restaurant_id=restaurant_id,
                date=f"{date_prefix}#{order_id}#ADJUSTMENT#{adjustment_id}",
                total_orders=0,
                total_earnings=float(delta_amount),
                order_id=order_id,
                settled=False,
                settled_at=None,
                settlement_id=None
            )

            dynamodb_client.put_item(
                TableName=TABLES['RESTAURANT_EARNINGS'],
                Item=earnings.to_dynamodb_item(),
                ConditionExpression='attribute_not_exists(restaurantId) AND attribute_not_exists(#date)',
                ExpressionAttributeNames={'#date': 'date'}
            )
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                return
            raise Exception(f"Failed to add restaurant item adjustment: {str(e)}")

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
    def add_manual_adjustment(
        restaurant_id: str,
        order_id: str,
        amount: float,
        comments: str,
    ) -> dict:
        """Add a manual ops adjustment row against a restaurant's earnings ledger.

        Posted from the admin "Order Issues" flow. `amount` is already signed:
          - positive  → credit (restaurant earns more)
          - negative  → debit  (restaurant earns less)

        Row key is `YYYY-MM-DD#orderId#ISSUE#adjustmentId` so multiple manual
        adjustments on the same order co-exist, and the unique generated
        adjustmentId keeps the attribute_not_exists ConditionExpression a no-op
        guard against an accidental duplicate write of the same id.

        The row carries `comments` for audit and is left unsettled so it shows
        up in the restaurant's earnings history alongside order rows.
        """
        try:
            adjustment_id = generate_id('ISSUE')
            date_prefix = datetime.utcnow().strftime('%Y-%m-%d')
            earnings = RestaurantEarnings(
                restaurant_id=restaurant_id,
                date=f"{date_prefix}#{order_id}#ISSUE#{adjustment_id}",
                total_orders=0,
                total_earnings=float(amount),
                order_id=order_id,
                settled=False,
                settled_at=None,
                settlement_id=None,
                comments=comments,
            )

            dynamodb_client.put_item(
                TableName=TABLES['RESTAURANT_EARNINGS'],
                Item=earnings.to_dynamodb_item(),
                ConditionExpression='attribute_not_exists(restaurantId) AND attribute_not_exists(#date)',
                ExpressionAttributeNames={'#date': 'date'}
            )
            return earnings.to_dict()
        except ClientError as e:
            raise Exception(f"Failed to add restaurant manual adjustment: {str(e)}")

    @staticmethod
    def get_earnings_for_date_range(restaurant_id: str, start_date: str, end_date: str) -> List[RestaurantEarnings]:
        """Get restaurant earnings for a date range.

        Paginates through all pages using LastEvaluatedKey so no records are
        silently dropped when the result set exceeds DynamoDB's 1 MB page limit.
        """
        try:
            query_kwargs = {
                'TableName': TABLES['RESTAURANT_EARNINGS'],
                'KeyConditionExpression': 'restaurantId = :restaurantId AND #date BETWEEN :start AND :end',
                'ExpressionAttributeNames': {'#date': 'date'},
                'ExpressionAttributeValues': {
                    ':restaurantId': {'S': restaurant_id},
                    ':start': {'S': f'{start_date}#'},
                    ':end':   {'S': f'{end_date}#\uffff'},
                },
            }

            earnings_list = []
            while True:
                response = dynamodb_client.query(**query_kwargs)
                for item in response.get('Items', []):
                    earnings_list.append(RestaurantEarnings.from_dynamodb_item(item))

                last_key = response.get('LastEvaluatedKey')
                if not last_key:
                    break
                query_kwargs['ExclusiveStartKey'] = last_key

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

                try:
                    dynamodb_client.update_item(
                        TableName=TABLES['RESTAURANT_EARNINGS'],
                        Key={
                            'restaurantId': {'S': restaurant_id},
                            'date': {'S': earning.date}
                        },
                        UpdateExpression='SET settled = :settled, settledAt = :settledAt, settlementId = :settlementId',
                        ConditionExpression='settled = :false',
                        ExpressionAttributeValues={
                            ':settled':      {'BOOL': True},
                            ':settledAt':    {'S': settled_at},
                            ':settlementId': {'S': settlement_id},
                            ':false':        {'BOOL': False},
                        }
                    )
                    updated_order_ids.append(earning.order_id)
                except ClientError as ce:
                    if ce.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                        # Already settled by a concurrent request — skip safely
                        continue
                    raise

            return updated_order_ids
        except ClientError as e:
            raise Exception(f"Failed to settle restaurant earnings: {str(e)}")
