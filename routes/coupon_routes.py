"""Coupon routes"""
from aws_lambda_powertools import Logger, Tracer, Metrics
from utils.dynamodb import dynamodb_client, TABLES

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_coupon_routes(app):
    """Register coupon routes"""

    @app.post("/api/v1/coupons")
    @tracer.capture_method
    def create_coupon():
        """Create or update a coupon in config table"""
        try:
            body = app.current_event.json_body or {}
            code = body.get('couponCode')
            coupon_type = body.get('couponType')
            coupon_value = body.get('couponValue')
            start_date = body.get('startDate')
            end_date = body.get('endDate')

            if not code or not coupon_type or coupon_value is None:
                return {"error": "couponCode, couponType, couponValue are required"}, 400

            pk = f"COUPON#{code}"
            sk = "DETAILS"

            item = {
                'partitionkey': {'S': pk},
                'sortKey': {'S': sk},
                'couponType': {'S': str(coupon_type)},
                'couponValue': {'N': str(coupon_value)}
            }
            if start_date:
                item['startDate'] = {'S': str(start_date)}
            if end_date:
                item['endDate'] = {'S': str(end_date)}

            dynamodb_client.put_item(
                TableName=TABLES['CONFIG'],
                Item=item
            )

            metrics.add_metric(name="CouponSaved", unit="Count", value=1)
            return {"message": "Coupon saved", "couponCode": code}, 200
        except Exception as e:
            logger.error("Error saving coupon", exc_info=True)
            return {"error": "Failed to save coupon", "message": str(e)}, 500

    @app.delete("/api/v1/coupons/<coupon_code>")
    @tracer.capture_method
    def delete_coupon(coupon_code: str):
        """Delete a coupon from config table"""
        try:
            pk = f"COUPON#{coupon_code}"
            sk = "DETAILS"

            dynamodb_client.delete_item(
                TableName=TABLES['CONFIG'],
                Key={
                    'partitionkey': {'S': pk},
                    'sortKey': {'S': sk}
                }
            )

            metrics.add_metric(name="CouponDeleted", unit="Count", value=1)
            return {"message": "Coupon deleted", "couponCode": coupon_code}, 200
        except Exception as e:
            logger.error("Error deleting coupon", exc_info=True)
            return {"error": "Failed to delete coupon", "message": str(e)}, 500
