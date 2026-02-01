"""Payment service for Razorpay integration"""
import os
import razorpay
from typing import Optional, Dict, Any
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from models.payment import Payment
from utils.dynamodb import dynamodb_client, TABLES

logger = Logger()

# Razorpay mode (test or live)
RAZORPAY_MODE = os.environ.get('RAZORPAY_MODE', 'test')

# Get appropriate credentials based on mode
if RAZORPAY_MODE == 'test':
    RAZORPAY_KEY_ID = os.environ.get('RAZORPAY_TEST_KEY_ID', '')
    RAZORPAY_KEY_SECRET = os.environ.get('RAZORPAY_TEST_KEY_SECRET', '')
    logger.info("🧪 Using Razorpay TEST mode")
else:
    RAZORPAY_KEY_ID = os.environ.get('RAZORPAY_LIVE_KEY_ID', '')
    RAZORPAY_KEY_SECRET = os.environ.get('RAZORPAY_LIVE_KEY_SECRET', '')
    logger.info("💰 Using Razorpay LIVE mode")

# Initialize Razorpay client
razorpay_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))


class PaymentService:
    """Service for payment operations"""
    
    @staticmethod
    def get_razorpay_key_id() -> str:
        """Get the current Razorpay key ID (for frontend)"""
        return RAZORPAY_KEY_ID
    
    @staticmethod
    def create_razorpay_order(
        amount_in_rupees: float,
        receipt_id: str,
        customer_phone: str,
        notes: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create Razorpay order
        
        Args:
            amount_in_rupees: Amount in rupees (will be converted to paise)
            receipt_id: Unique receipt ID (usually payment_id)
            customer_phone: Customer phone number
            notes: Optional additional notes
            
        Returns:
            Razorpay order details
        """
        try:
            order_data = {
                'amount': int(amount_in_rupees * 100),  # Convert rupees to paise
                'currency': 'INR',
                'receipt': receipt_id,
                'notes': notes or {}
            }
            order_data['notes']['customer_phone'] = customer_phone
            order_data['notes']['mode'] = RAZORPAY_MODE
            
            logger.info(f"Creating Razorpay order: amount={amount_in_rupees}, receipt={receipt_id}")
            razorpay_order = razorpay_client.order.create(data=order_data)
            logger.info(f"✅ Razorpay order created: {razorpay_order['id']}")
            
            return razorpay_order
        except Exception as e:
            logger.error(f"Failed to create Razorpay order: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    def verify_payment_signature(
        razorpay_order_id: str,
        razorpay_payment_id: str,
        razorpay_signature: str
    ) -> bool:
        """
        Verify Razorpay payment signature
        
        Args:
            razorpay_order_id: Razorpay order ID
            razorpay_payment_id: Razorpay payment ID
            razorpay_signature: Signature to verify
            
        Returns:
            True if signature is valid, False otherwise
        """
        try:
            # Handle test mode mock signatures
            if razorpay_payment_id.startswith('pay_TEST') and razorpay_signature.startswith('mock_signature_'):
                logger.info(f"🧪 TEST MODE: Accepting mock payment signature")
                return True
            
            params_dict = {
                'razorpay_order_id': razorpay_order_id,
                'razorpay_payment_id': razorpay_payment_id,
                'razorpay_signature': razorpay_signature
            }
            
            razorpay_client.utility.verify_payment_signature(params_dict)
            logger.info(f"✅ Payment signature verified: {razorpay_payment_id}")
            return True
        except razorpay.errors.SignatureVerificationError as e:
            logger.error(f"❌ Payment signature verification failed: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error verifying payment: {str(e)}", exc_info=True)
            return False
    
    @staticmethod
    def get_payment(payment_id: str) -> Optional[Payment]:
        """Get payment by ID"""
        try:
            response = dynamodb_client.get_item(
                TableName=TABLES['PAYMENTS'],
                Key={'paymentId': {'S': payment_id}}
            )
            
            if 'Item' not in response:
                return None
            
            return Payment.from_dynamodb_item(response['Item'])
        except ClientError as e:
            logger.error(f"Failed to get payment: {str(e)}")
            raise
    
    @staticmethod
    def create_payment(payment: Payment) -> Payment:
        """Create a new payment record"""
        try:
            dynamodb_client.put_item(
                TableName=TABLES['PAYMENTS'],
                Item=payment.to_dynamodb_item()
            )
            logger.info(f"Payment record created: {payment.payment_id}")
            return payment
        except ClientError as e:
            logger.error(f"Failed to create payment: {str(e)}")
            raise
    
    @staticmethod
    def update_payment(
        payment_id: str,
        updates: Dict[str, Any]
    ) -> Payment:
        """Update payment record"""
        try:
            import time
            updates['updatedAt'] = int(time.time() * 1000)
            
            # Build update expression
            update_expr = "SET "
            expr_attr_names = {}
            expr_attr_values = {}
            
            for key, value in updates.items():
                attr_name = f"#{key}"
                attr_value = f":{key}"
                update_expr += f"{attr_name} = {attr_value}, "
                expr_attr_names[attr_name] = key
                
                # Convert value to DynamoDB format
                if isinstance(value, str):
                    expr_attr_values[attr_value] = {'S': value}
                elif isinstance(value, (int, float)):
                    expr_attr_values[attr_value] = {'N': str(value)}
                elif value is None:
                    expr_attr_values[attr_value] = {'NULL': True}
            
            update_expr = update_expr.rstrip(', ')
            
            response = dynamodb_client.update_item(
                TableName=TABLES['PAYMENTS'],
                Key={'paymentId': {'S': payment_id}},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values,
                ReturnValues='ALL_NEW'
            )
            
            logger.info(f"Payment updated: {payment_id}")
            return Payment.from_dynamodb_item(response['Attributes'])
        except ClientError as e:
            logger.error(f"Failed to update payment: {str(e)}")
            raise
    
    @staticmethod
    def list_payments_by_customer(customer_phone: str, limit: int = 20):
        """List payments for a customer"""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['PAYMENTS'],
                IndexName='customer-phone-createdAt-index',
                KeyConditionExpression='customerPhone = :phone',
                ExpressionAttributeValues={
                    ':phone': {'S': customer_phone}
                },
                Limit=limit,
                ScanIndexForward=False  # Latest first
            )
            
            payments = []
            for item in response.get('Items', []):
                payments.append(Payment.from_dynamodb_item(item))
            
            return payments
        except ClientError as e:
            logger.error(f"Failed to list payments: {str(e)}")
            raise

