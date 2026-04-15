"""Payment service for Razorpay integration"""
import time
import razorpay
import requests
from typing import Optional, Dict, Any, List
from aws_lambda_powertools import Logger
from utils.datetime_ist import now_ist_iso
from botocore.exceptions import ClientError
from models.payment import Payment
from utils.dynamodb import dynamodb_client, TABLES
from utils.dynamodb_helpers import python_to_dynamodb
from utils.ssm import get_secret

logger = Logger()

# Razorpay mode (test or live)
RAZORPAY_MODE = get_secret('RAZORPAY_MODE', 'test')

# Get appropriate credentials based on mode
if RAZORPAY_MODE == 'test':
    RAZORPAY_KEY_ID = get_secret('RAZORPAY_TEST_KEY_ID', '')
    RAZORPAY_KEY_SECRET = get_secret('RAZORPAY_TEST_KEY_SECRET', '')
    logger.info("🧪 Using Razorpay TEST mode")
else:
    RAZORPAY_KEY_ID = get_secret('RAZORPAY_LIVE_KEY_ID', '')
    RAZORPAY_KEY_SECRET = get_secret('RAZORPAY_LIVE_KEY_SECRET', '')
    logger.info("💰 Using Razorpay LIVE mode")

# Initialize Razorpay client
razorpay_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))

# Merchant ops: enable UPI, card, netbanking, wallet (and test/live keys) in Razorpay Dashboard.
# COD in-app uses Payment.METHOD_COD via /payments/cod-confirm (Standard Checkout has no in-sheet COD).


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
    def get_payment_by_razorpay_order_id(razorpay_order_id: str) -> Optional[Payment]:
        """Get payment by Razorpay order ID using GSI."""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['PAYMENTS'],
                IndexName='razorpayOrderId-index',
                KeyConditionExpression='razorpayOrderId = :razorpay_order_id',
                ExpressionAttributeValues={
                    ':razorpay_order_id': {'S': razorpay_order_id}
                },
                Limit=1
            )

            items = response.get('Items', [])
            if not items:
                return None

            return Payment.from_dynamodb_item(items[0])
        except ClientError as e:
            logger.error(f"Failed to get payment by razorpayOrderId: {str(e)}")
            raise

    @staticmethod
    def get_payment_by_razorpay_payment_id(razorpay_payment_id: str) -> Optional[Payment]:
        """Get payment by Razorpay payment ID using GSI."""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['PAYMENTS'],
                IndexName='razorpayPaymentId-index',
                KeyConditionExpression='razorpayPaymentId = :razorpay_payment_id',
                ExpressionAttributeValues={
                    ':razorpay_payment_id': {'S': razorpay_payment_id}
                },
                Limit=1
            )

            items = response.get('Items', [])
            if not items:
                return None

            return Payment.from_dynamodb_item(items[0])
        except ClientError as e:
            logger.error(f"Failed to get payment by razorpayPaymentId: {str(e)}")
            raise

    @staticmethod
    def get_payment_by_razorpay_qr_code_id(qr_code_id: str) -> Optional[Payment]:
        """Lookup payment by Razorpay dynamic UPI QR id (GSI)."""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['PAYMENTS'],
                IndexName='razorpayQrCodeId-index',
                KeyConditionExpression='razorpayQrCodeId = :qid',
                ExpressionAttributeValues={':qid': {'S': qr_code_id}},
                Limit=1,
            )
            items = response.get('Items', [])
            if not items:
                return None
            return Payment.from_dynamodb_item(items[0])
        except ClientError as e:
            logger.error(f"Failed to get payment by razorpayQrCodeId: {str(e)}")
            raise

    @staticmethod
    def get_initiated_rider_upi_payments_for_order(order_id: str) -> List[Payment]:
        """INITIATED payments for this order (rider completes via UPI QR or cash at delivery)."""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['PAYMENTS'],
                IndexName='orderId-index',
                KeyConditionExpression='orderId = :oid',
                ExpressionAttributeValues={':oid': {'S': order_id}},
            )
            out: List[Payment] = []
            for raw in response.get('Items', []):
                p = Payment.from_dynamodb_item(raw)
                st = (p.payment_status or "").strip().upper()
                if st == Payment.STATUS_INITIATED or not st:
                    out.append(p)
            return out
        except ClientError as e:
            logger.error(f"Failed to query payments by orderId: {str(e)}")
            raise

    @staticmethod
    def create_upi_qr_code(
        amount_rupees: float,
        payment_id: str,
        order_id: str,
        close_by_epoch: int,
    ) -> Dict[str, Any]:
        """
        Create a single-use fixed-amount UPI QR via Razorpay REST API.
        Amount in rupees is converted to paise. close_by: Unix seconds (2 min–2 h ahead per Razorpay).
        """
        paise = max(1, int(round(float(amount_rupees) * 100)))
        url = 'https://api.razorpay.com/v1/payments/qr_codes'
        payload: Dict[str, Any] = {
            'type': 'upi_qr',
            'usage': 'single_use',
            'fixed_amount': True,
            'payment_amount': paise,
            'description': f'HonestEats order {order_id}',
            'close_by': int(close_by_epoch),
            'notes': {
                'payment_id': payment_id,
                'order_id': order_id,
            },
        }
        logger.info(
            f"Creating Razorpay UPI QR order={order_id} payment={payment_id} paise={paise} close_by={close_by_epoch}"
        )
        resp = requests.post(
            url,
            json=payload,
            auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET),
            timeout=45,
        )
        if resp.status_code >= 400:
            logger.error(f"Razorpay QR API HTTP {resp.status_code}: {resp.text}")
            raise RuntimeError(f"Razorpay QR create failed: {resp.text}")
        data = resp.json()
        logger.info(f"Razorpay UPI QR created id={data.get('id')}")
        return data

    @staticmethod
    def default_qr_close_by_epoch() -> int:
        """10 minutes from now (Razorpay single-use QR allows 2 min–2 h)."""
        return int(time.time()) + (10 * 60)
    
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
            updates['updatedAt'] = now_ist_iso()

            # None must use REMOVE, not SET NULL — GSI keys (e.g. razorpayQrCodeId) reject NULL.
            set_parts: List[str] = []
            remove_names: List[str] = []
            expr_attr_names: Dict[str, str] = {}
            expr_attr_values: Dict[str, Any] = {}

            for key, value in updates.items():
                attr_name = f"#{key}"
                expr_attr_names[attr_name] = key
                if value is None:
                    remove_names.append(attr_name)
                    continue
                attr_value = f":{key}"
                set_parts.append(f"{attr_name} = {attr_value}")
                if isinstance(value, bool):
                    expr_attr_values[attr_value] = {'BOOL': value}
                elif isinstance(value, (int, float)):
                    expr_attr_values[attr_value] = {'N': str(value)}
                elif isinstance(value, (list, dict)):
                    expr_attr_values[attr_value] = python_to_dynamodb(value)
                elif isinstance(value, str):
                    expr_attr_values[attr_value] = {'S': value}
                else:
                    expr_attr_values[attr_value] = {'S': str(value)}

            update_parts: List[str] = []
            if set_parts:
                update_parts.append('SET ' + ', '.join(set_parts))
            if remove_names:
                update_parts.append('REMOVE ' + ', '.join(remove_names))
            update_expr = ' '.join(update_parts)

            kwargs: Dict[str, Any] = {
                'TableName': TABLES['PAYMENTS'],
                'Key': {'paymentId': {'S': payment_id}},
                'UpdateExpression': update_expr,
                'ExpressionAttributeNames': expr_attr_names,
                'ReturnValues': 'ALL_NEW',
            }
            if expr_attr_values:
                kwargs['ExpressionAttributeValues'] = expr_attr_values

            response = dynamodb_client.update_item(**kwargs)
            
            logger.info(f"Payment updated: {payment_id}")
            return Payment.from_dynamodb_item(response['Attributes'])
        except ClientError as e:
            logger.error(f"Failed to update payment: {str(e)}")
            raise
    
    @staticmethod
    def list_payments_by_customer(customer_phone: str, limit: int = 20):
        """List payments for a customer."""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['PAYMENTS'],
                IndexName='customer-phone-createdAtIso-index',
                KeyConditionExpression='customerPhone = :phone',
                ExpressionAttributeValues={
                    ':phone': {'S': customer_phone}
                },
                Limit=limit,
                ScanIndexForward=False
            )

            payments = []
            for item in response.get('Items', []):
                payments.append(Payment.from_dynamodb_item(item))

            return payments
        except ClientError as e:
            logger.error(f"Failed to list payments: {str(e)}")
            raise
