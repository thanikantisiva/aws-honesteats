"""Payment model for Razorpay transactions"""
from typing import List, Optional, Dict, Any, Union
from utils.datetime_ist import now_ist_iso, epoch_ms_to_ist_iso


class Payment:
    """Payment model for tracking Razorpay transactions"""
    
    # Payment statuses
    STATUS_INITIATED = "INITIATED"
    STATUS_SUCCESS = "SUCCESS"
    STATUS_FAILED = "FAILED"
    STATUS_REFUNDED = "REFUNDED"
    
    # Payment methods
    METHOD_UPI = "UPI"
    METHOD_CARD = "CARD"
    METHOD_WALLET = "WALLET"
    METHOD_NETBANKING = "NETBANKING"

    def __init__(
        self,
        payment_id: str,
        customer_phone: str,
        restaurant_id: str,
        restaurant_name: str,
        amount: float,
        razorpay_order_id: Optional[str] = None,
        razorpay_payment_id: Optional[str] = None,
        razorpay_signature: Optional[str] = None,
        payment_status: str = STATUS_INITIATED,
        payment_method: Optional[str] = None,
        upi_app: Optional[str] = None,
        order_id: Optional[str] = None,
        error_code: Optional[str] = None,
        error_description: Optional[str] = None,
        refund_amount: Optional[float] = None,
        revenue: Optional[dict] = None,
        created_at: Optional[Union[int, str]] = None,
        updated_at: Optional[Union[int, str]] = None
    ):
        self.payment_id = payment_id
        self.customer_phone = customer_phone
        self.restaurant_id = restaurant_id
        self.restaurant_name = restaurant_name
        self.amount = amount
        self.razorpay_order_id = razorpay_order_id
        self.razorpay_payment_id = razorpay_payment_id
        self.razorpay_signature = razorpay_signature
        self.payment_status = payment_status
        self.payment_method = payment_method
        self.upi_app = upi_app
        self.order_id = order_id
        self.error_code = error_code
        self.error_description = error_description
        self.refund_amount = float(refund_amount) if refund_amount is not None else None
        self.revenue = revenue  # Revenue breakdown for analytics
        _now = now_ist_iso()
        self.created_at = created_at if created_at is not None else _now
        self.updated_at = updated_at if updated_at is not None else self.created_at
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'paymentId': self.payment_id,
            'customerPhone': self.customer_phone,
            'restaurantId': self.restaurant_id,
            'restaurantName': self.restaurant_name,
            'amount': self.amount,
            'razorpayOrderId': self.razorpay_order_id,
            'razorpayPaymentId': self.razorpay_payment_id,
            'paymentStatus': self.payment_status,
            'paymentMethod': self.payment_method,
            'upiApp': self.upi_app,
            'orderId': self.order_id,
            'errorCode': self.error_code,
            'errorDescription': self.error_description,
            'refundAmount': self.refund_amount,
            'revenue': self.revenue,
            'createdAt': epoch_ms_to_ist_iso(self.created_at) if isinstance(self.created_at, int) else self.created_at,
            'updatedAt': epoch_ms_to_ist_iso(self.updated_at) if isinstance(self.updated_at, int) else self.updated_at
        }
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format.

        Current representation:
        - createdAt (S): IST ISO string
        - createdAtIso (S): new index key
        - updatedAt (S): IST ISO string
        """
        created_at = self.created_at
        updated_at = self.updated_at
        if isinstance(created_at, int):
            created_at_iso = epoch_ms_to_ist_iso(created_at)
        else:
            created_at_iso = str(created_at)

        if isinstance(updated_at, int):
            updated_at_iso = epoch_ms_to_ist_iso(updated_at)
        else:
            updated_at_iso = str(updated_at)

        item = {
            'paymentId': {'S': self.payment_id},
            'customerPhone': {'S': self.customer_phone},
            'restaurantId': {'S': self.restaurant_id},
            'restaurantName': {'S': self.restaurant_name},
            'amount': {'N': str(self.amount)},
            'paymentStatus': {'S': self.payment_status},
            'createdAt': {'S': created_at_iso},
            'createdAtIso': {'S': created_at_iso},
            'updatedAt': {'S': updated_at_iso}
        }
        
        if self.razorpay_order_id:
            item['razorpayOrderId'] = {'S': self.razorpay_order_id}
        if self.razorpay_payment_id:
            item['razorpayPaymentId'] = {'S': self.razorpay_payment_id}
        if self.razorpay_signature:
            item['razorpaySignature'] = {'S': self.razorpay_signature}
        if self.payment_method:
            item['paymentMethod'] = {'S': self.payment_method}
        if self.upi_app:
            item['upiApp'] = {'S': self.upi_app}
        if self.order_id:
            item['orderId'] = {'S': self.order_id}
        if self.error_code:
            item['errorCode'] = {'S': self.error_code}
        if self.error_description:
            item['errorDescription'] = {'S': self.error_description}
        if self.refund_amount is not None:
            item['refundAmount'] = {'N': str(self.refund_amount)}
        if self.revenue:
            from utils.dynamodb_helpers import python_to_dynamodb
            item['revenue'] = python_to_dynamodb(self.revenue)  # Store as Map
        
        return item
    
    @staticmethod
    def from_dynamodb_item(item: dict) -> 'Payment':
        """Create Payment from DynamoDB item (accepts createdAt/updatedAt as S or legacy N)."""
        raw_ca = item.get('createdAt', {})
        raw_ca_iso = item.get('createdAtIso', {})
        raw_ua = item.get('updatedAt', {})

        if 'S' in raw_ca_iso:
            created_at = raw_ca_iso.get('S')
        elif 'S' in raw_ca:
            created_at = raw_ca.get('S')
        elif 'N' in raw_ca:
            created_at = int(float(raw_ca['N']))
        else:
            created_at = now_ist_iso()

        updated_at = raw_ua.get('S') if 'S' in raw_ua else (int(float(raw_ua['N'])) if 'N' in raw_ua else created_at)
        return Payment(
            payment_id=item['paymentId']['S'],
            customer_phone=item['customerPhone']['S'],
            restaurant_id=item['restaurantId']['S'],
            restaurant_name=item['restaurantName']['S'],
            amount=float(item['amount']['N']),
            razorpay_order_id=item.get('razorpayOrderId', {}).get('S'),
            razorpay_payment_id=item.get('razorpayPaymentId', {}).get('S'),
            razorpay_signature=item.get('razorpaySignature', {}).get('S'),
            payment_status=item['paymentStatus']['S'],
            payment_method=item.get('paymentMethod', {}).get('S'),
            upi_app=item.get('upiApp', {}).get('S'),
            order_id=item.get('orderId', {}).get('S'),
            error_code=item.get('errorCode', {}).get('S'),
            error_description=item.get('errorDescription', {}).get('S'),
            refund_amount=float(item['refundAmount']['N']) if 'refundAmount' in item and 'N' in item['refundAmount'] else None,
            created_at=created_at,
            updated_at=updated_at
        )
