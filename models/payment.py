"""Payment model for Razorpay transactions"""
import time
from typing import List, Optional, Dict, Any


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
        revenue: Optional[dict] = None,
        created_at: Optional[int] = None,
        updated_at: Optional[int] = None
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
        self.revenue = revenue  # Revenue breakdown for analytics
        self.created_at = created_at or int(time.time() * 1000)
        self.updated_at = updated_at or self.created_at
    
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
            'revenue': self.revenue,
            'createdAt': self.created_at,
            'updatedAt': self.updated_at
        }
    
    def to_dynamodb_item(self) -> dict:
        """Convert to DynamoDB item format"""
        item = {
            'paymentId': {'S': self.payment_id},
            'customerPhone': {'S': self.customer_phone},
            'restaurantId': {'S': self.restaurant_id},
            'restaurantName': {'S': self.restaurant_name},
            'amount': {'N': str(self.amount)},
            'paymentStatus': {'S': self.payment_status},
            'createdAt': {'N': str(self.created_at)},
            'updatedAt': {'N': str(self.updated_at)}
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
        if self.revenue:
            from utils.dynamodb_helpers import python_to_dynamodb
            item['revenue'] = python_to_dynamodb(self.revenue)  # Store as Map
        
        return item
    
    @staticmethod
    def from_dynamodb_item(item: dict) -> 'Payment':
        """Create Payment from DynamoDB item"""
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
            created_at=int(item['createdAt']['N']),
            updated_at=int(item['updatedAt']['N'])
        )

