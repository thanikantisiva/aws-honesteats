"""Order service"""
from typing import List, Optional
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from models.order import Order
from utils.dynamodb import dynamodb_client, TABLES

logger = Logger()


class OrderService:
    """Service for order operations"""
    
    @staticmethod
    def get_order(order_id: str) -> Optional[Order]:
        """Get order by ID"""
        try:
            logger.info(f"[orderId={order_id}] Fetching order")
            response = dynamodb_client.get_item(
                TableName=TABLES['ORDERS'],
                Key={'orderId': {'S': order_id}}
            )
            
            if 'Item' not in response:
                logger.info(f"[orderId={order_id}] Order not found")
                return None
            
            logger.info(f"[orderId={order_id}] Order loaded")
            return Order.from_dynamodb_item(response['Item'])
        except ClientError as e:
            raise Exception(f"Failed to get order: {str(e)}")
    
    @staticmethod
    def create_order(order: Order) -> Order:
        """Create a new order"""
        try:
            logger.info(f"[orderId={order.order_id}] Creating order")
            dynamodb_client.put_item(
                TableName=TABLES['ORDERS'],
                Item=order.to_dynamodb_item()
            )
            logger.info(f"[orderId={order.order_id}] Order created")
            return order
        except ClientError as e:
            raise Exception(f"Failed to create order: {str(e)}")
    
    @staticmethod
    def list_orders_by_customer(customer_phone: str, status: str = None, limit: int = 20) -> List[Order]:
        """List orders by customer phone using GSI with optional status filter"""
        try:
            logger.info(f"Listing orders for customer={customer_phone} status={status} limit={limit}")
            if status:
                # Use composite key for efficient status filtering
                response = dynamodb_client.query(
                    TableName=TABLES['ORDERS'],
                    IndexName='customer-phone-statusCreatedAt-index',
                    KeyConditionExpression='customerPhone = :phone AND begins_with(customerStatusCreatedAt, :statusPrefix)',
                    ExpressionAttributeValues={
                        ':phone': {'S': customer_phone},
                        ':statusPrefix': {'S': f'{status}#'}
                    },
                    ScanIndexForward=False,  # Most recent first
                    Limit=limit
                )
            else:
                # Fetch all statuses
                response = dynamodb_client.query(
                    TableName=TABLES['ORDERS'],
                    IndexName='customer-phone-statusCreatedAt-index',
                    KeyConditionExpression='customerPhone = :phone',
                    ExpressionAttributeValues={
                        ':phone': {'S': customer_phone}
                    },
                    ScanIndexForward=False,  # Most recent first
                    Limit=limit
                )
            
            orders = []
            for item in response.get('Items', []):
                status_val = item.get("status", {}).get("S")
                if status_val == Order.STATUS_INITIATED:
                    continue
                orders.append(Order.from_dynamodb_item(item))
            
            logger.info(f"Listed {len(orders)} orders for customer={customer_phone}")
            return orders
        except ClientError as e:
            raise Exception(f"Failed to list customer orders: {str(e)}")
    
    @staticmethod
    def list_orders_by_restaurant(restaurant_id: str, status: str = None, limit: int = 20) -> List[Order]:
        """List orders by restaurant ID using GSI with optional status filter"""
        try:
            logger.info(f"Listing orders for restaurant={restaurant_id} status={status} limit={limit}")
            if status:
                # Use composite key for efficient status filtering
                response = dynamodb_client.query(
                    TableName=TABLES['ORDERS'],
                    IndexName='restaurantId-statusCreatedAt-index',
                    KeyConditionExpression='restaurantId = :restaurantId AND begins_with(restaurantStatusCreatedAt, :statusPrefix)',
                    ExpressionAttributeValues={
                        ':restaurantId': {'S': restaurant_id},
                        ':statusPrefix': {'S': f'{status}#'}
                    },
                    ScanIndexForward=False,  # Most recent first
                    Limit=limit
                )
            else:
                # Fetch all statuses
                response = dynamodb_client.query(
                    TableName=TABLES['ORDERS'],
                    IndexName='restaurantId-statusCreatedAt-index',
                    KeyConditionExpression='restaurantId = :restaurantId',
                    ExpressionAttributeValues={
                        ':restaurantId': {'S': restaurant_id}
                    },
                    ScanIndexForward=False,  # Most recent first
                    Limit=limit
                )
            
            orders = []
            for item in response.get('Items', []):
                orders.append(Order.from_dynamodb_item(item))
            
            logger.info(f"Listed {len(orders)} orders for restaurant={restaurant_id}")
            return orders
        except ClientError as e:
            raise Exception(f"Failed to list restaurant orders: {str(e)}")
    
    @staticmethod
    def list_orders_by_rider(rider_id: str, status: str = None, limit: int = 20) -> List[Order]:
        """List orders by rider ID using GSI with optional status filter"""
        try:
            logger.info(f"Listing orders for rider={rider_id} status={status} limit={limit}")
            if status:
                # Use composite key for efficient status filtering
                response = dynamodb_client.query(
                    TableName=TABLES['ORDERS'],
                    IndexName='riderId-statusCreatedAt-index',
                    KeyConditionExpression='riderId = :riderId AND begins_with(riderStatusCreatedAt, :statusPrefix)',
                    ExpressionAttributeValues={
                        ':riderId': {'S': rider_id},
                        ':statusPrefix': {'S': f'{status}#'}
                    },
                    ScanIndexForward=False,  # Most recent first
                    Limit=limit
                )
            else:
                # Fetch all statuses
                response = dynamodb_client.query(
                    TableName=TABLES['ORDERS'],
                    IndexName='riderId-statusCreatedAt-index',
                    KeyConditionExpression='riderId = :riderId',
                    ExpressionAttributeValues={
                        ':riderId': {'S': rider_id}
                    },
                    ScanIndexForward=False,  # Most recent first
                    Limit=limit
                )
            
            orders = []
            for item in response.get('Items', []):
                orders.append(Order.from_dynamodb_item(item))
            
            logger.info(f"Listed {len(orders)} orders for rider={rider_id}")
            return orders
        except ClientError as e:
            raise Exception(f"Failed to list rider orders: {str(e)}")
    
    @staticmethod
    def get_orders_by_rider(rider_id: str, status: str = None, limit: int = 20) -> List[Order]:
        """Alias for list_orders_by_rider for backward compatibility"""
        return OrderService.list_orders_by_rider(rider_id, status, limit)
    
    @staticmethod
    def update_order(order_id: str, updates: dict) -> Order:
        """Update order with arbitrary fields and update composite keys if status/riderId changes"""
        try:
            from utils.dynamodb_helpers import python_to_dynamodb
            # Get current order to access createdAt for composite keys
            order = OrderService.get_order(order_id)
            if not order:
                raise Exception("Order not found")
            
            logger.info(f"[orderId={order_id}] Updating order fields: {list(updates.keys())}")
            
            # Build update expression
            set_expr_parts = []
            remove_expr_parts = []
            expr_attr_names = {}
            expr_attr_values = {}
            
            # Track if we need to update composite keys
            status_changed = 'status' in updates
            rider_changed = 'riderId' in updates
            new_status = updates.get('status', order.status)
            new_rider_id = updates.get('riderId', order.rider_id)
            
            for key, value in updates.items():
                attr_name = f"#{key}"
                attr_value = f":{key}"
                expr_attr_names[attr_name] = key
                
                # Handle different value types
                if isinstance(value, bool):
                    set_expr_parts.append(f"{attr_name} = {attr_value}")
                    expr_attr_values[attr_value] = {'BOOL': value}
                elif isinstance(value, (int, float)):
                    set_expr_parts.append(f"{attr_name} = {attr_value}")
                    expr_attr_values[attr_value] = {'N': str(value)}
                elif isinstance(value, (list, dict)):
                    set_expr_parts.append(f"{attr_name} = {attr_value}")
                    expr_attr_values[attr_value] = python_to_dynamodb(value)
                elif value is None:
                    # Remove attribute when value is None
                    remove_expr_parts.append(attr_name)
                    continue
                else:
                    set_expr_parts.append(f"{attr_name} = {attr_value}")
                    expr_attr_values[attr_value] = {'S': str(value)}
            
            # Log status change
            if status_changed and order.status != new_status:
                logger.info(f"[orderId={order_id}] Status change: {order.status} → {new_status}")

            # Update composite keys if status or riderId changed
            if status_changed or rider_changed:
                created_at = order.created_at
                
                if status_changed:
                    # Update all composite keys with new status
                    set_expr_parts.append("customerStatusCreatedAt = :csc")
                    set_expr_parts.append("restaurantStatusCreatedAt = :rsc")
                    expr_attr_values[':csc'] = {'S': f'{new_status}#{created_at}'}
                    expr_attr_values[':rsc'] = {'S': f'{new_status}#{created_at}'}
                    
                    if new_rider_id:
                        set_expr_parts.append("riderStatusCreatedAt = :risc")
                        expr_attr_values[':risc'] = {'S': f'{new_status}#{created_at}'}
                
                elif rider_changed and new_rider_id:
                    # Rider assigned - add riderStatusCreatedAt
                    set_expr_parts.append("riderStatusCreatedAt = :risc")
                    expr_attr_values[':risc'] = {'S': f'{new_status}#{created_at}'}

            update_expr_parts = []
            if set_expr_parts:
                update_expr_parts.append("SET " + ", ".join(set_expr_parts))
            if remove_expr_parts:
                update_expr_parts.append("REMOVE " + ", ".join(remove_expr_parts))
            update_expr = " ".join(update_expr_parts)
            
            dynamodb_client.update_item(
                TableName=TABLES['ORDERS'],
                Key={'orderId': {'S': order_id}},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values
            )
            
            logger.info(f"[orderId={order_id}] Order update complete")
            return OrderService.get_order(order_id)
        except ClientError as e:
            raise Exception(f"Failed to update order: {str(e)}")
    
    @staticmethod
    def update_order_status(order_id: str, status: str, rider_id: Optional[str] = None) -> Order:
        """Update order status and regenerate composite keys"""
        try:
            logger.info(f"[orderId={order_id}] update_order_status called status={status} riderId={rider_id}")
            # First get the order to get createdAt and other details
            order = OrderService.get_order(order_id)
            if not order:
                raise Exception("Order not found")

            if order.status != status:
                logger.info(f"[orderId={order_id}] Status change: {order.status} → {status}")
            
            created_at = order.created_at
            
            # Build update expression to update both status AND composite keys
            update_expressions = ['#status = :status']
            expression_attribute_names = {'#status': 'status'}
            expression_attribute_values = {':status': {'S': status}}
            
            # Update composite keys with new status
            update_expressions.append('customerStatusCreatedAt = :csc')
            update_expressions.append('restaurantStatusCreatedAt = :rsc')
            expression_attribute_values[':csc'] = {'S': f'{status}#{created_at}'}
            expression_attribute_values[':rsc'] = {'S': f'{status}#{created_at}'}
            
            if rider_id:
                update_expressions.append('#riderId = :riderId')
                update_expressions.append('riderStatusCreatedAt = :risc')
                expression_attribute_names['#riderId'] = 'riderId'
                expression_attribute_values[':riderId'] = {'S': rider_id}
                expression_attribute_values[':risc'] = {'S': f'{status}#{created_at}'}
            elif order.rider_id:
                # Update composite key for existing rider
                update_expressions.append('riderStatusCreatedAt = :risc')
                expression_attribute_values[':risc'] = {'S': f'{status}#{created_at}'}
            
            dynamodb_client.update_item(
                TableName=TABLES['ORDERS'],
                Key={'orderId': {'S': order_id}},
                UpdateExpression=f"SET {', '.join(update_expressions)}",
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            
            logger.info(f"[orderId={order_id}] Status update complete")
            return OrderService.get_order(order_id)
        except ClientError as e:
            raise Exception(f"Failed to update order status: {str(e)}")
