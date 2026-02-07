"""User service"""
from typing import Optional, List
from botocore.exceptions import ClientError
from models.user import User
from utils.dynamodb import dynamodb_client, TABLES


class UserService:
    """Service for user operations"""
    
    @staticmethod
    def get_user_by_role(phone: str, role: str) -> Optional[User]:
        """Get user by phone number and specific role (CUSTOMER or RIDER)"""
        try:
            response = dynamodb_client.get_item(
                TableName=TABLES['USERS'],
                Key={
                    'phone': {'S': phone},
                    'role': {'S': role}
                }
            )
            
            if 'Item' not in response:
                return None
            
            return User.from_dynamodb_item(response['Item'])
        except ClientError as e:
            raise Exception(f"Failed to get user: {str(e)}")
    
    @staticmethod
    def get_user(phone: str) -> Optional[User]:
        """Get user by phone number - returns first role found (for backward compatibility)"""
        try:
            # Query all roles for this phone
            response = dynamodb_client.query(
                TableName=TABLES['USERS'],
                KeyConditionExpression='phone = :phone',
                ExpressionAttributeValues={
                    ':phone': {'S': phone}
                }
            )
            
            if not response.get('Items'):
                return None
            
            # Return first role found
            return User.from_dynamodb_item(response['Items'][0])
        except ClientError as e:
            raise Exception(f"Failed to get user: {str(e)}")
    
    @staticmethod
    def get_all_user_roles(phone: str) -> List[User]:
        """Get all roles for a phone number (may return CUSTOMER and/or RIDER)"""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['USERS'],
                KeyConditionExpression='phone = :phone',
                ExpressionAttributeValues={
                    ':phone': {'S': phone}
                }
            )
            
            users = []
            for item in response.get('Items', []):
                users.append(User.from_dynamodb_item(item))
            
            return users
        except ClientError as e:
            raise Exception(f"Failed to get user roles: {str(e)}")
    
    @staticmethod
    def create_user(user: User) -> User:
        """Create a new user"""
        try:
            dynamodb_client.put_item(
                TableName=TABLES['USERS'],
                Item=user.to_dynamodb_item()
            )
            return user
        except ClientError as e:
            raise Exception(f"Failed to create user: {str(e)}")
    
    @staticmethod
    def update_user(phone: str, role: str, updates: dict) -> User:
        """Update user information (requires role for composite key)"""
        try:
            update_expressions = []
            expression_attribute_names = {}
            expression_attribute_values = {}
            
            if 'name' in updates:
                update_expressions.append('#name = :name')
                expression_attribute_names['#name'] = 'name'
                expression_attribute_values[':name'] = {'S': updates['name']}
            
            if 'email' in updates:
                update_expressions.append('#email = :email')
                expression_attribute_names['#email'] = 'email'
                expression_attribute_values[':email'] = {'S': updates['email']}
            
            if 'isActive' in updates:
                update_expressions.append('#isActive = :isActive')
                expression_attribute_names['#isActive'] = 'isActive'
                expression_attribute_values[':isActive'] = {'BOOL': updates['isActive']}
            
            if 'dateOfBirth' in updates:
                update_expressions.append('#dateOfBirth = :dateOfBirth')
                expression_attribute_names['#dateOfBirth'] = 'dateOfBirth'
                expression_attribute_values[':dateOfBirth'] = {'S': updates['dateOfBirth']}
            
            if 'fcmToken' in updates:
                update_expressions.append('#fcmToken = :fcmToken')
                expression_attribute_names['#fcmToken'] = 'fcmToken'
                expression_attribute_values[':fcmToken'] = {'S': updates['fcmToken']}
            
            if 'fcmTokenUpdatedAt' in updates:
                update_expressions.append('#fcmTokenUpdatedAt = :fcmTokenUpdatedAt')
                expression_attribute_names['#fcmTokenUpdatedAt'] = 'fcmTokenUpdatedAt'
                expression_attribute_values[':fcmTokenUpdatedAt'] = {'S': updates['fcmTokenUpdatedAt']}
            
            # Rider-specific fields
            if 'riderStatus' in updates:
                update_expressions.append('#riderStatus = :riderStatus')
                expression_attribute_names['#riderStatus'] = 'riderStatus'
                expression_attribute_values[':riderStatus'] = {'S': updates['riderStatus']}
            
            if 'rejectionReason' in updates:
                update_expressions.append('#rejectionReason = :rejectionReason')
                expression_attribute_names['#rejectionReason'] = 'rejectionReason'
                expression_attribute_values[':rejectionReason'] = {'S': updates['rejectionReason']}
            
            if 'approvedAt' in updates:
                update_expressions.append('#approvedAt = :approvedAt')
                expression_attribute_names['#approvedAt'] = 'approvedAt'
                expression_attribute_values[':approvedAt'] = {'S': updates['approvedAt']}
            
            if not update_expressions:
                return UserService.get_user_by_role(phone, role)
            
            dynamodb_client.update_item(
                TableName=TABLES['USERS'],
                Key={
                    'phone': {'S': phone},
                    'role': {'S': role}
                },
                UpdateExpression=f"SET {', '.join(update_expressions)}",
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            
            return UserService.get_user_by_role(phone, role)
        except ClientError as e:
            raise Exception(f"Failed to update user: {str(e)}")
    
    @staticmethod
    def get_rider_by_phone(phone: str) -> Optional[User]:
        """Get rider by phone (must have role=RIDER)"""
        return UserService.get_user_by_role(phone, "RIDER")
    
    @staticmethod
    def list_riders_by_status(status: str) -> list:
        """List all riders with specific status"""
        try:
            response = dynamodb_client.scan(
                TableName=TABLES['USERS'],
                FilterExpression='#role = :role AND #riderStatus = :status',
                ExpressionAttributeNames={
                    '#role': 'role',
                    '#riderStatus': 'riderStatus'
                },
                ExpressionAttributeValues={
                    ':role': {'S': 'RIDER'},
                    ':status': {'S': status}
                }
            )
            
            riders = []
            for item in response.get('Items', []):
                riders.append(User.from_dynamodb_item(item))
            
            return riders
        except ClientError as e:
            raise Exception(f"Failed to list riders: {str(e)}")

    @staticmethod
    def get_rider_by_aadhar(aadhar_number: str) -> Optional[User]:
        """Get rider by Aadhar number (role=RIDER)"""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['USERS'],
                IndexName='aadharNumber-index',
                KeyConditionExpression='aadharNumber = :aadhar',
                FilterExpression='#role = :role',
                ExpressionAttributeNames={
                    '#role': 'role'
                },
                ExpressionAttributeValues={
                    ':aadhar': {'S': aadhar_number},
                    ':role': {'S': 'RIDER'}
                }
            )

            items = response.get('Items', [])
            if not items:
                return None

            return User.from_dynamodb_item(items[0])
        except ClientError as e:
            raise Exception(f"Failed to get rider by aadhar: {str(e)}")

    @staticmethod
    def get_rider_by_pan(pan_number: str) -> Optional[User]:
        """Get rider by PAN number (role=RIDER)"""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['USERS'],
                IndexName='panNumber-index',
                KeyConditionExpression='panNumber = :pan',
                FilterExpression='#role = :role',
                ExpressionAttributeNames={
                    '#role': 'role'
                },
                ExpressionAttributeValues={
                    ':pan': {'S': pan_number},
                    ':role': {'S': 'RIDER'}
                }
            )

            items = response.get('Items', [])
            if not items:
                return None

            return User.from_dynamodb_item(items[0])
        except ClientError as e:
            raise Exception(f"Failed to get rider by pan: {str(e)}")

    @staticmethod
    def get_rider_by_rider_id(rider_id: str) -> Optional[User]:
        """Get rider by rider_id using GSI (role=RIDER)"""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['USERS'],
                IndexName='riderId-index',
                KeyConditionExpression='riderId = :riderId',
                FilterExpression='#role = :role',
                ExpressionAttributeNames={
                    '#role': 'role'
                },
                ExpressionAttributeValues={
                    ':riderId': {'S': rider_id},
                    ':role': {'S': 'RIDER'}
                }
            )

            items = response.get('Items', [])
            if not items:
                return None

            return User.from_dynamodb_item(items[0])
        except ClientError as e:
            raise Exception(f"Failed to get rider by riderId: {str(e)}")
