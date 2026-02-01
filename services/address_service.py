"""Address service"""
from typing import List, Optional
from botocore.exceptions import ClientError
from models.address import Address
from utils.dynamodb import dynamodb_client, TABLES


class AddressService:
    """Service for address operations"""
    
    @staticmethod
    def get_address(phone: str, address_id: str) -> Optional[Address]:
        """Get address by phone and address ID"""
        try:
            response = dynamodb_client.get_item(
                TableName=TABLES['ADDRESSES'],
                Key={
                    'phone': {'S': phone},
                    'addressId': {'S': address_id}
                }
            )
            
            if 'Item' not in response:
                return None
            
            return Address.from_dynamodb_item(response['Item'])
        except ClientError as e:
            raise Exception(f"Failed to get address: {str(e)}")
    
    @staticmethod
    def create_address(address: Address) -> Address:
        """Create a new address"""
        try:
            dynamodb_client.put_item(
                TableName=TABLES['ADDRESSES'],
                Item=address.to_dynamodb_item()
            )
            return address
        except ClientError as e:
            raise Exception(f"Failed to create address: {str(e)}")
    
    @staticmethod
    def list_addresses(phone: str) -> List[Address]:
        """List all addresses for a customer"""
        try:
            response = dynamodb_client.query(
                TableName=TABLES['ADDRESSES'],
                KeyConditionExpression='phone = :phone',
                ExpressionAttributeValues={
                    ':phone': {'S': phone}
                }
            )
            
            addresses = []
            for item in response.get('Items', []):
                addresses.append(Address.from_dynamodb_item(item))
            
            return addresses
        except ClientError as e:
            raise Exception(f"Failed to list addresses: {str(e)}")
    
    @staticmethod
    def update_address(phone: str, address_id: str, updates: dict) -> Address:
        """Update address"""
        try:
            update_expressions = []
            expression_attribute_names = {}
            expression_attribute_values = {}
            
            if 'label' in updates:
                update_expressions.append('#label = :label')
                expression_attribute_names['#label'] = 'label'
                expression_attribute_values[':label'] = {'S': updates['label']}
            
            if 'address' in updates:
                update_expressions.append('#address = :address')
                expression_attribute_names['#address'] = 'address'
                expression_attribute_values[':address'] = {'S': updates['address']}
            
            if 'lat' in updates:
                update_expressions.append('#lat = :lat')
                expression_attribute_names['#lat'] = 'lat'
                expression_attribute_values[':lat'] = {'N': str(updates['lat'])}
            
            if 'lng' in updates:
                update_expressions.append('#lng = :lng')
                expression_attribute_names['#lng'] = 'lng'
                expression_attribute_values[':lng'] = {'N': str(updates['lng'])}
            
            if 'geocodedAddress' in updates:
                update_expressions.append('#geocodedAddress = :geocodedAddress')
                expression_attribute_names['#geocodedAddress'] = 'geocodedAddress'
                expression_attribute_values[':geocodedAddress'] = {'S': updates['geocodedAddress']}
            
            if 'formattedAddress' in updates:
                update_expressions.append('#formattedAddress = :formattedAddress')
                expression_attribute_names['#formattedAddress'] = 'formattedAddress'
                expression_attribute_values[':formattedAddress'] = {'S': updates['formattedAddress']}
            
            if 'placeId' in updates:
                update_expressions.append('#placeId = :placeId')
                expression_attribute_names['#placeId'] = 'placeId'
                expression_attribute_values[':placeId'] = {'S': updates['placeId']}
            
            if 'components' in updates:
                import json
                update_expressions.append('#components = :components')
                expression_attribute_names['#components'] = 'components'
                expression_attribute_values[':components'] = {'S': json.dumps(updates['components'])}
            
            if not update_expressions:
                return AddressService.get_address(phone, address_id)
            
            dynamodb_client.update_item(
                TableName=TABLES['ADDRESSES'],
                Key={
                    'phone': {'S': phone},
                    'addressId': {'S': address_id}
                },
                UpdateExpression=f"SET {', '.join(update_expressions)}",
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            
            return AddressService.get_address(phone, address_id)
        except ClientError as e:
            raise Exception(f"Failed to update address: {str(e)}")
    
    @staticmethod
    def delete_address(phone: str, address_id: str) -> None:
        """Delete an address"""
        try:
            dynamodb_client.delete_item(
                TableName=TABLES['ADDRESSES'],
                Key={
                    'phone': {'S': phone},
                    'addressId': {'S': address_id}
                }
            )
        except ClientError as e:
            raise Exception(f"Failed to delete address: {str(e)}")

