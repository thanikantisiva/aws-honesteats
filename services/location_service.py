"""Location service for reverse geocoding"""
import json
import urllib.request
import urllib.parse
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from utils.dynamodb import dynamodb_client, TABLES
from utils.ssm import get_secret

logger = Logger()


class LocationService:
    """Service for location operations using Google Maps Geocoding API with DynamoDB caching"""
    
    GOOGLE_MAPS_BASE_URL = 'https://maps.googleapis.com/maps/api/geocode/json'
    CACHE_TTL_DAYS = 30  # Cache for 30 days
    
    @staticmethod
    def _get_from_cache(latitude: float, longitude: float) -> Optional[Dict[str, Any]]:
        """Check if location is cached in DynamoDB AddressesTable"""
        try:
            # Round to 6 decimal places for cache key (~0.1m precision)
            lat_rounded = round(latitude, 6)
            lng_rounded = round(longitude, 6)
            phone = f"LOCATION#{lat_rounded}#{lng_rounded}"  # Use LOCATION# prefix for cache entries
            address_id = "CACHE"
            
            response = dynamodb_client.get_item(
                TableName=TABLES['ADDRESSES'],
                Key={
                    'phone': {'S': phone},
                    'addressId': {'S': address_id}
                }
            )
            
            if 'Item' not in response:
                logger.info(f"❌ Cache miss for {lat_rounded}, {lng_rounded}")
                return None
            
            item = response['Item']
            # Parse cached data
            cached_data = {
                'latitude': float(item.get('lat', {}).get('N', latitude)),
                'longitude': float(item.get('lng', {}).get('N', longitude)),
                'address': item.get('address', {}).get('S', ''),
                'formatted_address': item.get('formatted_address', {}).get('S'),
                'place_id': item.get('place_id', {}).get('S'),
                'components': json.loads(item.get('components', {}).get('S', '{}'))
            }
            logger.info(f"✅ Cache hit for {lat_rounded}, {lng_rounded}")
            return cached_data
        except Exception as e:
            logger.error(f"Error reading from cache: {str(e)}")
            return None
    
    @staticmethod
    def _save_to_cache(latitude: float, longitude: float, data: Dict[str, Any]) -> None:
        """Save location data to DynamoDB AddressesTable as cache"""
        try:
            lat_rounded = round(latitude, 6)
            lng_rounded = round(longitude, 6)
            phone = f"LOCATION#{lat_rounded}#{lng_rounded}"  # Use LOCATION# prefix
            address_id = "CACHE"
            
            # Calculate TTL (30 days from now)
            ttl = int((datetime.utcnow() + timedelta(days=LocationService.CACHE_TTL_DAYS)).timestamp())
            
            item = {
                'phone': {'S': phone},
                'addressId': {'S': address_id},
                'label': {'S': 'CACHE'},
                'lat': {'N': str(latitude)},
                'lng': {'N': str(longitude)},
                'address': {'S': data.get('address', '')},
                'ttl': {'N': str(ttl)}
            }
            
            if data.get('formatted_address'):
                item['formatted_address'] = {'S': data['formatted_address']}
            if data.get('place_id'):
                item['place_id'] = {'S': data['place_id']}
            if data.get('components'):
                item['components'] = {'S': json.dumps(data['components'])}
            
            dynamodb_client.put_item(
                TableName=TABLES['ADDRESSES'],
                Item=item
            )
            
            logger.info(f"💾 Saved to cache: {phone} -> {address_id}")
        except Exception as e:
            logger.error(f"Error saving to cache: {str(e)}")
    
    @staticmethod
    def reverse_geocode(latitude: float, longitude: float) -> Dict[str, Any]:
        """
        Reverse geocode coordinates to address using Google Maps Geocoding API with DynamoDB caching
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            
        Returns:
            Dict containing latitude, longitude, address, and components
        """
        # Check cache first
        cached_data = LocationService._get_from_cache(latitude, longitude)
        if cached_data:
            return cached_data
        
        # Cache miss - call Google Maps API
        try:
            # Build Google Maps API URL
            params = {
                'key': get_secret('GOOGLE_MAPS_API_KEY', ''),
                'latlng': f"{latitude},{longitude}",
                'result_type': 'street_address|route|sublocality|locality|administrative_area_level_2|administrative_area_level_1|country'
            }
            
            url = f"{LocationService.GOOGLE_MAPS_BASE_URL}?{urllib.parse.urlencode(params)}"
            
            logger.info(f"Calling Google Maps for reverse geocoding: lat={latitude}, lon={longitude}")
            
            # Make API request
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'RorkHonestEatsApp/1.0')
            
            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode('utf-8'))
            
            if data.get('status') != 'OK' or not data.get('results'):
                error_msg = data.get('error_message', 'No results found')
                logger.error(f"Google Maps API error: {data.get('status')} - {error_msg}")
                raise Exception(f"Reverse geocoding failed: {error_msg}")
            
            # Get the first result (most relevant)
            result = data['results'][0]
            
            # Extract address components from Google's response
            address_components = {}
            for component in result.get('address_components', []):
                types = component.get('types', [])
                name = component.get('long_name', '')
                
                if 'street_number' in types:
                    address_components['street_number'] = name
                elif 'subpremise' in types:
                    address_components['subpremise'] = name
                elif 'premise' in types:
                    address_components['premise'] = name
                elif 'route' in types:
                    address_components['road'] = name
                elif 'neighborhood' in types:
                    address_components['neighborhood'] = name
                elif 'sublocality' in types or 'sublocality_level_1' in types:
                    address_components['suburb'] = name
                elif 'administrative_area_level_2' in types:
                    address_components['city_district'] = name
                elif 'locality' in types:
                    address_components['city'] = name
                elif 'administrative_area_level_1' in types:
                    address_components['state'] = name
                elif 'postal_code' in types:
                    address_components['postal_code'] = name
                elif 'country' in types:
                    address_components['country'] = name
            
            # Build a concise app display address while keeping Google's full address separately.
            short_address_parts = []
            line1_parts = [
                address_components.get('subpremise'),
                address_components.get('premise'),
                address_components.get('street_number'),
                address_components.get('road'),
            ]
            line1 = ', '.join([part for part in line1_parts if part])
            if line1:
                short_address_parts.append(line1)
            elif address_components.get('neighborhood'):
                short_address_parts.append(address_components['neighborhood'])
            elif address_components.get('suburb'):
                short_address_parts.append(address_components['suburb'])
            elif address_components.get('city_district'):
                short_address_parts.append(address_components['city_district'])

            city_like = (
                address_components.get('city')
                or address_components.get('town')
                or address_components.get('village')
            )
            if city_like:
                short_address_parts.append(city_like)

            short_address = ', '.join([part for part in short_address_parts if part])
            full_address = result.get('formatted_address', '')

            if not short_address:
                short_address = full_address

            logger.info(f"Reverse geocoding successful: short='{short_address}', full='{full_address}'")
            
            response_data = {
                'latitude': latitude,
                'longitude': longitude,
                'address': short_address,
                'components': address_components,
                'formatted_address': full_address,
                'place_id': result.get('place_id')
            }
            
            # Save to cache
            LocationService._save_to_cache(latitude, longitude, response_data)
            
            return response_data
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else 'Unknown error'
            logger.error(f"Google Maps HTTP error: {e.code} - {error_body}")
            raise Exception(f"Reverse geocoding failed: {error_body}")
        except Exception as e:
            logger.error(f"Reverse geocoding error: {str(e)}")
            raise Exception(f"Failed to reverse geocode: {str(e)}")
