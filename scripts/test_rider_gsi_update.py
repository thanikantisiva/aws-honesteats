#!/usr/bin/env python3
"""
Test script to verify rider GSI updates when going online
"""
import os
import sys
import boto3
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.geohash import encode as geohash_encode
from utils.dynamodb import dynamodb_client

# Configuration
ENVIRONMENT = 'dev'
RIDERS_TABLE = f'food-delivery-riders-{ENVIRONMENT}'

# Test data
TEST_RIDER_ID = 'RDR_TEST_001'
TEST_PHONE = '+919999999999'
TEST_LAT = 12.9716
TEST_LNG = 77.5946


def create_test_rider():
    """Create a test rider"""
    print(f"\n1️⃣ Creating test rider: {TEST_RIDER_ID}")
    
    item = {
        'riderId': {'S': TEST_RIDER_ID},
        'phone': {'S': TEST_PHONE},
        'isActive': {'BOOL': False},
        'timestamp': {'S': datetime.utcnow().isoformat()},
        'lastSeen': {'S': datetime.utcnow().isoformat()}
    }
    
    try:
        dynamodb_client.put_item(
            TableName=RIDERS_TABLE,
            Item=item
        )
        print(f"   ✅ Test rider created (offline, no location)")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    
    return True


def go_online_with_location():
    """Simulate rider going online - should update GSI fields"""
    print(f"\n2️⃣ Simulating rider going ONLINE with location ({TEST_LAT}, {TEST_LNG})")
    
    # Calculate geohash. GSI3 partitions on the 2-char prefix.
    geohash_p7 = geohash_encode(TEST_LAT, TEST_LNG, precision=7)
    geohash_p2 = geohash_p7[:2]

    print(f"   📍 Geohash P7: {geohash_p7}")
    print(f"   📍 Geohash P2: {geohash_p2}")
    
    try:
        timestamp = datetime.utcnow().isoformat()
        
        dynamodb_client.update_item(
            TableName=RIDERS_TABLE,
            Key={'riderId': {'S': TEST_RIDER_ID}},
            UpdateExpression='SET isActive = :active, lastSeen = :lastSeen, lat = :lat, lng = :lng, geohash = :geohash, GSI3PK = :gsi3pk, GSI3SK = :gsi3sk',
            ExpressionAttributeValues={
                ':active': {'BOOL': True},
                ':lastSeen': {'S': timestamp},
                ':lat': {'N': str(TEST_LAT)},
                ':lng': {'N': str(TEST_LNG)},
                ':geohash': {'S': geohash_p7},
                ':gsi3pk': {'S': geohash_p2},
                ':gsi3sk': {'S': f'RIDER#{TEST_RIDER_ID}'}
            }
        )
        
        print(f"   ✅ Rider status updated to ONLINE")
        print(f"   ✅ GSI fields updated")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    
    return True


def verify_gsi_fields():
    """Verify GSI fields were correctly set"""
    print(f"\n3️⃣ Verifying GSI fields in DynamoDB")
    
    try:
        response = dynamodb_client.get_item(
            TableName=RIDERS_TABLE,
            Key={'riderId': {'S': TEST_RIDER_ID}}
        )
        
        if 'Item' not in response:
            print(f"   ❌ Rider not found")
            return False
        
        item = response['Item']
        
        # Check all required fields
        checks = {
            'isActive': item.get('isActive', {}).get('BOOL'),
            'lat': item.get('lat', {}).get('N'),
            'lng': item.get('lng', {}).get('N'),
            'geohash': item.get('geohash', {}).get('S'),
            'GSI3PK': item.get('GSI3PK', {}).get('S'),
            'GSI3SK': item.get('GSI3SK', {}).get('S')
        }
        
        print(f"\n   Field Values:")
        all_ok = True
        for field, value in checks.items():
            status = "✅" if value else "❌"
            print(f"   {status} {field}: {value}")
            if not value:
                all_ok = False
        
        return all_ok
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_gsi_query():
    """Test querying by GSI3 (the only spatial GSI on the riders table)"""
    print(f"\n4️⃣ Testing GSI queries")

    geohash_p2 = geohash_encode(TEST_LAT, TEST_LNG, precision=2)
    print(f"   Querying GSI3 for geohash: {geohash_p2}")

    try:
        response = dynamodb_client.query(
            TableName=RIDERS_TABLE,
            IndexName='GSI3',
            KeyConditionExpression='GSI3PK = :pk',
            ExpressionAttributeValues={
                ':pk': {'S': geohash_p2}
            }
        )

        count = len(response.get('Items', []))
        print(f"   ✅ Found {count} rider(s) in GSI3 query")

        found_test_rider = any(
            item.get('riderId', {}).get('S') == TEST_RIDER_ID
            for item in response.get('Items', [])
        )

        if found_test_rider:
            print(f"   ✅ Test rider found in GSI3 query results")
        else:
            print(f"   ❌ Test rider NOT found in GSI3 query results")

        return found_test_rider

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def cleanup_test_rider():
    """Clean up test rider"""
    print(f"\n5️⃣ Cleaning up test rider")
    
    try:
        dynamodb_client.delete_item(
            TableName=RIDERS_TABLE,
            Key={'riderId': {'S': TEST_RIDER_ID}}
        )
        print(f"   ✅ Test rider deleted")
    except Exception as e:
        print(f"   ⚠️ Cleanup error (may not exist): {e}")


def main():
    """Run all tests"""
    print("=" * 60)
    print("🧪 Testing Rider GSI Updates")
    print("=" * 60)
    
    # Clean up any existing test rider
    cleanup_test_rider()
    
    # Run tests
    success = True
    success = success and create_test_rider()
    success = success and go_online_with_location()
    success = success and verify_gsi_fields()
    success = success and test_gsi_query()
    
    # Cleanup
    cleanup_test_rider()
    
    # Summary
    print("\n" + "=" * 60)
    if success:
        print("✅ ALL TESTS PASSED")
        print("✅ GSI fields are correctly updated when rider goes online")
    else:
        print("❌ SOME TESTS FAILED")
        print("❌ Check the errors above")
    print("=" * 60)
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
