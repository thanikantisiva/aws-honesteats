#!/usr/bin/env python3
"""
Test script to verify automatic order assignment when status changes to READY_FOR_PICKUP
"""
import os
import sys
import boto3
import time
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.geohash import encode as geohash_encode
from utils.dynamodb import dynamodb_client

# Configuration
ENVIRONMENT = 'dev'
ORDERS_TABLE = f'food-delivery-orders-{ENVIRONMENT}'
RIDERS_TABLE = f'food-delivery-riders-{ENVIRONMENT}'
RESTAURANTS_TABLE = f'food-delivery-restaurants-{ENVIRONMENT}'

# Test data
TEST_ORDER_ID = 'ORD_TEST_ASSIGNMENT'
TEST_RIDER_ID = 'RDR_TEST_002'
TEST_RESTAURANT_ID = 'REST_TEST_001'
TEST_RIDER_PHONE = '+919999999998'

# Bangalore coordinates
RESTAURANT_LAT = 12.9716
RESTAURANT_LNG = 77.5946


def cleanup_existing_test_data():
    """Clean up any existing test data"""
    print("\n🧹 Cleaning up existing test data...")
    
    try:
        # Delete test order
        dynamodb_client.delete_item(
            TableName=ORDERS_TABLE,
            Key={'orderId': {'S': TEST_ORDER_ID}}
        )
        print("   ✅ Deleted test order (if existed)")
    except:
        pass
    
    try:
        # Delete test rider
        dynamodb_client.delete_item(
            TableName=RIDERS_TABLE,
            Key={'riderId': {'S': TEST_RIDER_ID}}
        )
        print("   ✅ Deleted test rider (if existed)")
    except:
        pass


def create_test_rider():
    """Create a test rider near the restaurant"""
    print(f"\n1️⃣ Creating test rider: {TEST_RIDER_ID}")
    
    # Place rider 500m away from restaurant
    rider_lat = RESTAURANT_LAT + 0.0045  # ~500m north
    rider_lng = RESTAURANT_LNG
    
    geohash_p7 = geohash_encode(rider_lat, rider_lng, 7)
    geohash_p6 = geohash_p7[:6]
    geohash_p5 = geohash_p7[:5]
    geohash_p4 = geohash_p7[:4]
    
    print(f"   📍 Rider location: ({rider_lat}, {rider_lng})")
    print(f"   📍 Geohash: {geohash_p7}")
    
    item = {
        'riderId': {'S': TEST_RIDER_ID},
        'phone': {'S': TEST_RIDER_PHONE},
        'isActive': {'BOOL': True},  # ONLINE
        'lat': {'N': str(rider_lat)},
        'lng': {'N': str(rider_lng)},
        'speed': {'N': '0'},
        'heading': {'N': '0'},
        'geohash': {'S': geohash_p7},
        'GSI1PK': {'S': geohash_p6},
        'GSI1SK': {'S': f'RIDER#{TEST_RIDER_ID}'},
        'GSI2PK': {'S': geohash_p5},
        'GSI2SK': {'S': f'RIDER#{TEST_RIDER_ID}'},
        'GSI3PK': {'S': geohash_p4},
        'GSI3SK': {'S': f'RIDER#{TEST_RIDER_ID}'},
        'timestamp': {'S': datetime.utcnow().isoformat()},
        'lastSeen': {'S': datetime.utcnow().isoformat()}
    }
    
    try:
        dynamodb_client.put_item(
            TableName=RIDERS_TABLE,
            Item=item
        )
        print(f"   ✅ Test rider created (ONLINE, ~500m from restaurant)")
        return True
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def create_test_order():
    """Create a test order in PREPARING status"""
    print(f"\n2️⃣ Creating test order: {TEST_ORDER_ID}")
    
    item = {
        'orderId': {'S': TEST_ORDER_ID},
        'customerPhone': {'S': '+919999999999'},
        'restaurantId': {'S': TEST_RESTAURANT_ID},
        'items': {'L': []},
        'foodTotal': {'N': '250'},
        'deliveryFee': {'N': '40'},
        'platformFee': {'N': '10'},
        'grandTotal': {'N': '300'},
        'status': {'S': 'PREPARING'},
        'restaurantName': {'S': 'Test Restaurant'},
        'pickupLat': {'N': str(RESTAURANT_LAT)},
        'pickupLng': {'N': str(RESTAURANT_LNG)},
        'createdAt': {'N': str(int(time.time() * 1000))}
    }
    
    try:
        dynamodb_client.put_item(
            TableName=ORDERS_TABLE,
            Item=item
        )
        print(f"   ✅ Test order created (status: PREPARING)")
        return True
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def update_order_to_ready():
    """Update order status to READY_FOR_PICKUP to trigger assignment"""
    print(f"\n3️⃣ Updating order status to READY_FOR_PICKUP")
    print(f"   ⏳ This should trigger the OrderAssignmentHandler Lambda...")
    
    try:
        dynamodb_client.update_item(
            TableName=ORDERS_TABLE,
            Key={'orderId': {'S': TEST_ORDER_ID}},
            UpdateExpression='SET #status = :status',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': {'S': 'READY_FOR_PICKUP'}}
        )
        print(f"   ✅ Order status updated to READY_FOR_PICKUP")
        return True
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def wait_for_assignment(max_wait_seconds=15):
    """Wait for Lambda to process and assign rider"""
    print(f"\n4️⃣ Waiting for automatic assignment (max {max_wait_seconds}s)...")
    
    for i in range(max_wait_seconds):
        try:
            response = dynamodb_client.get_item(
                TableName=ORDERS_TABLE,
                Key={'orderId': {'S': TEST_ORDER_ID}}
            )
            
            if 'Item' not in response:
                print(f"   ❌ Order not found")
                return False
            
            order = response['Item']
            rider_id = order.get('riderId', {}).get('S')
            status = order.get('status', {}).get('S')
            
            if rider_id:
                print(f"   ✅ Rider assigned: {rider_id}")
                print(f"   ✅ Order status: {status}")
                return True
            
            if i < max_wait_seconds - 1:
                print(f"   ⏳ Waiting... ({i+1}s)", end='\r')
                time.sleep(1)
        
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    print(f"\n   ❌ Timeout: Order not assigned after {max_wait_seconds}s")
    return False


def verify_assignment():
    """Verify order and rider were updated correctly"""
    print(f"\n5️⃣ Verifying assignment details...")
    
    try:
        # Check order
        order_response = dynamodb_client.get_item(
            TableName=ORDERS_TABLE,
            Key={'orderId': {'S': TEST_ORDER_ID}}
        )
        
        if 'Item' not in order_response:
            print(f"   ❌ Order not found")
            return False
        
        order = order_response['Item']
        rider_id = order.get('riderId', {}).get('S')
        status = order.get('status', {}).get('S')
        delivery_otp = order.get('deliveryOtp', {}).get('S')
        rider_assigned_at = order.get('riderAssignedAt', {}).get('S')
        
        print(f"\n   Order Details:")
        print(f"   ✅ riderId: {rider_id}")
        print(f"   ✅ status: {status}")
        print(f"   ✅ deliveryOtp: {delivery_otp}")
        print(f"   ✅ riderAssignedAt: {rider_assigned_at}")
        
        # Check rider
        rider_response = dynamodb_client.get_item(
            TableName=RIDERS_TABLE,
            Key={'riderId': {'S': rider_id or TEST_RIDER_ID}}
        )
        
        if 'Item' not in rider_response:
            print(f"   ❌ Rider not found")
            return False
        
        rider = rider_response['Item']
        working_on_order = [v.get('S') for v in rider.get('workingOnOrder', {}).get('L', [])]
        
        print(f"\n   Rider Details:")
        print(f"   ✅ workingOnOrder: {working_on_order}")
        
        # Validate
        all_ok = True
        if rider_id != TEST_RIDER_ID:
            print(f"   ❌ Expected rider {TEST_RIDER_ID}, got {rider_id}")
            all_ok = False
        
        if status != 'RIDER_ASSIGNED':
            print(f"   ❌ Expected status RIDER_ASSIGNED, got {status}")
            all_ok = False
        
        if not delivery_otp or len(delivery_otp) != 4:
            print(f"   ❌ Invalid delivery OTP: {delivery_otp}")
            all_ok = False
        
        if working_on_order != TEST_ORDER_ID:
            print(f"   ❌ Rider should be working on {TEST_ORDER_ID}, got {working_on_order}")
            all_ok = False
        
        return all_ok
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def cleanup_test_data():
    """Clean up test data"""
    print(f"\n6️⃣ Cleaning up test data...")
    cleanup_existing_test_data()


def main():
    """Run all tests"""
    print("=" * 70)
    print("🧪 Testing Automatic Order Assignment Handler")
    print("=" * 70)
    
    # Cleanup first
    cleanup_existing_test_data()
    
    # Run tests
    success = True
    success = success and create_test_rider()
    success = success and create_test_order()
    success = success and update_order_to_ready()
    
    if success:
        success = success and wait_for_assignment(max_wait_seconds=15)
    
    if success:
        success = success and verify_assignment()
    
    # Cleanup
    cleanup_test_data()
    
    # Summary
    print("\n" + "=" * 70)
    if success:
        print("✅ ALL TESTS PASSED")
        print("✅ Order assignment handler working correctly")
        print("✅ Rider assigned automatically when order status = READY_FOR_PICKUP")
    else:
        print("❌ SOME TESTS FAILED")
        print("❌ Check CloudWatch Logs for OrderAssignmentHandlerFunction")
        print("❌ Verify Lambda function is deployed and stream is connected")
    print("=" * 70)
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
