#!/usr/bin/env python3
"""
Test script for S3 document upload flow
Tests the complete flow: presigned URL generation → S3 upload → signup
"""
import requests
import base64
import json
import sys
from pathlib import Path

# Configuration
API_BASE_URL = "https://htgicpllf2.execute-api.ap-south-1.amazonaws.com/default"
API_KEY = "dev-mobile-key-12345"  # Replace with actual key
TEST_PHONE = "9999999999"  # Test phone number

def test_upload_document(document_type):
    """Test: Upload document via backend"""
    print("\n" + "="*60)
    print(f"TEST: Upload {document_type.upper()} via Backend")
    print("="*60)
    
    url = f"{API_BASE_URL}/api/v1/riders/documents/upload"
    
    # Create a small test image (1x1 pixel JPEG)
    test_image_base64 = "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJCQwLDBgNDRgyIRwhMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjL/wAARCAABAAEDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAv/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAAAAX/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIRAxEAPwCwAA8A/9k="
    
    payload = {
        "phone": TEST_PHONE,
        "documentType": document_type,
        "imageBase64": test_image_base64
    }
    
    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": API_KEY
    }
    
    print(f"\nRequest URL: {url}")
    print(f"Document Type: {document_type}")
    print(f"Image size: ~{len(test_image_base64)} chars (base64)")
    
    response = requests.post(url, json=payload, headers=headers)
    
    print(f"\nResponse Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    if response.status_code == 200:
        file_url = response.json().get('fileUrl')
        print(f"✅ {document_type.upper()} uploaded successfully")
        print(f"   S3 URL: {file_url}")
        return file_url
    else:
        print(f"❌ Failed to upload {document_type}")
        return None


def test_signup(aadhar_url, pan_url):
    """Test 3: Signup with S3 URLs"""
    print("\n" + "="*60)
    print("TEST 3: Rider Signup with S3 URLs")
    print("="*60)
    
    if not aadhar_url or not pan_url:
        print("❌ Skipping - missing S3 URLs")
        return
    
    url = f"{API_BASE_URL}/api/v1/riders/signup"
    
    payload = {
        "phone": TEST_PHONE,
        "firstName": "Test",
        "lastName": "Rider",
        "address": "123 Test Street, Mumbai, Maharashtra 400001",
        "aadharNumber": "123456789012",
        "aadharImageUrl": aadhar_url,
        "panNumber": "ABCDE1234F",
        "panImageUrl": pan_url
    }
    
    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": API_KEY
    }
    
    print(f"\nRequest URL: {url}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    response = requests.post(url, json=payload, headers=headers)
    
    print(f"\nResponse Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    if response.status_code in [200, 201]:
        print("✅ Signup completed successfully")
    else:
        print("❌ Signup failed")


def main():
    print("\n" + "="*60)
    print("S3 DOCUMENT UPLOAD INTEGRATION TEST")
    print("="*60)
    print(f"\nAPI Base URL: {API_BASE_URL}")
    print(f"Test Phone: {TEST_PHONE}")
    
    # Test 1: Upload Aadhar via backend
    print("\n📤 Testing Aadhar upload via backend...")
    aadhar_url = test_upload_document('aadhar')
    
    if not aadhar_url:
        print("\n❌ TESTS FAILED: Cannot upload Aadhar")
        sys.exit(1)
    
    # Test 2: Upload PAN via backend
    print("\n📤 Testing PAN upload via backend...")
    pan_url = test_upload_document('pan')
    
    if not pan_url:
        print("\n❌ TESTS FAILED: Cannot upload PAN")
        sys.exit(1)
    
    # Test 3: Complete signup
    print("\n📝 Testing complete signup flow...")
    test_signup(aadhar_url, pan_url)
    
    print("\n" + "="*60)
    print("TESTS COMPLETED")
    print("="*60)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
