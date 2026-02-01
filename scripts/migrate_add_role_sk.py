#!/usr/bin/env python3
"""
Migration script to add role as Sort Key to UsersTable
Converts from PK-only (phone) to composite key (phone, role)
"""
import boto3
import json
import sys
from datetime import datetime

# Configuration
ENVIRONMENT = 'dev'  # Change to 'prod' when ready
TABLE_NAME = f'food-delivery-users-{ENVIRONMENT}'
BACKUP_FILE = f'users_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

dynamodb_client = boto3.client('dynamodb')


def backup_existing_data():
    """Backup existing users table data"""
    print(f"\n{'='*60}")
    print("STEP 1: Backing up existing data")
    print(f"{'='*60}")
    
    try:
        items = []
        last_evaluated_key = None
        
        while True:
            if last_evaluated_key:
                response = dynamodb_client.scan(
                    TableName=TABLE_NAME,
                    ExclusiveStartKey=last_evaluated_key
                )
            else:
                response = dynamodb_client.scan(TableName=TABLE_NAME)
            
            items.extend(response.get('Items', []))
            
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break
        
        # Save to file
        with open(BACKUP_FILE, 'w') as f:
            json.dump(items, f, indent=2)
        
        print(f"✅ Backed up {len(items)} items to {BACKUP_FILE}")
        return items
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        sys.exit(1)


def delete_old_items(items):
    """Delete old items (with phone-only key)"""
    print(f"\n{'='*60}")
    print("STEP 2: Deleting old items")
    print(f"{'='*60}")
    
    try:
        for i, item in enumerate(items):
            phone = item.get('phone', {}).get('S', '')
            
            # Delete old item (phone-only key)
            dynamodb_client.delete_item(
                TableName=TABLE_NAME,
                Key={'phone': {'S': phone}}
            )
            
            if (i + 1) % 10 == 0:
                print(f"  Deleted {i + 1}/{len(items)} items...")
        
        print(f"✅ Deleted {len(items)} old items")
    except Exception as e:
        print(f"❌ Deletion failed: {e}")
        print("⚠️ You may need to manually delete remaining items")
        sys.exit(1)


def create_new_items(items):
    """Create new items with composite key (phone, role)"""
    print(f"\n{'='*60}")
    print("STEP 3: Creating new items with composite key")
    print(f"{'='*60}")
    
    try:
        for i, item in enumerate(items):
            phone = item.get('phone', {}).get('S', '')
            role = item.get('role', {}).get('S', 'CUSTOMER')  # Default to CUSTOMER
            
            # Item already has 'role' attribute, just put it back
            dynamodb_client.put_item(
                TableName=TABLE_NAME,
                Item=item
            )
            
            if (i + 1) % 10 == 0:
                print(f"  Created {i + 1}/{len(items)} items...")
        
        print(f"✅ Created {len(items)} items with composite key (phone, role)")
    except Exception as e:
        print(f"❌ Creation failed: {e}")
        print(f"⚠️ Restore from backup: {BACKUP_FILE}")
        sys.exit(1)


def verify_migration(original_count):
    """Verify migration was successful"""
    print(f"\n{'='*60}")
    print("STEP 4: Verifying migration")
    print(f"{'='*60}")
    
    try:
        response = dynamodb_client.scan(
            TableName=TABLE_NAME,
            Select='COUNT'
        )
        
        new_count = response.get('Count', 0)
        
        print(f"  Original items: {original_count}")
        print(f"  New items: {new_count}")
        
        if new_count == original_count:
            print(f"✅ Migration successful - item count matches")
        else:
            print(f"⚠️ Warning: Item count mismatch!")
            print(f"   Expected: {original_count}, Got: {new_count}")
        
        # Sample a few items to verify composite key
        sample = dynamodb_client.scan(TableName=TABLE_NAME, Limit=3)
        
        print(f"\n  Sample items:")
        for item in sample.get('Items', []):
            phone = item.get('phone', {}).get('S', 'N/A')
            role = item.get('role', {}).get('S', 'N/A')
            print(f"    - Phone: {phone}, Role: {role}")
        
        print(f"\n✅ Migration verification complete")
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")


def main():
    print("\n" + "="*60)
    print("USERS TABLE MIGRATION: Add Role as Sort Key")
    print("="*60)
    print(f"\nEnvironment: {ENVIRONMENT}")
    print(f"Table: {TABLE_NAME}")
    print(f"Backup file: {BACKUP_FILE}")
    
    # Confirm before proceeding
    print(f"\n⚠️  WARNING: This will modify the Users table!")
    print("   - All existing items will be deleted and recreated")
    print("   - A backup will be saved to file")
    print("   - The table structure will change")
    
    response = input("\nAre you sure you want to proceed? (yes/no): ")
    if response.lower() != 'yes':
        print("\n❌ Migration cancelled")
        sys.exit(0)
    
    # Step 1: Backup
    items = backup_existing_data()
    original_count = len(items)
    
    # Step 2: Delete old items
    delete_old_items(items)
    
    # Step 3: Create new items with composite key
    create_new_items(items)
    
    # Step 4: Verify
    verify_migration(original_count)
    
    print(f"\n{'='*60}")
    print("MIGRATION COMPLETED SUCCESSFULLY!")
    print(f"{'='*60}")
    print(f"\nBackup saved to: {BACKUP_FILE}")
    print("You can now deploy the updated Lambda code.")
    print(f"\nNext steps:")
    print("  1. Deploy Lambda: sam build && sam deploy")
    print("  2. Test signup as customer")
    print("  3. Test signup as rider with same phone")
    print("  4. Verify both roles work independently")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Migration interrupted by user")
        print(f"Backup saved to: {BACKUP_FILE}")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ MIGRATION FAILED: {e}")
        print(f"Restore from backup: {BACKUP_FILE}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
