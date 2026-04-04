"""
Seed script: Insert minimum app version config into DynamoDB ConfigTable.

This config drives force-update checks in the customer and rider mobile apps.
To force users to update, change the version strings here and re-run,
or update the DynamoDB item directly via the console / Retool.

Usage:
  ENVIRONMENT=dev python scripts/seed_app_version.py
  ENVIRONMENT=prod python scripts/seed_app_version.py
"""
import os
import boto3

ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
TABLE_NAME = f"food-delivery-config-{ENVIRONMENT}"

dynamodb = boto3.client("dynamodb", region_name="ap-south-1")


def python_to_dynamodb(obj):
    """Simple Python -> DynamoDB attribute value converter."""
    if obj is None:
        return {"NULL": True}
    if isinstance(obj, bool):
        return {"BOOL": obj}
    if isinstance(obj, (int, float)):
        return {"N": str(obj)}
    if isinstance(obj, str):
        return {"S": obj}
    if isinstance(obj, list):
        return {"L": [python_to_dynamodb(i) for i in obj]}
    if isinstance(obj, dict):
        return {"M": {k: python_to_dynamodb(v) for k, v in obj.items()}}
    return {"S": str(obj)}


APP_VERSION_CONFIG = {
    "minAppVersions": {
        "customer": {
            "android": "1.0.10",
            "ios": "1.0.10",
        },
        "rider": {
            "android": "1.0.0",
            "ios": "1.0.0",
        },
    },
    "storeUrls": {
        "customer": {
            "android": "https://play.google.com/store/apps/details?id=app.rork.honesteats",
            "ios": "https://apps.apple.com/app/idYOUR_CUSTOMER_APP_ID",
        },
        "rider": {
            "android": "https://play.google.com/store/apps/details?id=app.rork.honesteats.rider",
            "ios": "https://apps.apple.com/app/idYOUR_RIDER_APP_ID",
        },
    },
}


def main():
    print(f"Seeding app version config into: {TABLE_NAME}\n")

    item = {
        "partitionkey": {"S": "CONFIG#APP_VERSION"},
        "sortKey": {"S": "MINIMUM"},
        "config": python_to_dynamodb(APP_VERSION_CONFIG),
    }
    dynamodb.put_item(TableName=TABLE_NAME, Item=item)

    print("  Inserted CONFIG#APP_VERSION / MINIMUM")
    for app_type, versions in APP_VERSION_CONFIG["minAppVersions"].items():
        print(f"    {app_type}: android={versions['android']}  ios={versions['ios']}")
    print("\n  Store URLs:")
    for app_type, urls in APP_VERSION_CONFIG["storeUrls"].items():
        print(f"    {app_type}:")
        print(f"      android: {urls['android']}")
        print(f"      ios:     {urls['ios']}")
    print("\nDone! Update the DynamoDB item to change minimum versions without redeploying.")


if __name__ == "__main__":
    main()
