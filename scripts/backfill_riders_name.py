"""Backfill firstName/lastName on the riders table from the users table.

For each rider row missing `firstName`/`lastName`, look up the matching RIDER
record in the Users table by `riderId-index` and copy `firstName` / `lastName`
across.

Usage:
  python3 scripts/backfill_riders_name.py \
    --riders-table food-delivery-riders-prod \
    --users-table food-delivery-users-prod
  python3 scripts/backfill_riders_name.py \
    --riders-table food-delivery-riders-prod \
    --users-table food-delivery-users-prod \
    --dry-run
"""

import argparse

import boto3


def fetch_user_names(users_client, users_table: str, rider_id: str):
    """Look up RIDER user by riderId via riderId-index. Returns (firstName, lastName)."""
    try:
        response = users_client.query(
            TableName=users_table,
            IndexName="riderId-index",
            KeyConditionExpression="riderId = :rid",
            ExpressionAttributeValues={":rid": {"S": rider_id}},
            Limit=1,
        )
        items = response.get("Items") or []
        if not items:
            return None, None
        item = items[0]
        first = item.get("firstName", {}).get("S") or None
        last = item.get("lastName", {}).get("S") or None
        return first, last
    except Exception as e:
        print(f"WARN: users lookup failed for riderId={rider_id}: {e}")
        return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--riders-table", required=True, help="Riders table name")
    parser.add_argument("--users-table", required=True, help="Users table name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only log changes; do not write to DynamoDB",
    )
    args = parser.parse_args()

    dynamodb = boto3.client("dynamodb")
    riders_table = args.riders_table
    users_table = args.users_table

    scanned = 0
    updated = 0
    already_named = 0
    no_user = 0
    no_name_on_user = 0
    failed = 0
    last_evaluated_key = None

    while True:
        scan_kwargs = {
            "TableName": riders_table,
            "ProjectionExpression": "riderId, firstName, lastName",
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = dynamodb.scan(**scan_kwargs)

        for item in response.get("Items", []):
            scanned += 1

            rider_id_attr = item.get("riderId") or {}
            rider_id = rider_id_attr.get("S")
            if not rider_id:
                continue

            current_first = (item.get("firstName") or {}).get("S")
            current_last = (item.get("lastName") or {}).get("S")
            if current_first and current_last:
                already_named += 1
                continue

            first, last = fetch_user_names(dynamodb, users_table, rider_id)
            if first is None and last is None:
                no_user += 1
                continue
            if not first and not last:
                no_name_on_user += 1
                continue

            update_parts = []
            values = {}
            if first and not current_first:
                update_parts.append("firstName = :f")
                values[":f"] = {"S": first}
            if last and not current_last:
                update_parts.append("lastName = :l")
                values[":l"] = {"S": last}

            if not update_parts:
                already_named += 1
                continue

            if args.dry_run:
                print(
                    f"[DRY RUN] riderId={rider_id} -> firstName={first!r} lastName={last!r}"
                )
                updated += 1
                continue

            try:
                dynamodb.update_item(
                    TableName=riders_table,
                    Key={"riderId": {"S": rider_id}},
                    UpdateExpression="SET " + ", ".join(update_parts),
                    ExpressionAttributeValues=values,
                )
                updated += 1
                if updated % 100 == 0:
                    print(f"Updated {updated} riders so far...")
            except Exception as e:
                failed += 1
                print(f"FAILED riderId={rider_id}: {e}")

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    print("---")
    print(f"Scanned:           {scanned}")
    print(f"Updated:           {updated}{' (dry-run)' if args.dry_run else ''}")
    print(f"Already had name:  {already_named}")
    print(f"No matching user:  {no_user}")
    print(f"User had no name:  {no_name_on_user}")
    print(f"Failed:            {failed}")


if __name__ == "__main__":
    main()
