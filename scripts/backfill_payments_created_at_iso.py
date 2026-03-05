"""Backfill createdAtIso for existing payments records.

Usage:
  python3 scripts/backfill_payments_created_at_iso.py --table food-delivery-payments-dev
"""

import argparse

import boto3

from utils.datetime_ist import epoch_ms_to_ist_iso


def to_iso_from_attr(created_at_attr: dict) -> str:
    """Resolve createdAt attribute (N/S) into ISO string."""
    if not created_at_attr:
        return ""

    if "S" in created_at_attr and created_at_attr["S"]:
        return created_at_attr["S"]

    if "N" in created_at_attr:
        try:
            return epoch_ms_to_ist_iso(int(float(created_at_attr["N"])))
        except Exception:
            return ""

    return ""


def needs_updated_at_fix(updated_at_attr: dict) -> bool:
    """Return True if updatedAt exists as numeric and should be converted to string."""
    return bool(updated_at_attr and "N" in updated_at_attr)


def needs_created_at_fix(created_at_attr: dict) -> bool:
    """Return True if createdAt exists as numeric and should be converted to string."""
    return bool(created_at_attr and "N" in created_at_attr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="Payments table name")
    args = parser.parse_args()

    dynamodb = boto3.client("dynamodb")
    table_name = args.table

    scanned = 0
    updated = 0
    skipped = 0
    failed = 0
    last_evaluated_key = None

    while True:
        scan_kwargs = {
            "TableName": table_name,
            "ProjectionExpression": "paymentId, createdAt, createdAtIso, updatedAt",
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = dynamodb.scan(**scan_kwargs)
        items = response.get("Items", [])
        scanned += len(items)

        for item in items:
            payment_id = item.get("paymentId", {}).get("S")
            if not payment_id:
                skipped += 1
                continue

            # Already backfilled
            already_backfilled = bool(item.get("createdAtIso", {}).get("S"))
            should_fix_created_at = needs_created_at_fix(item.get("createdAt", {}))
            should_fix_updated_at = needs_updated_at_fix(item.get("updatedAt", {}))
            if already_backfilled and not should_fix_created_at and not should_fix_updated_at:
                skipped += 1
                continue

            created_at_iso = to_iso_from_attr(item.get("createdAt", {}))
            if not created_at_iso and (not should_fix_created_at and not should_fix_updated_at):
                failed += 1
                print(f"Unable to resolve createdAt for paymentId={payment_id}")
                continue

            expression_parts = []
            expression_values = {}

            if not already_backfilled and created_at_iso:
                expression_parts.append("createdAtIso = :iso")
                expression_values[":iso"] = {"S": created_at_iso}

            if should_fix_created_at and created_at_iso:
                expression_parts.append("createdAt = :createdAtIso")
                expression_values[":createdAtIso"] = {"S": created_at_iso}

            if should_fix_updated_at:
                updated_at_iso = to_iso_from_attr(item.get("updatedAt", {}))
                if updated_at_iso:
                    expression_parts.append("updatedAt = :updatedAtIso")
                    expression_values[":updatedAtIso"] = {"S": updated_at_iso}

            if not expression_parts:
                skipped += 1
                continue

            try:
                dynamodb.update_item(
                    TableName=table_name,
                    Key={"paymentId": {"S": payment_id}},
                    UpdateExpression=f"SET {', '.join(expression_parts)}",
                    ExpressionAttributeValues=expression_values,
                )
                updated += 1
            except Exception as e:
                failed += 1
                print(f"Failed paymentId={payment_id}: {str(e)}")

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    print(
        f"Backfill complete | scanned={scanned} updated={updated} skipped={skipped} failed={failed}"
    )


if __name__ == "__main__":
    main()
