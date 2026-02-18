#!/usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-dev}"
BASE="/rork-honesteats/${ENVIRONMENT}"

put() {
  local name="$1"
  local value="$2"
  aws ssm put-parameter --name "${name}" --type "SecureString" --value "${value}" --overwrite >/dev/null
  echo "OK: ${name}"
}

put "${BASE}/JWT_SECRET_KEY" "dev-jwt-secret-change-in-production-use-parameter-store"

put "${BASE}/MSG91_AUTH_KEY" "488382AxH6BrN7gVmV69692811P1"
put "${BASE}/MSG91_TEMPLATE_ID" "69692941ff1c8f04bb4698a5"

put "${BASE}/MESSAGE_CENTRAL_CUSTOMER_ID" "C-85C3118AA6A340A"
put "${BASE}/MESSAGE_CENTRAL_KEY" "U2l2YWt1bWFyQDE="
put "${BASE}/MESSAGE_CENTRAL_EMAIL" "tvskumar.1995@gmail.com"
put "${BASE}/MESSAGE_CENTRAL_COUNTRY_CODE" "91"

put "${BASE}/GOOGLE_MAPS_API_KEY" "AIzaSyCL5AHrcH6PHCA4Lh1poEOk2nUPpQLNTK0"

put "${BASE}/RAZORPAY_TEST_KEY_ID" "rzp_test_S29cM7srG3pwX6"
put "${BASE}/RAZORPAY_TEST_KEY_SECRET" "hkcURSnzUksXZVWPbNaTaXXK"
put "${BASE}/RAZORPAY_LIVE_KEY_ID" "rzp_live_PLACEHOLDER_REPLACE_WHEN_READY"
put "${BASE}/RAZORPAY_LIVE_KEY_SECRET" "PLACEHOLDER_REPLACE_WHEN_READY"
put "${BASE}/RAZORPAY_WEBHOOK_SECRET" "7c3e9b8a-4d2f-4f6e-9a1b-2e8f6c0a5d41"

put "${BASE}/MOBILE_API_KEY" "dev-mobile-key-12345"
put "${BASE}/WEB_API_KEY" "dev-web-key-12345"
put "${BASE}/ADMIN_API_KEY" "dev-admin-key-12345"

echo "Dummy SSM parameters created for ${ENVIRONMENT}"
