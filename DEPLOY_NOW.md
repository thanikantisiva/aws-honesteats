# Quick Deploy Guide - Dual Role Support

## TL;DR

```bash
cd /Users/user/startup/aws-honesteats
sam build && sam deploy
python3 scripts/migrate_add_role_sk.py
```

Done! Now customers can signup as riders with the same phone number.

---

## What This Does

Allows `+919876543210` to be BOTH:
- CUSTOMER (orders food)
- RIDER (delivers food)

No more "Phone already registered" error!

---

## Verification

Test it:
1. Signup as customer in customer app with `9876543210`
2. Signup as rider in rider app with `9876543210`
3. Both should work! ✅

---

## If Something Breaks

Rollback:
```bash
aws cloudformation update-stack \
  --stack-name rork-honesteats-api-dev \
  --use-previous-template \
  --capabilities CAPABILITY_IAM
```

Restore data from backup:
```bash
# Check scripts/ for backup file: users_backup_*.json
```

---

**See `DUAL_ROLE_DEPLOYMENT.md` for detailed steps**
