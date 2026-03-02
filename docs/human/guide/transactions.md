# Transactions

DblEntry provides full **ACID transaction** support — atomicity, consistency, isolation, and durability.

## Explicit Transactions

Use `BEGIN`, `COMMIT`, and `ROLLBACK` to control transaction boundaries:

```sql
BEGIN;

CREATE JOURNAL 2024-01-15, 1000, 'Transfer'
  DEBIT @checking,
  CREDIT @savings;

CREATE JOURNAL 2024-01-15, 50, 'Transfer fee'
  DEBIT @checking,
  CREDIT @fee_income;

COMMIT;
```

If anything goes wrong, roll back:

```sql
BEGIN;

CREATE JOURNAL 2024-01-15, 1000, 'Risky operation'
  DEBIT @checking,
  CREDIT @savings;

-- Something went wrong, undo everything
ROLLBACK;
```

## Implicit Transactions

Every batch of statements submitted without explicit `BEGIN`/`COMMIT` is **automatically wrapped** in a transaction:

```sql
-- These two statements are atomic — if the second fails,
-- the first is also rolled back
CREATE ACCOUNT @bank ASSET;
CREATE JOURNAL 2024-01-01, 1000, 'Deposit'
  DEBIT @bank,
  CREDIT @equity;
```

If the journal creation fails (e.g., `@equity` doesn't exist), the account creation is also rolled back.

## Guarantees

| Property | Guarantee |
|----------|-----------|
| **Atomicity** | All statements in a transaction succeed or all are rolled back |
| **Consistency** | Debits always equal credits; account types are enforced |
| **Isolation** | Concurrent access is serialized (single-writer model) |
| **Durability** | Committed transactions persist (depends on storage backend) |

## Error Handling

- If a statement fails inside an implicit transaction, the **entire batch** is rolled back
- If a statement fails inside an explicit transaction, the transaction remains open — you must explicitly `COMMIT` or `ROLLBACK`
- Calling `COMMIT` or `ROLLBACK` without an active transaction returns an error
