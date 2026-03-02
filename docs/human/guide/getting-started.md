# Getting Started

DblEntry is a **Layer 2 database** for double-entry bookkeeping. It sits on top of any storage backend (in-memory, SQLite, PostgreSQL) and provides a purpose-built query language called **FQL** (Financial Query Language).

## Quick Start

### 1. Create Accounts

Every accounting system starts with a chart of accounts. Each account has a type that determines its behavior:

```sql
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @loans ASSET;
CREATE ACCOUNT @interest_income INCOME;
CREATE ACCOUNT @equity EQUITY;
```

### 2. Record Transactions

Journals are double-entry transactions where debits must equal credits:

```sql
CREATE JOURNAL 2024-01-01, 20000, 'Investor capital'
  DEBIT @bank,
  CREDIT @equity;
```

### 3. Query Your Books

```sql
GET trial_balance(2024-12-31) AS tb;
GET balance(@bank, 2024-12-31) AS cash;
GET statement(@bank, 2024-01-01, 2024-12-31) AS activity;
```

## Account Types

| Type | Description | Normal Balance |
|------|-------------|----------------|
| `ASSET` | Things you own (cash, receivables, equipment) | Debit |
| `LIABILITY` | Things you owe (payables, loans received) | Credit |
| `EQUITY` | Owner's investment and retained earnings | Credit |
| `INCOME` | Revenue earned | Credit |
| `EXPENSE` | Costs incurred | Debit |

## What's Next?

- [Core Concepts](/guide/core-concepts) — dimensions, rates, and accruals
- [Entities](/guide/entities) — multi-entity isolation
- [Transactions](/guide/transactions) — ACID guarantees
- [FQL Reference](/reference/statements) — complete statement reference
