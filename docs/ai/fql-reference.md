# FQL Language Reference — AI Agent Context

> **Purpose**: This document is a complete, self-contained reference for the FQL (Financial Query Language) used by [DblEntry](https://github.com/danielgerlag/dblentry). Include this file in your LLM context to generate correct FQL.

## Overview

FQL is a declarative, SQL-inspired language for double-entry bookkeeping. It operates on **entities** (isolated sets of books), each containing **accounts**, **journals** (transactions), and **rates** (time-varying values). All monetary values use arbitrary-precision decimals (not floating point).

## Grammar

```ebnf
script        = statement (";" statement)* ";"?

statement     = create_command
              | get_expression
              | set_command
              | accrue_command
              | "USE" "ENTITY" text
              | "BEGIN"
              | "COMMIT"
              | "ROLLBACK"

create_command = "CREATE" ( entity | account | journal | rate )

entity        = "ENTITY" text
account       = "ACCOUNT" account_id account_type
journal       = "JOURNAL" date "," amount "," text
                ["FOR" dimension ("," dimension)*]
                ledger_op ("," ledger_op)*
rate          = "RATE" identifier

get_expression = "GET" alias_expr ("," alias_expr)*
alias_expr     = expression "AS" identifier

set_command    = "SET" "RATE" identifier expression expression

accrue_command = "ACCRUE" account_id "FROM" date "TO" date
                 "WITH" "RATE" identifier
                 [compound_method]
                 "BY" identifier
                 "INTO" "JOURNAL" date "," text
                 ledger_op ("," ledger_op)*

compound_method = "COMPOUND" ("DAILY" | "CONTINUOUS")

ledger_op      = ("DEBIT" | "CREDIT") account_id [amount_or_pct]
amount_or_pct  = expression | percentage

dimension      = identifier "=" expression
```

### Literals

| Type | Syntax | Examples |
|------|--------|---------|
| Integer | `-?[0-9]+` | `42`, `-1`, `0` |
| Decimal | `-?[0-9]+\.[0-9]+` | `100.50`, `-0.53` |
| Percentage | `[0-9]+(\.[0-9]+)?%` | `50%`, `3.5%` |
| Text | `'...'` (single-quoted, `''` to escape) | `'Payment'`, `'O''Brien'` |
| Date | `YYYY-MM-DD` | `2024-01-15` |
| Account ID | `@identifier` | `@bank`, `@interest_earned` |
| Boolean | `TRUE` \| `FALSE` | |
| Null | `NULL` | |
| Parameter | `$name` | `$amount`, `$date` |

### Operators (by precedence, lowest first)

| Precedence | Operators |
|-----------|-----------|
| 1 | `AND`, `OR` |
| 2 | `NOT` (unary) |
| 3 | `=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`, `IN` |
| 4 | `+`, `-` |
| 5 | `*`, `/` |
| 6 | `%` (modulo), `^` (exponent) |
| 7 | `IS NULL`, `IS NOT NULL` |

### Account Types

Five types, case-insensitive: `ASSET`, `LIABILITY`, `INCOME`, `EXPENSE`, `EQUITY`.

## Statements

### CREATE ACCOUNT

```sql
CREATE ACCOUNT @account_id TYPE;
```

Creates a named account of the given type.

```sql
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @equity EQUITY;
CREATE ACCOUNT @interest_income INCOME;
```

### CREATE JOURNAL

```sql
CREATE JOURNAL date, amount, 'description'
  [FOR dim1=val1, dim2=val2]
  DEBIT @account [amount_or_pct],
  CREDIT @account [amount_or_pct];
```

Creates a double-entry transaction. Ledger operations must balance (total debits = total credits). If an operation omits the amount, the full journal amount is used.

```sql
-- Simple two-sided entry
CREATE JOURNAL 2024-01-15, 1000, 'Investment'
  DEBIT @bank,
  CREDIT @equity;

-- With dimensions for tagging
CREATE JOURNAL 2024-02-01, 500, 'Loan to Acme'
  FOR customer='Acme', product='Term Loan'
  DEBIT @loans,
  CREDIT @bank;

-- Split entry with percentages
CREATE JOURNAL 2024-03-01, 200, 'Fee split'
  DEBIT @bank,
  CREDIT @fee_income 60%,
  CREDIT @tax_payable 40%;

-- Split entry with fixed amounts
CREATE JOURNAL 2024-03-01, 200, 'Fee split'
  DEBIT @bank,
  CREDIT @fee_income 120,
  CREDIT @tax_payable 80;
```

### CREATE RATE

```sql
CREATE RATE identifier;
```

Creates a named rate (for interest, FX, tax, etc.).

```sql
CREATE RATE prime;
CREATE RATE usd_eur;
```

### SET RATE

```sql
SET RATE identifier value date;
```

Sets a rate value effective from the given date. Multiple values can be set at different dates to create a time series.

```sql
SET RATE prime 0.05 2024-01-01;
SET RATE prime 0.055 2024-07-01;
SET RATE usd_eur 1.08 2024-01-01;
```

### CREATE ENTITY

```sql
CREATE ENTITY 'name';
```

Creates an isolated set of books. Each entity has its own accounts, journals, and rates.

```sql
CREATE ENTITY 'Acme Corp';
```

### USE ENTITY

```sql
USE ENTITY 'name';
```

Switches the active entity. All subsequent statements operate on this entity.

```sql
USE ENTITY 'Acme Corp';
CREATE ACCOUNT @bank ASSET;  -- belongs to 'Acme Corp'
```

### GET

```sql
GET expression AS alias [, expression AS alias ...];
```

Evaluates expressions and returns results.

```sql
GET balance(@bank, 2024-12-31) AS bank_balance;
GET trial_balance(2024-12-31) AS tb;
GET account_count() AS count;
GET balance(@bank, 2024-12-31) AS b, account_count() AS c;
```

### ACCRUE

```sql
ACCRUE @account FROM start_date TO end_date
  WITH RATE rate_name
  [COMPOUND DAILY | COMPOUND CONTINUOUS]
  BY dimension_name
  INTO JOURNAL accrual_date, 'description'
  DEBIT @target_debit,
  CREDIT @target_credit;
```

Calculates interest accrual on per-dimension balances and creates journal entries.

- Without `COMPOUND`: `daily = balance × rate`
- `COMPOUND DAILY`: `daily = balance × rate / 365`
- `COMPOUND CONTINUOUS`: `daily = balance × rate`

```sql
ACCRUE @loans FROM 2024-01-01 TO 2024-01-31
  WITH RATE prime
  COMPOUND DAILY
  BY customer
  INTO JOURNAL 2024-02-01, 'January interest'
  DEBIT @interest_receivable,
  CREDIT @interest_income;
```

### Transactions

```sql
BEGIN;
-- statements...
COMMIT;
-- or
ROLLBACK;
```

Explicit ACID transactions. Additionally, every batch submitted without explicit `BEGIN`/`COMMIT` is implicitly wrapped in a transaction — if any statement fails, the entire batch rolls back.

## Built-in Functions

| Function | Signature | Returns | Description |
|----------|-----------|---------|-------------|
| `balance` | `balance(@acct, date [, dim=val])` | Decimal | Account balance at date, optionally filtered by dimension |
| `statement` | `statement(@acct, from, to [, dim=val])` | Table | Ledger entries for period (date, description, amount, balance) |
| `trial_balance` | `trial_balance(date)` | Table | All accounts with debit/credit columns |
| `income_statement` | `income_statement(from, to)` | Table | Income & expense changes for period |
| `account_count` | `account_count()` | Integer | Number of accounts in active entity |
| `fx_rate` | `fx_rate('name', date)` | Decimal | Rate value at date (closest prior date) |
| `convert` | `convert(amount, 'rate', date)` | Decimal | `amount × fx_rate(rate, date)` |
| `round` | `round(value [, places])` | Decimal | Round to N decimal places (default 2) |
| `abs` | `abs(value)` | Decimal | Absolute value |
| `min` | `min(a, b)` | Decimal | Smaller of two values |
| `max` | `max(a, b)` | Decimal | Larger of two values |

## Entity Model

- **Default entity**: `"default"` — used when no `USE ENTITY` is specified
- **Isolation**: Each entity has completely independent accounts, journals, and rates
- **Multi-tenancy**: Multiple entities coexist in one DblEntry instance
- Entities are created with `CREATE ENTITY 'name'` and selected with `USE ENTITY 'name'`

## Complete Example

```sql
-- Set up a lending fund
CREATE ENTITY 'Lending Fund Q1';
USE ENTITY 'Lending Fund Q1';

-- Chart of accounts
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @loans ASSET;
CREATE ACCOUNT @interest_income INCOME;
CREATE ACCOUNT @equity EQUITY;

-- Interest rate
CREATE RATE prime;
SET RATE prime 0.05 2024-01-01;

-- Investor puts in capital
CREATE JOURNAL 2024-01-01, 20000, 'Investor capital'
  DEBIT @bank,
  CREDIT @equity;

-- Issue a loan
CREATE JOURNAL 2024-01-15, 1500, 'Loan to Acme'
  FOR customer='Acme'
  DEBIT @loans,
  CREDIT @bank;

-- Accrue interest
ACCRUE @loans FROM 2024-01-15 TO 2024-01-31
  WITH RATE prime
  COMPOUND DAILY
  BY customer
  INTO JOURNAL 2024-02-01, 'January interest'
  DEBIT @loans,
  CREDIT @interest_income;

-- Query results
GET trial_balance(2024-02-01) AS tb;
GET balance(@bank, 2024-02-01) AS cash;
GET statement(@loans, 2024-01-01, 2024-02-01, customer='Acme') AS acme_stmt;
GET income_statement(2024-01-01, 2024-02-01) AS pnl;
```

## Error Handling

- Missing account: `"Account not found: account_name"`
- Duplicate account: `"Account already exists: account_name"`
- Unbalanced journal: `"Journal entries do not balance"`
- Division by zero: returns error, does not panic
- Entity not found: `"Entity not found: name"`
- Entity already exists: `"Entity already exists: name"`
- No active transaction for COMMIT/ROLLBACK: returns error

## Key Design Principles

1. **Precision**: All monetary values are `rust_decimal::Decimal` (128-bit), never floating point
2. **Immutability**: Journals are append-only; corrections are made via reversing entries
3. **Isolation**: Entities provide complete data isolation (multi-tenancy)
4. **Atomicity**: Every batch is transactional — all or nothing
5. **Determinism**: Same input always produces same output (no implicit timestamps)
6. **AI-friendly**: Simple, regular syntax with no ambiguity; ideal for LLM code generation
