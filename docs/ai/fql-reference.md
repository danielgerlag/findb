# FQL Language Reference — AI Agent Context

> **Purpose**: This document is a complete, self-contained reference for the FQL (Financial Query Language) used by [DblEntry](https://github.com/danielgerlag/dblentry). Include this file in your LLM context to generate correct FQL.

## Overview

FQL is a declarative, SQL-inspired language for double-entry bookkeeping. It operates on **entities** (isolated sets of books), each containing **accounts**, **journals** (transactions), and **rates** (time-varying values). All monetary values use arbitrary-precision decimals (not floating point).

## Grammar

```ebnf
script        = statement (";" statement)* ";"?

statement     = create_command
              | sell_command
              | split_command
              | get_expression
              | set_command
              | accrue_command
              | "USE" "ENTITY" text
              | "BEGIN"
              | "COMMIT"
              | "ROLLBACK"

create_command = "CREATE" ( entity | account | journal | rate )

entity        = "ENTITY" text
account       = "ACCOUNT" account_id account_type ["UNITS" "'" identifier "'"]
journal       = "JOURNAL" date "," amount "," text
                ["FOR" dimension ("," dimension)*]
                ledger_op ("," ledger_op)*
rate          = "RATE" identifier

sell_command   = "SELL" amount "UNITS" "OF" account_id "AT" expression
                "ON" date
                ["FOR" dimension ("," dimension)*]
                ["METHOD" ("FIFO" | "LIFO" | "AVERAGE")]
                "PROCEEDS" account_id
                "GAIN_LOSS" account_id
                "DESCRIPTION" text

split_command  = "SPLIT" account_id expression "FOR" expression date

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

ledger_op      = ("DEBIT" | "CREDIT") account_id [amount_or_pct] [units_clause]
amount_or_pct  = expression | percentage
units_clause   = expression "UNITS" "AT" expression

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

### Hierarchical Dimensions

Dimension values support `/` as a path separator for hierarchical grouping:

```
Region='Americas/US/West'
Sector='Technology/Software'
```

Querying at a parent level aggregates all children — `balance(@acct, date, Region='Americas')` includes `Americas/US`, `Americas/Canada`, etc. This applies to all functions that accept dimension filters.

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

A unit-tracked account is linked to a rate for price tracking:

```sql
CREATE ACCOUNT @stock_aapl ASSET UNITS 'aapl_price';
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

-- Unit-tracked purchase: buy 10 shares at $150 each (total $1500)
CREATE JOURNAL 2024-04-01, 1500, 'Buy AAPL'
  FOR Sector='Technology/Software'
  DEBIT @stock_aapl 10 UNITS AT 150,
  CREDIT @bank;
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

### DISTRIBUTE

```sql
DISTRIBUTE amount
  FROM start_date TO end_date
  PERIOD MONTHLY | QUARTERLY | YEARLY
  [PRORATE]
  [FOR dim1=val1, dim2=val2]
  DESCRIPTION 'text'
  DEBIT @account,
  CREDIT @account;
```

Spreads a fixed amount evenly across time periods, generating one journal per period.

- **Even split**: `amount / num_periods` per journal, remainder to last period
- **PRORATE**: allocates by day count instead of even split (for partial first/last periods)
- **Journal dates**: last day of each period (clamped to end date)
- **Dimensions**: optional `FOR` clause attaches dimensions to all generated journals

```sql
-- Spread $12,000 annual subscription over 12 months
DISTRIBUTE 12000
  FROM 2024-01-01 TO 2024-12-31
  PERIOD MONTHLY
  FOR Customer='Acme'
  DESCRIPTION 'Revenue recognition - Acme'
  DEBIT @deferred_revenue,
  CREDIT @subscription_revenue;

-- Prorated insurance across partial months
DISTRIBUTE 2400
  FROM 2024-03-15 TO 2024-06-14
  PERIOD MONTHLY
  PRORATE
  DESCRIPTION 'Insurance amortization'
  DEBIT @insurance_expense,
  CREDIT @prepaid_insurance;
```

### SELL

```sql
SELL units UNITS OF @account AT price ON date
  [FOR dim1=val1, dim2=val2]
  [METHOD FIFO | LIFO | AVERAGE]
  PROCEEDS @proceeds_account
  GAIN_LOSS @gain_loss_account
  DESCRIPTION 'text';
```

Sells units from a unit-tracked account, depleting lots using the specified cost method (default FIFO). Automatically calculates realized gain/loss and records the proceeds.

- **FIFO** (default): Depletes oldest lots first
- **LIFO**: Depletes newest lots first
- **AVERAGE**: Uses weighted average cost basis
- **FOR clause**: Scopes depletion to lots matching the given dimensions. With hierarchical dimensions, depletes matching lots across sub-levels using FIFO ordering by date.

```sql
-- Sell 5 shares of AAPL using FIFO (default)
SELL 5 UNITS OF @stock_aapl AT 175 ON 2024-06-15
  PROCEEDS @bank
  GAIN_LOSS @realized_gains
  DESCRIPTION 'Sell AAPL shares';

-- Sell within a specific dimensional scope using LIFO
SELL 3 UNITS OF @stock_aapl AT 180 ON 2024-07-01
  FOR Sector='Technology'
  METHOD LIFO
  PROCEEDS @bank
  GAIN_LOSS @realized_gains
  DESCRIPTION 'Sell tech sector AAPL';
```

### SPLIT

```sql
SPLIT @account new FOR old date;
```

Records a stock split, adjusting all open lot units and cost basis proportionally. `new FOR old` describes the split ratio (e.g., `3 FOR 1` triples the units and reduces cost per unit by a factor of 3).

```sql
-- 2-for-1 stock split
SPLIT @stock_aapl 2 FOR 1 2024-08-01;

-- 3-for-2 stock split
SPLIT @stock_aapl 3 FOR 2 2024-09-15;
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
| `balance` | `balance(@acct, date [, dim=val])` | Decimal | Account balance at date, optionally filtered by dimension. Hierarchical dimension values use prefix matching. |
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
| `units` | `units(@acct, date [, dim=val])` | Decimal | Total units held in a unit-tracked account |
| `market_value` | `market_value(@acct, date [, dim=val])` | Decimal | Units × current rate (mark-to-market value) |
| `unrealized_gain` | `unrealized_gain(@acct, date [, dim=val])` | Decimal | Market value minus cost basis |
| `cost_basis` | `cost_basis(@acct, date [, dim=val])` | Decimal | Weighted average cost per unit |
| `lots` | `lots(@acct, date [, dim=val])` | Table | Open lots with date, units, cost per unit |

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

### Unit-Tracked Portfolio Example

```sql
-- Set up a stock portfolio
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @equity EQUITY;
CREATE ACCOUNT @realized_gains INCOME;
CREATE RATE aapl_price;
CREATE ACCOUNT @stock_aapl ASSET UNITS 'aapl_price';

-- Fund the account
CREATE JOURNAL 2024-01-01, 50000, 'Initial capital'
  DEBIT @bank,
  CREDIT @equity;

-- Buy 100 shares at $150 across regions
SET RATE aapl_price 150 2024-01-15;
CREATE JOURNAL 2024-01-15, 9000, 'Buy AAPL (US West)'
  FOR Region='Americas/US/West'
  DEBIT @stock_aapl 60 UNITS AT 150,
  CREDIT @bank;

CREATE JOURNAL 2024-02-01, 6200, 'Buy AAPL (US East)'
  FOR Region='Americas/US/East'
  DEBIT @stock_aapl 40 UNITS AT 155,
  CREDIT @bank;

-- Price rises
SET RATE aapl_price 180 2024-06-01;

-- Sell 20 shares from Americas region (FIFO across sub-levels)
SELL 20 UNITS OF @stock_aapl AT 180 ON 2024-06-15
  FOR Region='Americas'
  PROCEEDS @bank
  GAIN_LOSS @realized_gains
  DESCRIPTION 'Partial sale - Americas';

-- Stock split: 2-for-1
SET RATE aapl_price 90 2024-08-01;
SPLIT @stock_aapl 2 FOR 1 2024-08-01;

-- Query the portfolio
GET units(@stock_aapl, 2024-08-01) AS total_units;
GET market_value(@stock_aapl, 2024-08-01) AS mv;
GET unrealized_gain(@stock_aapl, 2024-08-01) AS ug;
GET cost_basis(@stock_aapl, 2024-08-01) AS cb;
GET lots(@stock_aapl, 2024-08-01) AS open_lots;
GET units(@stock_aapl, 2024-08-01, Region='Americas/US') AS us_units;
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
