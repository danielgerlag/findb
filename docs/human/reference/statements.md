# Statements

## CREATE ACCOUNT

Creates a named ledger account.

**Syntax:**

```sql
CREATE ACCOUNT @name TYPE;
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `@name` | Account identifier (letters, numbers, underscores) |
| `TYPE` | One of: `ASSET`, `LIABILITY`, `INCOME`, `EXPENSE`, `EQUITY` |

**Example:**

```sql
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @accounts_payable LIABILITY;
CREATE ACCOUNT @equity EQUITY;
CREATE ACCOUNT @sales_revenue INCOME;
CREATE ACCOUNT @rent_expense EXPENSE;
```

**Errors:**
- `"Account already exists: name"` — if the account already exists in the active entity

---

## CREATE JOURNAL

Creates a double-entry transaction.

**Syntax:**

```sql
CREATE JOURNAL date, amount, 'description'
  [FOR dimension=value, ...]
  DEBIT @account [amount | percentage],
  CREDIT @account [amount | percentage];
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `date` | Transaction date (`YYYY-MM-DD`) |
| `amount` | Total transaction amount (decimal) |
| `'description'` | Single-quoted description text |
| `FOR ...` | Optional dimension tags (key-value pairs) |
| `DEBIT/CREDIT` | Ledger operations — must balance |

Each ledger operation can optionally specify an amount (fixed or percentage). If omitted, the full journal amount is used.

**Examples:**

```sql
-- Simple entry
CREATE JOURNAL 2024-01-15, 1000, 'Investment'
  DEBIT @bank,
  CREDIT @equity;

-- With dimensions
CREATE JOURNAL 2024-02-01, 500, 'Loan disbursement'
  FOR customer='Acme', product='Term Loan'
  DEBIT @loans,
  CREDIT @bank;

-- Percentage split
CREATE JOURNAL 2024-03-01, 1000, 'Revenue'
  DEBIT @bank,
  CREDIT @product_revenue 70%,
  CREDIT @service_revenue 30%;

-- Fixed amount split
CREATE JOURNAL 2024-03-01, 1000, 'Revenue'
  DEBIT @bank,
  CREDIT @product_revenue 700,
  CREDIT @service_revenue 300;
```

**Errors:**
- `"Journal entries do not balance"` — debits ≠ credits
- `"Account not found: name"` — account doesn't exist

---

## CREATE RATE

Creates a named rate for tracking time-varying values.

**Syntax:**

```sql
CREATE RATE name;
```

**Example:**

```sql
CREATE RATE prime;
CREATE RATE usd_eur;
```

---

## SET RATE

Sets a rate value effective from a given date.

**Syntax:**

```sql
SET RATE name value date;
```

**Example:**

```sql
SET RATE prime 0.05 2024-01-01;
SET RATE prime 0.055 2024-07-01;
SET RATE usd_eur 0.92 2024-01-01;
```

Multiple values at different dates create a time series. Queries return the most recent value on or before the requested date.

---

## CREATE ENTITY

Creates an isolated set of books.

**Syntax:**

```sql
CREATE ENTITY 'name';
```

**Example:**

```sql
CREATE ENTITY 'Acme Corp';
```

**Errors:**
- `"Entity already exists: name"` — if the entity already exists

---

## USE ENTITY

Switches the active entity for subsequent statements.

**Syntax:**

```sql
USE ENTITY 'name';
```

**Example:**

```sql
USE ENTITY 'Acme Corp';
```

**Errors:**
- `"Entity not found: name"` — if the entity doesn't exist

---

## GET

Evaluates expressions and returns results.

**Syntax:**

```sql
GET expression AS alias [, expression AS alias ...];
```

**Examples:**

```sql
GET balance(@bank, 2024-12-31) AS cash;
GET trial_balance(2024-12-31) AS tb;
GET account_count() AS n;
GET balance(@bank, 2024-12-31) AS b, account_count() AS c;
```

---

## ACCRUE

Calculates interest accrual on per-dimension balances and creates journal entries.

**Syntax:**

```sql
ACCRUE @account FROM start_date TO end_date
  WITH RATE rate_name
  [COMPOUND DAILY | COMPOUND CONTINUOUS]
  BY dimension_name
  INTO JOURNAL date, 'description'
  DEBIT @account,
  CREDIT @account;
```

**Example:**

```sql
ACCRUE @loans FROM 2024-01-01 TO 2024-01-31
  WITH RATE prime
  COMPOUND DAILY
  BY customer
  INTO JOURNAL 2024-02-01, 'January interest'
  DEBIT @interest_receivable,
  CREDIT @interest_income;
```

---

## DISTRIBUTE

Spreads a fixed amount evenly across time periods, generating one journal entry per period. Useful for revenue recognition, straight-line depreciation, and prepaid expense amortization.

**Syntax:**

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

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `amount` | Total amount to distribute across all periods |
| `FROM ... TO` | Date range for the distribution |
| `PERIOD` | Frequency: `MONTHLY`, `QUARTERLY`, or `YEARLY` |
| `PRORATE` | Optional. Allocate by day count instead of even split (for partial periods) |
| `FOR` | Optional. Attach dimensions to all generated journals |
| `DESCRIPTION` | Description text for all generated journals |

**Behavior:**

- **Even split** (default): Each period gets `amount / num_periods`, last period absorbs rounding remainder
- **PRORATE**: Each period gets `amount × days_in_period / total_days`, remainder to last period
- **Journal dates**: Last day of each period, clamped to the end date
- **Amount must not be zero**; end date must be on or after start date

**Examples:**

```sql
-- Revenue recognition: spread $12,000 over 12 months
DISTRIBUTE 12000
  FROM 2024-01-01 TO 2024-12-31
  PERIOD MONTHLY
  FOR Customer='Acme'
  DESCRIPTION 'Revenue recognition - Acme'
  DEBIT @deferred_revenue,
  CREDIT @subscription_revenue;

-- Straight-line depreciation over 5 years
DISTRIBUTE 60000
  FROM 2024-01-01 TO 2028-12-31
  PERIOD YEARLY
  DESCRIPTION 'Annual depreciation - truck'
  DEBIT @depreciation_expense,
  CREDIT @accumulated_depreciation;

-- Prorated insurance across partial months
DISTRIBUTE 2400
  FROM 2024-03-15 TO 2024-06-14
  PERIOD MONTHLY
  PRORATE
  DESCRIPTION 'Insurance amortization'
  DEBIT @insurance_expense,
  CREDIT @prepaid_insurance;
```

---

## BEGIN / COMMIT / ROLLBACK

Explicit ACID transaction control.

**Syntax:**

```sql
BEGIN;
-- statements...
COMMIT;
-- or
ROLLBACK;
```

See [Transactions](/guide/transactions) for details.
