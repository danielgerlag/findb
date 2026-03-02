# Core Concepts

## Journals

A **journal** is a double-entry transaction. Every journal has:

- A **date** — when the transaction occurred
- An **amount** — the total value of the transaction
- A **description** — human-readable label
- **Ledger operations** — debits and credits that must balance

```sql
CREATE JOURNAL 2024-01-15, 500, 'Loan to Acme'
  DEBIT @loans,
  CREDIT @bank;
```

### Split Entries

A journal can have multiple debits and/or credits. You can specify amounts as fixed values or percentages:

```sql
-- Split by percentage
CREATE JOURNAL 2024-03-01, 1000, 'Revenue split'
  DEBIT @bank,
  CREDIT @product_revenue 70%,
  CREDIT @service_revenue 30%;

-- Split by fixed amount
CREATE JOURNAL 2024-03-01, 1000, 'Revenue split'
  DEBIT @bank,
  CREDIT @product_revenue 700,
  CREDIT @service_revenue 300;
```

## Dimensions

**Dimensions** are key-value tags on journal entries that enable sub-ledger analysis. They let you filter balances and statements without creating separate accounts.

```sql
CREATE JOURNAL 2024-02-01, 500, 'Loan to Acme'
  FOR customer='Acme', product='Term Loan'
  DEBIT @loans,
  CREDIT @bank;

CREATE JOURNAL 2024-02-15, 800, 'Loan to Globex'
  FOR customer='Globex', product='Credit Line'
  DEBIT @loans,
  CREDIT @bank;
```

Now you can query by dimension:

```sql
-- Balance for a specific customer
GET balance(@loans, 2024-12-31, customer='Acme') AS acme_balance;

-- Statement filtered by dimension
GET statement(@loans, 2024-01-01, 2024-12-31, customer='Acme') AS acme_stmt;
```

## Rates

**Rates** are time-varying values — useful for interest rates, exchange rates, tax rates, or any value that changes over time.

```sql
CREATE RATE prime;
SET RATE prime 0.05 2024-01-01;
SET RATE prime 0.055 2024-07-01;
```

When you query a rate, DblEntry returns the value effective at the given date (the most recent value on or before that date):

```sql
GET fx_rate('prime', 2024-03-15) AS rate;  -- Returns 0.05
GET fx_rate('prime', 2024-09-01) AS rate;  -- Returns 0.055
```

### Currency Conversion

Use `convert()` to apply a rate to an amount:

```sql
CREATE RATE usd_eur;
SET RATE usd_eur 0.92 2024-01-01;

GET convert(1000, 'usd_eur', 2024-01-15) AS eur_amount;  -- 920.00
```

## Accruals

The `ACCRUE` statement automates interest accrual. It calculates daily interest on per-dimension balances and creates journal entries.

```sql
ACCRUE @loans FROM 2024-01-01 TO 2024-01-31
  WITH RATE prime
  COMPOUND DAILY
  BY customer
  INTO JOURNAL 2024-02-01, 'January interest accrual'
  DEBIT @interest_receivable,
  CREDIT @interest_income;
```

This:
1. Gets the balance of `@loans` for each unique `customer` dimension value
2. Calculates daily interest using the `prime` rate
3. Creates one journal entry per customer with the accrued amount

### Compounding Methods

| Method | Formula | Use Case |
|--------|---------|----------|
| *(none)* | `balance × rate` | Simple interest |
| `COMPOUND DAILY` | `balance × rate / 365` | Standard daily compounding |
| `COMPOUND CONTINUOUS` | `balance × rate` | Continuous compounding |

## Precision

All monetary values in DblEntry use `rust_decimal::Decimal` (128-bit), never floating point. Numbers in FQL are parsed as strings and converted losslessly — there are no rounding surprises from IEEE 754.
