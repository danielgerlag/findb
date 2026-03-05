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

## Hierarchical Dimensions

Dimension values support `/` as a path separator, enabling hierarchical grouping. This lets you organize data into trees and query at any level.

### Syntax

Use `/` to separate levels in a dimension value:

```sql
CREATE JOURNAL 2024-01-15, 5000, 'Revenue - US West'
  FOR Region='Americas/US/West', Product='SaaS/Enterprise'
  DEBIT @bank,
  CREDIT @revenue;

CREATE JOURNAL 2024-01-20, 3000, 'Revenue - Canada'
  FOR Region='Americas/Canada', Product='SaaS/Starter'
  DEBIT @bank,
  CREDIT @revenue;
```

### Prefix Query Semantics

Querying at a parent level automatically aggregates all children. The dimension filter matches any value that starts with the given prefix:

```sql
-- All Americas revenue (includes Americas/US/West, Americas/Canada, etc.)
GET balance(@revenue, 2024-12-31, Region='Americas') AS americas_rev;

-- Just US revenue (includes Americas/US/West, Americas/US/East, etc.)
GET balance(@revenue, 2024-12-31, Region='Americas/US') AS us_rev;

-- Exact match still works
GET balance(@revenue, 2024-12-31, Region='Americas/US/West') AS west_rev;
```

### Hierarchical Dimensions with Lots

When an account tracks units (see [Unit-Based Asset Tracking](#unit-based-asset-tracking) below), lots are tagged with their journal's dimensions. This creates **dimensional lot pools**:

```sql
-- Buy shares in different regions
CREATE JOURNAL 2024-01-15, 9000, 'Buy AAPL (US West)'
  FOR Region='Americas/US/West'
  DEBIT @stock_aapl 60 UNITS AT 150,
  CREDIT @bank;

CREATE JOURNAL 2024-02-01, 6200, 'Buy AAPL (US East)'
  FOR Region='Americas/US/East'
  DEBIT @stock_aapl 40 UNITS AT 155,
  CREDIT @bank;
```

Use the `FOR` clause on `SELL` to scope depletion to matching lots. With hierarchical dimensions, this depletes lots across all sub-levels, ordered by date (FIFO):

```sql
-- Sells from Americas/US/West first (oldest), then Americas/US/East
SELL 20 UNITS OF @stock_aapl AT 180 ON 2024-06-15
  FOR Region='Americas'
  PROCEEDS @bank
  GAIN_LOSS @realized_gains
  DESCRIPTION 'Sell Americas shares';
```

## Unit-Based Asset Tracking

DblEntry supports lot-level tracking for assets denominated in units — stocks, bonds, commodities, or any holding where you need to track individual purchase lots, compute gain/loss, and handle splits.

### Creating a Unit-Tracked Account

Link an account to a rate that tracks the asset's price:

```sql
CREATE RATE aapl_price;
CREATE ACCOUNT @stock_aapl ASSET UNITS 'aapl_price';
```

The `UNITS` clause tells DblEntry that this account holds discrete lots. The rate is used by `market_value()` and `unrealized_gain()` to look up the current price.

### Lot Creation

Create lots by using the `UNITS AT` syntax on a `DEBIT` ledger operation:

```sql
-- Buy 50 shares at $150 each ($7,500 total)
CREATE JOURNAL 2024-01-15, 7500, 'Buy AAPL'
  DEBIT @stock_aapl 50 UNITS AT 150,
  CREDIT @bank;

-- Buy another lot at a different price
CREATE JOURNAL 2024-03-01, 8000, 'Buy more AAPL'
  DEBIT @stock_aapl 50 UNITS AT 160,
  CREDIT @bank;
```

Each journal creates a separate lot recording the date, unit count, and cost per unit.

### Selling Units

The `SELL` command depletes lots and records realized gain/loss:

```sql
SELL 30 UNITS OF @stock_aapl AT 175 ON 2024-06-15
  PROCEEDS @bank
  GAIN_LOSS @realized_gains
  DESCRIPTION 'Sell AAPL shares';
```

Choose a cost method:

| Method | Behavior |
|--------|----------|
| `FIFO` (default) | Depletes oldest lots first |
| `LIFO` | Depletes newest lots first |
| `AVERAGE` | Uses weighted average cost basis across all lots |

```sql
-- Explicit LIFO
SELL 20 UNITS OF @stock_aapl AT 180 ON 2024-07-01
  METHOD LIFO
  PROCEEDS @bank
  GAIN_LOSS @realized_gains
  DESCRIPTION 'LIFO sale';
```

### Stock Splits

The `SPLIT` command adjusts all open lots proportionally:

```sql
-- 2-for-1 split: doubles units, halves cost per unit
SPLIT @stock_aapl 2 FOR 1 2024-08-01;

-- 3-for-2 split
SPLIT @stock_aapl 3 FOR 2 2024-09-15;
```

After a 2-for-1 split, a lot of 50 shares at $150 becomes 100 shares at $75.

### Query Functions

| Function | Returns |
|----------|---------|
| `units(@acct, date)` | Total units held |
| `market_value(@acct, date)` | Units × current rate |
| `unrealized_gain(@acct, date)` | Market value − cost basis |
| `cost_basis(@acct, date)` | Weighted average cost per unit |
| `lots(@acct, date)` | Table of open lots (date, units, cost per unit) |

All accept an optional dimension filter for scoped queries:

```sql
GET units(@stock_aapl, 2024-06-30) AS total_shares;
GET market_value(@stock_aapl, 2024-06-30) AS portfolio_value;
GET unrealized_gain(@stock_aapl, 2024-06-30, Region='Americas/US') AS us_gain;
GET lots(@stock_aapl, 2024-06-30) AS open_lots;
```

## Precision

All monetary values in DblEntry use `rust_decimal::Decimal` (128-bit), never floating point. Numbers in FQL are parsed as strings and converted losslessly — there are no rounding surprises from IEEE 754.
