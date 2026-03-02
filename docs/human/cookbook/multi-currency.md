# Cookbook: Multi-Currency

Track transactions in multiple currencies using rates and the `convert()` function.

## 1. Set Up Accounts

```sql
CREATE ACCOUNT @bank_usd ASSET;
CREATE ACCOUNT @bank_eur ASSET;
CREATE ACCOUNT @bank_gbp ASSET;
CREATE ACCOUNT @equity EQUITY;
CREATE ACCOUNT @fx_gain_loss INCOME;
```

## 2. Define Exchange Rates

```sql
CREATE RATE usd_eur;
CREATE RATE usd_gbp;

SET RATE usd_eur 0.92 2024-01-01;
SET RATE usd_eur 0.91 2024-04-01;
SET RATE usd_gbp 0.79 2024-01-01;
SET RATE usd_gbp 0.78 2024-04-01;
```

## 3. Record Transactions

```sql
-- Initial capital in USD
CREATE JOURNAL 2024-01-15, 100000, 'Initial capital'
  DEBIT @bank_usd,
  CREDIT @equity;

-- Convert USD to EUR
CREATE JOURNAL 2024-02-01, 10000, 'USD to EUR conversion'
  DEBIT @bank_eur,
  CREDIT @bank_usd;
```

## 4. Query Converted Values

```sql
-- What's 10,000 USD in EUR today?
GET convert(10000, 'usd_eur', 2024-01-15) AS eur_value;

-- What's the EUR rate at different dates?
GET fx_rate('usd_eur', 2024-01-15) AS jan_rate;
GET fx_rate('usd_eur', 2024-04-15) AS apr_rate;

-- Trial balance in base currency
GET trial_balance(2024-12-31) AS tb;
```

## 5. Track Rate Changes

Rates form a time series. Each `SET RATE` adds a new point. Queries automatically use the most recent rate on or before the requested date:

```sql
-- On 2024-03-15, the rate is still 0.92 (set on Jan 1)
GET fx_rate('usd_eur', 2024-03-15) AS q1_rate;

-- On 2024-05-15, the rate is 0.91 (updated on Apr 1)
GET fx_rate('usd_eur', 2024-05-15) AS q2_rate;
```

## What This Demonstrates

- **Multiple currency accounts** — separate accounts per currency
- **Exchange rate tracking** — rates change over time
- **Currency conversion** — `convert()` applies the correct rate for any date
- **Rate time series** — historical rates preserved and queryable
