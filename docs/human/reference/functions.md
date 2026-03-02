# Functions

All built-in functions available in FQL `GET` expressions.

## Account Functions

### `balance()`

Returns the balance of an account at a given date, optionally filtered by dimension.

```sql
GET balance(@bank, 2024-12-31) AS cash;
GET balance(@loans, 2024-12-31, customer='Acme') AS acme_loans;
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `account` | `@account_id` | Yes | The account to query |
| `date` | `YYYY-MM-DD` | Yes | Effective date |
| `dimension` | `key=value` | No | Filter by dimension |

**Returns:** Decimal balance.

---

### `statement()`

Returns a table of ledger entries for an account over a date range.

```sql
GET statement(@bank, 2024-01-01, 2024-12-31) AS activity;
GET statement(@loans, 2024-01-01, 2024-12-31, customer='Acme') AS acme;
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `account` | `@account_id` | Yes | The account to query |
| `from` | `YYYY-MM-DD` | Yes | Start date (inclusive) |
| `to` | `YYYY-MM-DD` | Yes | End date (inclusive) |
| `dimension` | `key=value` | No | Filter by dimension |

**Returns:** Table with columns: `Date`, `Description`, `Amount`, `Balance`.

---

### `trial_balance()`

Returns all accounts with their debit and credit balances at a given date.

```sql
GET trial_balance(2024-12-31) AS tb;
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `date` | `YYYY-MM-DD` | Yes | Effective date |

**Returns:** Table with columns: `Account`, `Debit`, `Credit`.

---

### `income_statement()`

Returns income and expense account changes over a date range.

```sql
GET income_statement(2024-01-01, 2024-12-31) AS pnl;
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `from` | `YYYY-MM-DD` | Yes | Start date |
| `to` | `YYYY-MM-DD` | Yes | End date |

**Returns:** Table with columns: `Account`, `Debit`, `Credit`.

---

### `account_count()`

Returns the number of accounts in the active entity.

```sql
GET account_count() AS n;
```

**Returns:** Integer.

---

## Rate Functions

### `fx_rate()`

Returns the value of a rate at a given date. Uses the most recent value set on or before that date.

```sql
GET fx_rate('prime', 2024-06-15) AS rate;
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | `'text'` | Yes | Rate identifier (single-quoted) |
| `date` | `YYYY-MM-DD` | Yes | Effective date |

**Returns:** Decimal rate value.

---

### `convert()`

Multiplies an amount by a rate value at a given date. Equivalent to `amount × fx_rate(name, date)`.

```sql
GET convert(1000, 'usd_eur', 2024-01-15) AS eur_amount;
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `amount` | Decimal | Yes | Amount to convert |
| `name` | `'text'` | Yes | Rate identifier |
| `date` | `YYYY-MM-DD` | Yes | Effective date |

**Returns:** Decimal converted amount.

---

## Math Functions

### `round()`

Rounds a value to the specified number of decimal places (default: 2).

```sql
GET round(100.556) AS r;     -- 100.56
GET round(100.556, 1) AS r;  -- 100.6
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `value` | Decimal | Yes | Value to round |
| `places` | Integer | No | Decimal places (default: 2) |

---

### `abs()`

Returns the absolute value.

```sql
GET abs(-42.5) AS a;  -- 42.5
```

---

### `min()`

Returns the smaller of two values.

```sql
GET min(100, 200) AS m;  -- 100
```

---

### `max()`

Returns the larger of two values.

```sql
GET max(100, 200) AS m;  -- 200
```
