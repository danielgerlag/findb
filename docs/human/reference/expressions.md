# Expressions & Operators

## Literals

| Type | Syntax | Examples |
|------|--------|---------|
| Integer | `-?[0-9]+` | `42`, `-1`, `0` |
| Decimal | `-?[0-9]+\.[0-9]+` | `100.50`, `-0.53` |
| Percentage | `[0-9]+(\.[0-9]+)?%` | `50%`, `3.5%` |
| Text | `'...'` | `'Hello'`, `'O''Brien'` |
| Date | `YYYY-MM-DD` | `2024-01-15` |
| Account ID | `@name` | `@bank`, `@loans` |
| Boolean | `TRUE` / `FALSE` | |
| Null | `NULL` | |
| Parameter | `$name` | `$amount` |

### Text Escaping

Single quotes are escaped by doubling them:

```sql
CREATE JOURNAL 2024-01-01, 100, 'O''Brien''s payment'
  DEBIT @bank,
  CREDIT @revenue;
```

## Operators

Listed from lowest to highest precedence:

| Precedence | Operator | Description | Example |
|-----------|----------|-------------|---------|
| 1 | `AND` | Logical AND | `a > 0 AND b > 0` |
| 1 | `OR` | Logical OR | `a > 0 OR b > 0` |
| 2 | `NOT` | Logical negation | `NOT a > 0` |
| 3 | `=` | Equal | `a = 5` |
| 3 | `<>`, `!=` | Not equal | `a <> 5` |
| 3 | `<`, `<=` | Less than (or equal) | `a < 10` |
| 3 | `>`, `>=` | Greater than (or equal) | `a > 0` |
| 3 | `IN` | List membership | `a IN [1, 2, 3]` |
| 4 | `+`, `-` | Addition, subtraction | `a + b` |
| 5 | `*`, `/` | Multiplication, division | `a * 1.1` |
| 6 | `%` | Modulo | `a % 2` |
| 6 | `^` | Exponentiation | `a ^ 2` |
| 7 | `IS NULL` | Null check | `a IS NULL` |
| 7 | `IS NOT NULL` | Not-null check | `a IS NOT NULL` |

## Special Expressions

### CASE

Conditional logic:

```sql
GET CASE
  WHEN balance(@bank, 2024-12-31) > 10000 THEN 'Healthy'
  WHEN balance(@bank, 2024-12-31) > 0 THEN 'Low'
  ELSE 'Overdrawn'
END AS status;
```

### WITH RATE

References a rate value in an expression:

```sql
-- Used in ACCRUE statement
ACCRUE @loans FROM 2024-01-01 TO 2024-01-31
  WITH RATE prime
  ...
```

### Lists

```sql
-- Used with IN operator
GET CASE WHEN 5 IN [1, 2, 3, 4, 5] THEN 'found' ELSE 'missing' END AS result;
```

### Property Access

```sql
-- Access named properties on objects
identifier.key
```

## Arithmetic Notes

- Division by zero returns an error (does not panic)
- Modulo by zero returns an error
- All arithmetic uses 128-bit `Decimal` — no floating point
