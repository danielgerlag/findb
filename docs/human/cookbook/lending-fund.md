# Cookbook: Lending Fund

Build a complete lending fund from scratch — investor equity, loan issuance, interest accrual, and financial reporting.

## 1. Set Up the Chart of Accounts

```sql
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @loans ASSET;
CREATE ACCOUNT @interest_receivable ASSET;
CREATE ACCOUNT @interest_income INCOME;
CREATE ACCOUNT @equity EQUITY;
```

## 2. Define the Interest Rate

```sql
CREATE RATE prime;
SET RATE prime 0.05 2024-01-01;
```

## 3. Accept Investor Capital

```sql
CREATE JOURNAL 2024-01-01, 20000, 'Investor capital contribution'
  DEBIT @bank,
  CREDIT @equity;
```

## 4. Issue Loans

Tag each loan with a `customer` dimension for tracking:

```sql
CREATE JOURNAL 2024-01-15, 1500, 'Loan disbursement - Acme'
  FOR customer='Acme'
  DEBIT @loans,
  CREDIT @bank;

CREATE JOURNAL 2024-01-20, 3000, 'Loan disbursement - Globex'
  FOR customer='Globex'
  DEBIT @loans,
  CREDIT @bank;
```

## 5. Accrue Interest

Calculate daily interest for each borrower and create journal entries:

```sql
ACCRUE @loans FROM 2024-01-15 TO 2024-01-31
  WITH RATE prime
  COMPOUND DAILY
  BY customer
  INTO JOURNAL 2024-02-01, 'January interest accrual'
  DEBIT @interest_receivable,
  CREDIT @interest_income;
```

## 6. View Reports

```sql
-- Overall position
GET trial_balance(2024-02-01) AS tb;

-- Cash position
GET balance(@bank, 2024-02-01) AS cash;

-- Total loans outstanding
GET balance(@loans, 2024-02-01) AS total_loans;

-- Loans per customer
GET balance(@loans, 2024-02-01, customer='Acme') AS acme;
GET balance(@loans, 2024-02-01, customer='Globex') AS globex;

-- Loan activity detail
GET statement(@loans, 2024-01-01, 2024-02-01) AS loan_activity;

-- P&L for the period
GET income_statement(2024-01-01, 2024-02-01) AS pnl;
```

## What This Demonstrates

- **Chart of accounts** with all five account types
- **Dimensional tracking** — per-customer loan balances without separate accounts
- **Time-varying rates** — interest rate changes are handled automatically
- **Automated accrual** — compound interest calculated per dimension
- **Financial reporting** — trial balance, income statement, account statements
