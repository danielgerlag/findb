# FinanceDB

FinanceDB is a domain specific database for finance. It is an open-source accounting primitive, a building block upon which financial products can be built. It natively supports double-entry bookkeeping, multi-currency transactions, complex tax rules, and various accounting standards. It maintains specialized indexes tuned for financial use cases that enable fast and accurate queries and calculations.

FinanceDB is not just a data store. It also provides a domain specific query language (DSL) crafted for financial use cases, that is expressive, concise, and intuitive. It is based on the principles of double-entry bookkeeping and supports common financial concepts, such as accounts, journals, debits, credits, adjustments, reversals, accruals, deferrals, allocations, budgets, forecasts, and reports.  It enables developers and non-developers alike to perform complex financial operations and analyses with simple and readable syntax, such as calculating interest with fluctuating rates, amortization, cash flow, net present value, internal rate of return, and more. 

FinanceDB is an accounting ledger at its core, so standardized financial concepts and statements (balance sheet, income statement, etc..) are native to the platform.

## Project Status

FinanceDB is current in a proof-of-concept stage.  You can download the source and spin up an instance which will expose an API endpoint on port 3000.  You can then HTTP POST plain text scripts to this API to interact with it.  Currently storage is only in-memory at this stage and all data will be lost when the server shuts down.

## Example use case: Lending Fund

We will create a lending fund to illustrate some of the key concepts.  The fund will be a distinct legal entity where investors deposit money into a pooled fund that in-turn lends out to borrowers and charges interest on the outstanding amount.

First, let’s create the ledger accounts required.

```
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @loans ASSET;
CREATE ACCOUNT @interest_earned INCOME;
CREATE ACCOUNT @equity EQUITY;
```

 
Now, let’s define a rate named “prime” to track our variable interest rate, and set it to 5% on 1 January and then increase it to 6% on 15 February.

```
CREATE RATE prime;
SET RATE prime 0.05 2023-01-01;
SET RATE prime 0.06 2023-02-15;
```
 

Now, let’s journal an investment transaction from Frank for $20,000.  The “Investor” tag that we include on the journal will enable us to track balances on the underlying ledger account segmented per investor.  You can add as many of these tags as you wish.
The journal will post a CREDIT to the ‘equity’ account and a corresponding DEBIT to the ‘bank’ account.

```
CREATE JOURNAL 
    2023-01-01, 20000, 'Investment'
FOR
    Investor='Frank'
CREDIT @equity,
DEBIT @bank;
```
 

Now, let’s journal the issue of two loans to different customers.  We’ll include a second tag of region for these so that we can track loan balances on multiple dimensions.  We’ll issue a loan of $1000 to John and a loan of $500 to Joe.
 
```
CREATE JOURNAL 
    2023-02-01, 1000, 'Loan Issued'
FOR
    Customer='John',
    Region='US'
DEBIT @loans,
CREDIT @bank;

CREATE JOURNAL 
    2023-02-01, 500, 'Loan Issued'
FOR
    Customer='Joe',
    Region='US'
DEBIT @loans,
CREDIT @bank;
```

Let’s generate some statements against the ‘loans’ account.  We’ll generate one for each customer and one for the US region as a whole for the month of February.

```
GET 
    statement(@loans, 2023-02-01, 2023-03-01, Customer='John') as John,
    statement(@loans, 2023-02-01, 2023-03-01, Customer='Joe') as Joe,
    statement(@loans, 2023-02-01, 2023-03-01, Region='US') as US
```

For each customer we can see the “Loan Issued” transaction, and for the region we can see both and a running balance. 

```
Joe: 
+------------+-------------+--------+---------+
| Date       | Description | Amount | Balance |
+------------+-------------+--------+---------+
|            |             |        |         |
+------------+-------------+--------+---------+
| 2023-02-01 | Loan Issued | 500    | 500     |
+------------+-------------+--------+---------+

John: 
+------------+-------------+--------+---------+
| Date       | Description | Amount | Balance |
+------------+-------------+--------+---------+
|            |             |        |         |
+------------+-------------+--------+---------+
| 2023-02-01 | Loan Issued | 1000   | 1000    |
+------------+-------------+--------+---------+

US: 
+------------+-------------+--------+---------+
| Date       | Description | Amount | Balance |
+------------+-------------+--------+---------+
|            |             |        |         |
+------------+-------------+--------+---------+
| 2023-02-01 | Loan Issued | 1000   | 1000    |
+------------+-------------+--------+---------+
| 2023-02-01 | Loan Issued | 500    | 1500    |
+------------+-------------+--------+---------+
```

At the end of February, we want to journal the interest transaction for every customer, considering any mid-month fluctuations in the interest rate or additional transactions per customer that may affect the interest accrued.  We use the ‘prime’ rate that we created earlier and calculate with daily compounding, and then write a journal with the description of ‘Interest’ on 1 March for each customer.  This journal will post to the ‘loans’ account (capitalizing the interest) and the ‘interest earned’ account so that we can track profits per customer.

```
ACCRUE @loans FROM 2023-02-01 TO 2023-02-28
WITH RATE prime COMPOUND DAILY
BY Customer
INTO JOURNAL
    2023-03-01, 'Interest'
DEBIT @loans,
CREDIT @interest_earned;

----------------
journals_created: 2

```
 
Let’s pull another statement for each customer, the total balance of our loan book and generate a trial balance for the fund as a whole:

```
GET 
    statement(@loans, 2023-02-01, 2023-03-01, Customer='John') as John,
    statement(@loans, 2023-02-01, 2023-03-01, Customer='Joe') as Joe,
    balance(@loans, 2023-03-01) AS LoanBookTotal,
    trial_balance(2023-03-01) AS TrialBalance
```

 
We can now see the “Loan Issued” and “Interest” transactions for each customer with running balances, the total value of our loan book and the trial balance of the entire fund (which is in balance).

```
Joe: 
+------------+-------------+--------+---------+
| Date       | Description | Amount | Balance |
+------------+-------------+--------+---------+
|            |             |        |         |
+------------+-------------+--------+---------+
| 2023-02-01 | Loan Issued | 500    | 500     |
+------------+-------------+--------+---------+
| 2023-03-01 | Interest    | 2.11   | 502.11  |
+------------+-------------+--------+---------+

John: 
+------------+-------------+--------+---------+
| Date       | Description | Amount | Balance |
+------------+-------------+--------+---------+
|            |             |        |         |
+------------+-------------+--------+---------+
| 2023-02-01 | Loan Issued | 1000   | 1000    |
+------------+-------------+--------+---------+
| 2023-03-01 | Interest    | 4.23   | 1004.23 |
+------------+-------------+--------+---------+

LoanBookTotal: 1506.34

TrialBalance: 
+-----------------+---------+--------+
| Account         | Debit   | Credit |
+-----------------+---------+--------+
|                 |         |        |
+-----------------+---------+--------+
| bank            | 18500   |        |
+-----------------+---------+--------+
| equity          |         | 20000  |
+-----------------+---------+--------+
| interest_earned |         | 6.34   |
+-----------------+---------+--------+
| loans           | 1506.34 |        |
+-----------------+---------+--------+
```
 


## Example: How to add sales taxes

Create bank, sales and tax payable accounts.  Define a tax rate of 5% effective 1 January.

```
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @sales INCOME;
CREATE ACCOUNT @tax_payable LIABILITY;

CREATE RATE sales_tax;
SET RATE sales_tax 0.05 2023-01-01;
```

Record a sales transaction, with additional ledger postings to track the sales tax.

```
CREATE JOURNAL 
    2023-01-01, 100, 'Sales'
FOR
    Customer='John Doe'
CREDIT @sales,
DEBIT @bank,
CREDIT @tax_payable WITH RATE sales_tax,
DEBIT @bank WITH RATE sales_tax;
```

Pull the balances:

```
GET 
    balance(@bank, 2023-03-01) AS BankBalance,
    trial_balance(2023-03-01) AS TrialBalance
```

```
BankBalance: 105

TrialBalance: 
+-------------+-------+--------+
| Account     | Debit | Credit |
+-------------+-------+--------+
|             |       |        |
+-------------+-------+--------+
| bank        | 105   |        |
+-------------+-------+--------+
| sales       |       | 100    |
+-------------+-------+--------+
| tax_payable |       | 5      |
+-------------+-------+--------+
```