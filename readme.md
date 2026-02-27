# DblEntry

**A Layer 2 database for double-entry bookkeeping.**

DblEntry is an open-source accounting engine that sits on top of your existing database infrastructure. Rather than replacing your primary data store, it acts as a specialized **Layer 2** — you bring your own storage (SQLite, PostgreSQL, or in-memory) and DblEntry adds a purpose-built financial data model, query language, and accounting logic on top.

Think of it like a domain-specific compute layer: your Layer 1 database handles persistence and durability, while DblEntry provides the financial semantics — double-entry bookkeeping, dimensional indexing, ACID journal entries, accrual calculations, and multi-currency support — through a dedicated query language called FQL.

### Why Layer 2?

Most financial applications implement accounting logic in application code scattered across services, ORMs, and stored procedures. This leads to duplicated balance calculations, ad-hoc ledger consistency checks, and fragile reporting pipelines. DblEntry consolidates all of that into a single layer that enforces double-entry invariants, maintains specialized indexes tuned for financial queries, and exposes a concise DSL for operations that would otherwise require hundreds of lines of SQL.

- **Your database stays your database.** DblEntry delegates persistence to SQLite or PostgreSQL — it doesn't compete with your existing infrastructure.
- **Financial logic lives in one place.** Accounts, journals, balances, statements, accruals, and trial balances are native primitives, not application-level abstractions.
- **FQL replaces boilerplate.** A single `ACCRUE` statement does what would take a multi-step pipeline of queries, calculations, and inserts in a traditional setup.

## Features

- **FQL (Finance Query Language)** — Domain-specific language for financial operations
- **AI-agent friendly** — Small, deterministic grammar that fits in a system prompt; ideal for LLM tool-use
- **Layer 2 architecture** — Pluggable storage backends; bring your own SQLite or PostgreSQL
- **Double-entry bookkeeping** — Every transaction debits and credits balanced accounts
- **Dimension-based indexing** — Slice data by any combination of tags (Customer, Region, etc.)
- **Variable rate accruals** — Compound interest calculations with fluctuating rates
- **Decimal precision** — Uses `rust_decimal` for exact monetary arithmetic (no floating-point errors)
- **ACID transactions** — `BEGIN` / `COMMIT` / `ROLLBACK` with implicit transaction wrapping
- **Immutable ledger** — Append-only journal entries with sequence numbers and timestamps
- **REST & gRPC APIs** — Full REST API, FQL-over-HTTP, and Protocol Buffers service
- **Authentication** — API key-based auth with role support (admin/writer/reader)
- **Observability** — Structured logging (tracing), Prometheus metrics (`/metrics`), health checks
- **Configurable** — TOML config file, CLI args, environment variable support
- **Multi-currency** — FX rate conversion functions (`convert`, `fx_rate`)
- **Built-in functions** — `balance`, `statement`, `trial_balance`, `income_statement`, `convert`, `round`, `abs`, `min`, `max`

### Built for AI Agents

FQL is a small, deterministic language with a constrained grammar — exactly the kind of interface that LLMs and AI agents excel at generating. Unlike raw SQL, where an agent must reason about schema design, join strategies, and transaction isolation, FQL exposes financial operations as high-level primitives:

```sql
-- An agent can express "record a $500 sale for Acme in the US region" as:
CREATE JOURNAL 2024-03-15, 500, 'Sale'
FOR Customer='Acme', Region='US'
CREDIT @revenue, DEBIT @bank;

-- "What's Acme's balance?" becomes:
GET balance(@receivables, 2024-03-15, Customer='Acme') AS result;

-- "Accrue interest on all loans for February" becomes:
ACCRUE @loans FROM 2024-02-01 TO 2024-02-29
WITH RATE prime COMPOUND DAILY
BY Customer
INTO JOURNAL 2024-03-01, 'Interest'
DEBIT @loans, CREDIT @interest_earned;
```

The entire language fits in a single system prompt. There are no tables to `CREATE`, no schemas to manage, no `INSERT INTO ... SELECT` chains to orchestrate. An agent that knows the handful of FQL verbs (`CREATE ACCOUNT`, `CREATE JOURNAL`, `GET`, `ACCRUE`, `SET RATE`) can perform any accounting operation — and the double-entry invariants are enforced by DblEntry, not by the agent. This makes FQL a natural tool-use interface for financial AI workflows.

## Quick Start

### Build & Run

```bash
cargo build --release
./target/release/dblentry
```

DblEntry will start listening on `0.0.0.0:3000` by default.

### Docker

```bash
docker build -t dblentry .
docker run -p 3000:3000 dblentry
```

### Configuration

Copy `dblentry.toml.example` to `dblentry.toml` and customize:

```toml
[server]
host = "0.0.0.0"
port = 3000

[logging]
level = "info"
json = false

[storage]
backend = "memory"        # "memory", "sqlite", or "postgres"
# sqlite_path = "dblentry.db"  # path for sqlite backend
# postgres_url = "host=localhost user=dblentry password=dblentry dbname=dblentry"

[auth]
enabled = false
# [[auth.api_keys]]
# name = "my-service"
# key = "secret-key"
# role = "admin"

[grpc]
enabled = false
# port = 50051
```

CLI flags override config values:
```bash
dblentry --port 8080 --log-level debug --config /path/to/dblentry.toml
```

## API Endpoints

### FQL (Finance Query Language)

```bash
# Execute FQL scripts
POST /fql
Content-Type: text/plain

CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @equity EQUITY;
```

Response:
```json
{
  "success": true,
  "results": [],
  "metadata": {
    "statements_executed": 2,
    "journals_created": 0
  }
}
```

### REST API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/accounts` | Create account (`{"id": "bank", "account_type": "ASSET"}`) |
| `GET` | `/api/accounts` | List accounts |
| `GET` | `/api/accounts/:id/balance?date=2023-12-31` | Query balance |
| `GET` | `/api/accounts/:id/statement?from=...&to=...` | Get statement |
| `POST` | `/api/journals` | Create journal (JSON body) |
| `POST` | `/api/rates` | Create rate |
| `POST` | `/api/rates/:id` | Set rate value |
| `GET` | `/api/trial-balance?date=...` | Trial balance |

### Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Liveness check |
| `GET` | `/ready` | Readiness check |
| `GET` | `/metrics` | Prometheus metrics |

### gRPC API

Enable with `[grpc] enabled = true` in `dblentry.toml`. Default port: `50051`.

The proto definition is at `proto/dblentry.proto`. Available RPCs:

| RPC | Description |
|-----|-------------|
| `ExecuteFql` | Execute raw FQL queries |
| `CreateAccount` | Create a ledger account |
| `ListAccounts` | List all accounts |
| `GetBalance` | Query account balance at a date |
| `GetStatement` | Get account statement for a period |
| `GetTrialBalance` | Get trial balance at a date |
| `CreateRate` | Create an FX/interest rate |
| `SetRate` | Set rate value at a date |
| `CreateJournal` | Create a journal entry |
| `Health` | Health check |

### Authentication

When `auth.enabled = true`, all API endpoints (except `/health`, `/ready`, `/metrics`) require authentication via:

- `X-API-Key: <key>` header, or
- `Authorization: Bearer <key>` header

## FQL Language Reference

### Accounts

```sql
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @equity EQUITY;
CREATE ACCOUNT @revenue INCOME;
CREATE ACCOUNT @cogs EXPENSE;
CREATE ACCOUNT @payable LIABILITY;
```

Account types: `ASSET`, `LIABILITY`, `EQUITY`, `INCOME`, `EXPENSE`

### Journals

```sql
-- Simple journal
CREATE JOURNAL 2023-01-01, 1000, 'Investment'
CREDIT @equity, DEBIT @bank;

-- Journal with dimensions
CREATE JOURNAL 2023-02-01, 500, 'Sale'
FOR Customer='Acme', Region='US'
CREDIT @revenue, DEBIT @bank;

-- Partial amounts using rates
CREATE JOURNAL 2023-01-01, 100, 'Sale with tax'
CREDIT @revenue,
DEBIT @bank,
CREDIT @tax_payable WITH RATE sales_tax,
DEBIT @bank WITH RATE sales_tax;
```

### Rates

```sql
CREATE RATE prime;
SET RATE prime 0.05 2023-01-01;
SET RATE prime 0.06 2023-07-01;
```

### Queries

```sql
GET balance(@bank, 2023-12-31) AS cash;
GET balance(@loans, 2023-12-31, Customer='John') AS john_balance;
GET statement(@bank, 2023-01-01, 2023-12-31) AS bank_stmt;
GET trial_balance(2023-12-31) AS tb;
```

### Accruals

```sql
ACCRUE @loans FROM 2023-01-01 TO 2023-01-31
WITH RATE prime COMPOUND DAILY
BY Customer
INTO JOURNAL
    2023-02-01, 'Interest'
DEBIT @loans,
CREDIT @interest_earned;
```

Compounding modes: `COMPOUND DAILY`, `COMPOUND CONTINUOUS`

### Transactions

```sql
BEGIN;
CREATE JOURNAL 2023-01-01, 1000, 'Transfer'
CREDIT @savings, DEBIT @checking;
CREATE JOURNAL 2023-01-01, 50, 'Fee'
CREDIT @checking, DEBIT @fees;
COMMIT;
```

Multi-statement FQL scripts are automatically wrapped in an implicit transaction. On any error, all changes are rolled back.

### Built-in Functions

| Function | Description |
|----------|-------------|
| `balance(@acct, date, [dim])` | Account balance at a date, optionally filtered by dimension |
| `statement(@acct, from, to, [dim])` | Account statement for a period |
| `trial_balance(date)` | Trial balance across all accounts |
| `income_statement(from, to)` | P&L report for a period |
| `account_count()` | Number of accounts |
| `convert(amount, 'rate', date)` | Convert amount using an FX rate |
| `fx_rate('rate', date)` | Get rate value at a date |
| `round(value, places)` | Round to N decimal places (default 2) |
| `abs(value)` | Absolute value |
| `min(a, b)` | Minimum of two values |
| `max(a, b)` | Maximum of two values |

### Multi-Currency

```sql
CREATE RATE usd_eur;
SET RATE usd_eur 0.85 2023-01-01;
SET RATE usd_eur 0.92 2023-06-01;

GET convert(1000, 'usd_eur', 2023-07-01) AS euros,
    fx_rate('usd_eur', 2023-07-01) AS rate;
-- euros: 920, rate: 0.92
```

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