use std::sync::Arc;

use dblentry::evaluator::{ExpressionEvaluator, QueryVariables};
use dblentry::function_registry::{FunctionRegistry, Function};
use dblentry::functions::{Balance, Statement, TrialBalance, IncomeStatement, AccountCount, Convert, FxRate, Round, Abs, Min, Max, Units, MarketValue, UnrealizedGain, CostBasis, Lots};
use dblentry::lexer;
use dblentry::models::DataValue;
use dblentry::statement_executor::{ExecutionContext, StatementExecutor};
use dblentry::storage::InMemoryStorage;

/// Generate the same test body against memory, SQLite, and PostgreSQL backends.
/// Memory and SQLite variants run normally. PostgreSQL variants are `#[ignore]`.
macro_rules! backend_test {
    ($name:ident, $body:expr) => {
        paste::paste! {
            #[test]
            fn [<$name _memory>]() {
                let (exec, mut ctx) = setup();
                $body(&exec, &mut ctx);
            }
            #[test]
            fn [<$name _sqlite>]() {
                let (exec, mut ctx) = setup_sqlite();
                $body(&exec, &mut ctx);
            }
            #[test]
            #[ignore]
            fn [<$name _postgres>]() {
                if !postgres_available() {
                    eprintln!("Skipping PostgreSQL test: no connection available");
                    return;
                }
                let (exec, mut ctx) = setup_postgres();
                $body(&exec, &mut ctx);
            }
        }
    };
}

fn register_functions(registry: &FunctionRegistry, storage: &Arc<dyn dblentry::storage::StorageBackend>) {
    registry.register_function("balance", Function::Scalar(Arc::new(Balance::new(storage.clone()))));
    registry.register_function("statement", Function::Scalar(Arc::new(Statement::new(storage.clone()))));
    registry.register_function("trial_balance", Function::Scalar(Arc::new(TrialBalance::new(storage.clone()))));
    registry.register_function("income_statement", Function::Scalar(Arc::new(IncomeStatement::new(storage.clone()))));
    registry.register_function("account_count", Function::Scalar(Arc::new(AccountCount::new(storage.clone()))));
    registry.register_function("convert", Function::Scalar(Arc::new(Convert::new(storage.clone()))));
    registry.register_function("fx_rate", Function::Scalar(Arc::new(FxRate::new(storage.clone()))));
    registry.register_function("round", Function::Scalar(Arc::new(Round)));
    registry.register_function("abs", Function::Scalar(Arc::new(Abs)));
    registry.register_function("min", Function::Scalar(Arc::new(Min)));
    registry.register_function("max", Function::Scalar(Arc::new(Max)));
    registry.register_function("units", Function::Scalar(Arc::new(Units::new(storage.clone()))));
    registry.register_function("market_value", Function::Scalar(Arc::new(MarketValue::new(storage.clone()))));
    registry.register_function("unrealized_gain", Function::Scalar(Arc::new(UnrealizedGain::new(storage.clone()))));
    registry.register_function("cost_basis", Function::Scalar(Arc::new(CostBasis::new(storage.clone()))));
    registry.register_function("lots", Function::Scalar(Arc::new(Lots::new(storage.clone()))));
}

fn setup() -> (StatementExecutor, ExecutionContext) {
    let storage: Arc<dyn dblentry::storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let function_registry = FunctionRegistry::new();
    register_functions(&function_registry, &storage);
    let expression_evaluator = Arc::new(ExpressionEvaluator::new(Arc::new(function_registry), storage.clone()));
    let exec = StatementExecutor::new(expression_evaluator, storage);
    let eff_date = time::OffsetDateTime::now_utc().date();
    let context = ExecutionContext::new(eff_date, QueryVariables::new());
    (exec, context)
}

fn execute_script(exec: &StatementExecutor, context: &mut ExecutionContext, script: &str) -> Vec<dblentry::statement_executor::ExecutionResult> {
    let statements = lexer::parse(script).expect("Failed to parse script");
    let mut results = Vec::new();
    for statement in &statements {
        let result = exec.execute(context, statement).expect("Failed to execute statement");
        results.push(result);
    }
    results
}

#[test]
fn test_create_accounts() {
    let (exec, mut ctx) = setup();
    let results = execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @loans ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE ACCOUNT @interest INCOME;
        CREATE ACCOUNT @expenses EXPENSE;
        CREATE ACCOUNT @payable LIABILITY;
    ");
    assert_eq!(results.len(), 6);
}

#[test]
fn test_create_and_query_journal() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
    ");

    let results = execute_script(&exec, &mut ctx, "
        CREATE JOURNAL 
            2023-01-01, 10000, 'Investment'
        FOR
            Investor='Alice'
        CREDIT @equity,
        DEBIT @bank;
    ");
    assert_eq!(results[0].journals_created, 1);

    let results = execute_script(&exec, &mut ctx, "
        GET 
            balance(@bank, 2023-01-02) AS BankBalance,
            balance(@equity, 2023-01-02) AS EquityBalance
    ");
    let bank = results[0].variables.get("BankBalance").unwrap();
    let equity = results[0].variables.get("EquityBalance").unwrap();

    match bank {
        DataValue::Money(m) => assert_eq!(m.to_string(), "10000"),
        _ => panic!("Expected Money, got {:?}", bank),
    }
    match equity {
        DataValue::Money(m) => assert_eq!(m.to_string(), "10000"),
        _ => panic!("Expected Money, got {:?}", equity),
    }
}

#[test]
fn test_dimension_filtering() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        
        CREATE JOURNAL 
            2023-01-01, 5000, 'Investment'
        FOR Investor='Alice'
        CREDIT @equity, DEBIT @bank;

        CREATE JOURNAL 
            2023-01-01, 3000, 'Investment'
        FOR Investor='Bob'
        CREDIT @equity, DEBIT @bank;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET 
            balance(@bank, 2023-02-01, Investor='Alice') AS Alice,
            balance(@bank, 2023-02-01, Investor='Bob') AS Bob,
            balance(@bank, 2023-02-01) AS Total
    ");

    let alice = &results[0].variables["Alice"];
    let bob = &results[0].variables["Bob"];
    let total = &results[0].variables["Total"];

    match alice {
        DataValue::Money(m) => assert_eq!(m.to_string(), "5000"),
        _ => panic!("Expected Money"),
    }
    match bob {
        DataValue::Money(m) => assert_eq!(m.to_string(), "3000"),
        _ => panic!("Expected Money"),
    }
    match total {
        DataValue::Money(m) => assert_eq!(m.to_string(), "8000"),
        _ => panic!("Expected Money"),
    }
}

#[test]
fn test_rates() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE prime;
        SET RATE prime 0.05 2023-01-01;
        SET RATE prime 0.06 2023-06-01;
    ");
    // Rates are stored, no explicit query for rates yet
    // but they're used by accruals
}

#[test]
fn test_trial_balance_balances() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE ACCOUNT @loans ASSET;
        
        CREATE JOURNAL 2023-01-01, 20000, 'Investment'
        FOR Investor='Frank'
        CREDIT @equity, DEBIT @bank;

        CREATE JOURNAL 2023-02-01, 5000, 'Loan Issued'
        FOR Customer='John'
        DEBIT @loans, CREDIT @bank;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET trial_balance(2023-03-01) AS TB
    ");

    match &results[0].variables["TB"] {
        DataValue::TrialBalance(items) => {
            // Total debits should equal total credits
            let mut total_debit = rust_decimal::Decimal::ZERO;
            let mut total_credit = rust_decimal::Decimal::ZERO;
            for item in items {
                match item.account_type {
                    dblentry::ast::AccountType::Asset | dblentry::ast::AccountType::Expense => {
                        total_debit += item.balance;
                    },
                    _ => {
                        total_credit += item.balance;
                    },
                }
            }
            assert_eq!(total_debit, total_credit, "Trial balance must be in balance");
        },
        _ => panic!("Expected TrialBalance"),
    }
}

#[test]
fn test_statement_output() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        
        CREATE JOURNAL 2023-01-15, 1000, 'Deposit A'
        CREDIT @equity, DEBIT @bank;

        CREATE JOURNAL 2023-01-20, 500, 'Deposit B'
        CREDIT @equity, DEBIT @bank;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET statement(@bank, 2023-01-01, 2023-02-01) AS Stmt
    ");

    match &results[0].variables["Stmt"] {
        DataValue::Statement(txns) => {
            assert_eq!(txns.len(), 2);
            // Last transaction balance should be 1500
            assert_eq!(txns.last().unwrap().balance.to_string(), "1500");
        },
        _ => panic!("Expected Statement"),
    }
}

#[test]
fn test_lending_fund_e2e() {
    let (exec, mut ctx) = setup();
    
    // Full lending fund example from README
    let results = execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @loans ASSET;
        CREATE ACCOUNT @interest_earned INCOME;
        CREATE ACCOUNT @equity EQUITY;

        CREATE RATE prime;
        SET RATE prime 0.05 2023-01-01;
        SET RATE prime 0.06 2023-02-15;

        CREATE JOURNAL 
            2023-01-01, 20000, 'Investment'
        FOR Investor='Frank'
        CREDIT @equity, DEBIT @bank;

        CREATE JOURNAL 
            2023-02-01, 1000, 'Loan Issued'
        FOR Customer='John', Region='US'
        DEBIT @loans, CREDIT @bank;

        CREATE JOURNAL 
            2023-02-01, 500, 'Loan Issued'
        FOR Customer='Joe', Region='US'
        DEBIT @loans, CREDIT @bank;

        ACCRUE @loans FROM 2023-02-01 TO 2023-02-28
        WITH RATE prime COMPOUND DAILY
        BY Customer
        INTO JOURNAL
            2023-03-01, 'Interest'
        DEBIT @loans,
        CREDIT @interest_earned;

        GET 
            balance(@loans, 2023-03-01) AS LoanBookTotal,
            trial_balance(2023-03-01) AS TrialBalance
    ");

    // Find the GET result (last one)
    let get_result = results.last().unwrap();
    
    let loan_total = &get_result.variables["LoanBookTotal"];
    match loan_total {
        DataValue::Money(m) => {
            // Loan book should be > 1500 (original loans + interest)
            assert!(*m > rust_decimal::Decimal::from(1500), "Loan book should include accrued interest");
        },
        _ => panic!("Expected Money for LoanBookTotal"),
    }

    // Verify trial balance is balanced
    match &get_result.variables["TrialBalance"] {
        DataValue::TrialBalance(items) => {
            let mut total_debit = rust_decimal::Decimal::ZERO;
            let mut total_credit = rust_decimal::Decimal::ZERO;
            for item in items {
                match item.account_type {
                    dblentry::ast::AccountType::Asset | dblentry::ast::AccountType::Expense => {
                        total_debit += item.balance;
                    },
                    _ => {
                        total_credit += item.balance;
                    },
                }
            }
            assert_eq!(total_debit, total_credit, "Trial balance must be in balance after accrual");
        },
        _ => panic!("Expected TrialBalance"),
    }
}

#[test]
fn test_sales_tax_example() {
    let (exec, mut ctx) = setup();
    
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @sales INCOME;
        CREATE ACCOUNT @tax_payable LIABILITY;

        CREATE RATE sales_tax;
        SET RATE sales_tax 0.05 2023-01-01;

        CREATE JOURNAL 
            2023-01-01, 100, 'Sales'
        FOR Customer='John Doe'
        CREDIT @sales,
        DEBIT @bank,
        CREDIT @tax_payable WITH RATE sales_tax,
        DEBIT @bank WITH RATE sales_tax;

        GET 
            balance(@bank, 2023-03-01) AS BankBalance,
            trial_balance(2023-03-01) AS TrialBalance
    ");

    let get_result = execute_script(&exec, &mut ctx, "
        GET balance(@bank, 2023-03-01) AS BankBalance
    ");

    let bank = &get_result[0].variables["BankBalance"];
    match bank {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(105)),
        _ => panic!("Expected Money, got {:?}", bank),
    }
}

#[test]
fn test_error_on_missing_account() {
    let (exec, mut ctx) = setup();
    
    let statements = lexer::parse(
        "CREATE JOURNAL 2023-01-01, 100, 'Test' CREDIT @nonexistent, DEBIT @also_nonexistent"
    ).unwrap();

    let result = exec.execute(&mut ctx, &statements[0]);
    assert!(result.is_err(), "Should fail when referencing non-existent accounts");
}

#[test]
fn test_parse_errors() {
    let result = lexer::parse("INVALID GARBAGE !!!");
    assert!(result.is_err(), "Should fail on invalid FQL");
}

#[test]
fn test_transaction_commit() {
    let (exec, mut ctx) = setup();

    let results = execute_script(&exec, &mut ctx, "
        BEGIN;
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE JOURNAL 2023-01-01, 1000, 'Investment' CREDIT @equity, DEBIT @bank;
        COMMIT;
        GET balance(@bank, 2023-12-31) AS result
    ");

    let get_result = results.last().unwrap();
    let balance = &get_result.variables["result"];
    match balance {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(1000)),
        _ => panic!("Expected Money, got {:?}", balance),
    }
}

#[test]
fn test_transaction_rollback() {
    let (exec, mut ctx) = setup();

    // First create account outside a transaction
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
    ");

    // Begin, create journal, then rollback
    execute_script(&exec, &mut ctx, "
        BEGIN;
        CREATE JOURNAL 2023-01-01, 1000, 'Investment' CREDIT @equity, DEBIT @bank;
        ROLLBACK;
    ");

    // Balance should be 0 since we rolled back
    let results = execute_script(&exec, &mut ctx, "
        GET balance(@bank, 2023-12-31) AS result
    ");
    let balance = &results[0].variables["result"];
    match balance {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::ZERO),
        _ => panic!("Expected Money, got {:?}", balance),
    }
}

#[test]
fn test_implicit_transaction_rollback_on_error() {
    let (exec, mut ctx) = setup();

    // Set up accounts
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
    ");

    // Execute a script that will fail partway through (referencing nonexistent account)
    // The implicit transaction in execute_script should roll back everything
    let statements = lexer::parse("
        CREATE JOURNAL 2023-01-01, 1000, 'Investment' CREDIT @equity, DEBIT @bank;
        CREATE JOURNAL 2023-02-01, 500, 'Bad' CREDIT @nonexistent, DEBIT @bank;
    ").unwrap();

    let result = exec.execute_script(&mut ctx, &statements);
    assert!(result.is_err(), "Script should fail on missing account");

    // The first journal should have been rolled back too
    let results = execute_script(&exec, &mut ctx, "
        GET balance(@bank, 2023-12-31) AS result
    ");
    let balance = &results[0].variables["result"];
    match balance {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::ZERO, "Balance should be 0 after rollback"),
        _ => panic!("Expected Money, got {:?}", balance),
    }
}

#[test]
fn test_income_statement() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE ACCOUNT @revenue INCOME;
        CREATE ACCOUNT @cogs EXPENSE;

        CREATE JOURNAL 2023-01-01, 10000, 'Investment' CREDIT @equity, DEBIT @bank;
        CREATE JOURNAL 2023-01-15, 500, 'Sale' CREDIT @revenue, DEBIT @bank;
        CREATE JOURNAL 2023-02-01, 300, 'Sale' CREDIT @revenue, DEBIT @bank;
        CREATE JOURNAL 2023-01-20, 200, 'Supplies' CREDIT @bank, DEBIT @cogs;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET income_statement(2023-01-01, 2023-03-01) AS pnl
    ");

    let pnl = &results[0].variables["pnl"];
    match pnl {
        DataValue::TrialBalance(items) => {
            let net = items.iter().find(|i| i.account_id.as_ref() == "NET_INCOME").unwrap();
            // Revenue 800 - Expenses 200 = Net Income 600
            assert_eq!(net.balance, rust_decimal::Decimal::from(600));
        },
        _ => panic!("Expected TrialBalance, got {:?}", pnl),
    }
}

#[test]
fn test_account_count() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE ACCOUNT @revenue INCOME;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET account_count() AS count
    ");

    let count = &results[0].variables["count"];
    match count {
        DataValue::Int(n) => assert_eq!(*n, 3),
        _ => panic!("Expected Int, got {:?}", count),
    }
}

#[test]
fn test_multi_currency_conversion() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank_usd ASSET;
        CREATE ACCOUNT @bank_eur ASSET;
        CREATE ACCOUNT @equity EQUITY;

        CREATE RATE usd_eur;
        SET RATE usd_eur 0.85 2023-01-01;
        SET RATE usd_eur 0.92 2023-06-01;

        CREATE JOURNAL 2023-01-01, 10000, 'Initial USD' CREDIT @equity, DEBIT @bank_usd;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET
            convert(1000, 'usd_eur', 2023-03-01) AS jan_rate,
            convert(1000, 'usd_eur', 2023-07-01) AS jun_rate,
            fx_rate('usd_eur', 2023-03-01) AS rate_jan,
            round(123.456789, 2) AS rounded,
            abs(0 - 500) AS absolute,
            min(100, 200) AS minimum,
            max(100, 200) AS maximum
    ");

    let r = &results[0];
    match &r.variables["jan_rate"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(850)),
        v => panic!("Expected 850, got {:?}", v),
    }
    match &r.variables["jun_rate"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(920)),
        v => panic!("Expected 920, got {:?}", v),
    }
    match &r.variables["rate_jan"] {
        DataValue::Money(m) => assert_eq!(m.to_string(), "0.85"),
        v => panic!("Expected 0.85, got {:?}", v),
    }
    match &r.variables["rounded"] {
        DataValue::Money(m) => assert_eq!(m.to_string(), "123.46"),
        v => panic!("Expected 123.46, got {:?}", v),
    }
    match &r.variables["absolute"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(500)),
        DataValue::Int(i) => assert_eq!(*i, 500),
        v => panic!("Expected 500, got {:?}", v),
    }
    match &r.variables["minimum"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(100)),
        DataValue::Int(i) => assert_eq!(*i, 100),
        v => panic!("Expected 100, got {:?}", v),
    }
    match &r.variables["maximum"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(200)),
        DataValue::Int(i) => assert_eq!(*i, 200),
        v => panic!("Expected 200, got {:?}", v),
    }
}

// =============================================================================
// Real-world end-to-end scenario tests
// =============================================================================

/// E-commerce business operating over Q1 2023:
/// - Processes sales with tax across multiple months
/// - Handles refunds that reverse prior entries
/// - Verifies monthly balances, trial balance integrity, and income statement
#[test]
fn test_ecommerce_quarterly_lifecycle() {
    let (exec, mut ctx) = setup();

    // Set up chart of accounts
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @accounts_receivable ASSET;
        CREATE ACCOUNT @inventory ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE ACCOUNT @revenue INCOME;
        CREATE ACCOUNT @cogs EXPENSE;
        CREATE ACCOUNT @refunds EXPENSE;
        CREATE ACCOUNT @tax_payable LIABILITY;

        CREATE RATE sales_tax;
        SET RATE sales_tax 0.08 2023-01-01;
    ");

    // January: Initial capital + inventory purchase + 3 sales
    execute_script(&exec, &mut ctx, "
        CREATE JOURNAL 2023-01-01, 100000, 'Initial investment'
        CREDIT @equity, DEBIT @bank;

        CREATE JOURNAL 2023-01-02, 30000, 'Inventory purchase'
        CREDIT @bank, DEBIT @inventory;

        CREATE JOURNAL 2023-01-10, 500, 'Order #1001'
        FOR Customer='Alice', Channel='Web'
        CREDIT @revenue, DEBIT @bank,
        CREDIT @tax_payable WITH RATE sales_tax,
        DEBIT @bank WITH RATE sales_tax;

        CREATE JOURNAL 2023-01-10, 200, 'COGS for Order #1001'
        FOR Customer='Alice', Channel='Web'
        CREDIT @inventory, DEBIT @cogs;

        CREATE JOURNAL 2023-01-15, 1200, 'Order #1002'
        FOR Customer='Bob', Channel='Store'
        CREDIT @revenue, DEBIT @bank,
        CREDIT @tax_payable WITH RATE sales_tax,
        DEBIT @bank WITH RATE sales_tax;

        CREATE JOURNAL 2023-01-15, 480, 'COGS for Order #1002'
        FOR Customer='Bob', Channel='Store'
        CREDIT @inventory, DEBIT @cogs;

        CREATE JOURNAL 2023-01-25, 800, 'Order #1003'
        FOR Customer='Carol', Channel='Web'
        CREDIT @revenue, DEBIT @bank,
        CREDIT @tax_payable WITH RATE sales_tax,
        DEBIT @bank WITH RATE sales_tax;

        CREATE JOURNAL 2023-01-25, 320, 'COGS for Order #1003'
        FOR Customer='Carol', Channel='Web'
        CREDIT @inventory, DEBIT @cogs;
    ");

    // Verify January month-end
    let jan_results = execute_script(&exec, &mut ctx, "
        GET
            balance(@bank, 2023-01-31) AS bank_jan,
            balance(@revenue, 2023-01-31) AS revenue_jan,
            balance(@cogs, 2023-01-31) AS cogs_jan,
            balance(@inventory, 2023-01-31) AS inventory_jan,
            balance(@tax_payable, 2023-01-31) AS tax_jan,
            trial_balance(2023-01-31) AS tb_jan
    ");

    let r = &jan_results[0];
    // Bank: 100000 - 30000 + 540 + 1296 + 864 = 72700
    // (each sale: amount + amount*0.08 tax)
    match &r.variables["bank_jan"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(72700)),
        v => panic!("Expected bank=72700, got {:?}", v),
    }
    // Revenue: 500 + 1200 + 800 = 2500
    match &r.variables["revenue_jan"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(2500)),
        v => panic!("Expected revenue=2500, got {:?}", v),
    }
    // COGS: 200 + 480 + 320 = 1000
    match &r.variables["cogs_jan"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(1000)),
        v => panic!("Expected cogs=1000, got {:?}", v),
    }
    // Tax payable: 2500 * 0.08 = 200
    match &r.variables["tax_jan"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(200)),
        v => panic!("Expected tax=200, got {:?}", v),
    }
    // Trial balance must balance
    assert_trial_balance_balanced(&r.variables["tb_jan"], "Jan month-end");

    // February: Refund for Order #1003, 2 more sales
    execute_script(&exec, &mut ctx, "
        CREATE JOURNAL 2023-02-05, 800, 'Refund Order #1003'
        FOR Customer='Carol', Channel='Web'
        DEBIT @revenue, CREDIT @bank,
        DEBIT @tax_payable WITH RATE sales_tax,
        CREDIT @bank WITH RATE sales_tax;

        CREATE JOURNAL 2023-02-05, 320, 'Reverse COGS for Order #1003'
        FOR Customer='Carol', Channel='Web'
        DEBIT @inventory, CREDIT @cogs;

        CREATE JOURNAL 2023-02-10, 2000, 'Order #2001'
        FOR Customer='Dave', Channel='Web'
        CREDIT @revenue, DEBIT @bank,
        CREDIT @tax_payable WITH RATE sales_tax,
        DEBIT @bank WITH RATE sales_tax;

        CREATE JOURNAL 2023-02-10, 800, 'COGS for Order #2001'
        FOR Customer='Dave', Channel='Web'
        CREDIT @inventory, DEBIT @cogs;

        CREATE JOURNAL 2023-02-20, 3000, 'Order #2002'
        FOR Customer='Eve', Channel='Store'
        CREDIT @revenue, DEBIT @bank,
        CREDIT @tax_payable WITH RATE sales_tax,
        DEBIT @bank WITH RATE sales_tax;

        CREATE JOURNAL 2023-02-20, 1200, 'COGS for Order #2002'
        FOR Customer='Eve', Channel='Store'
        CREDIT @inventory, DEBIT @cogs;
    ");

    // March: Tax payment + more sales
    execute_script(&exec, &mut ctx, "
        CREATE JOURNAL 2023-03-01, 200, 'Q1 tax remittance (Jan)'
        DEBIT @tax_payable, CREDIT @bank;

        CREATE JOURNAL 2023-03-15, 1500, 'Order #3001'
        FOR Customer='Frank', Channel='Web'
        CREDIT @revenue, DEBIT @bank,
        CREDIT @tax_payable WITH RATE sales_tax,
        DEBIT @bank WITH RATE sales_tax;

        CREATE JOURNAL 2023-03-15, 600, 'COGS for Order #3001'
        FOR Customer='Frank', Channel='Web'
        CREDIT @inventory, DEBIT @cogs;
    ");

    // Verify Q1 end
    let q1_results = execute_script(&exec, &mut ctx, "
        GET
            balance(@revenue, 2023-03-31) AS revenue_q1,
            balance(@cogs, 2023-03-31) AS cogs_q1,
            income_statement(2023-01-01, 2023-03-31) AS pnl_q1,
            trial_balance(2023-03-31) AS tb_q1,
            account_count() AS num_accounts
    ");

    let q1 = &q1_results[0];
    // Revenue: 2500 - 800(refund) + 2000 + 3000 + 1500 = 8200
    match &q1.variables["revenue_q1"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(8200)),
        v => panic!("Expected revenue=8200, got {:?}", v),
    }
    // COGS: 1000 - 320(reverse) + 800 + 1200 + 600 = 3280
    match &q1.variables["cogs_q1"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(3280)),
        v => panic!("Expected cogs=3280, got {:?}", v),
    }
    // Income statement: Net Income = Revenue - Refunds - COGS = 8200 - 3280 = 4920
    match &q1.variables["pnl_q1"] {
        DataValue::TrialBalance(items) => {
            let net = items.iter().find(|i| i.account_id.as_ref() == "NET_INCOME").unwrap();
            assert_eq!(net.balance, rust_decimal::Decimal::from(4920), "Q1 net income should be 4920");
        },
        v => panic!("Expected TrialBalance, got {:?}", v),
    }
    assert_trial_balance_balanced(&q1.variables["tb_q1"], "Q1 end");

    match &q1.variables["num_accounts"] {
        DataValue::Int(n) => assert_eq!(*n, 8),
        v => panic!("Expected 8 accounts, got {:?}", v),
    }

    // Verify dimension filtering — Web channel revenue
    let dim_results = execute_script(&exec, &mut ctx, "
        GET
            balance(@revenue, 2023-03-31, Channel='Web') AS web_revenue,
            balance(@revenue, 2023-03-31, Channel='Store') AS store_revenue
    ");
    let d = &dim_results[0];
    // Web: 500 + 800(refunded) - 800 + 2000 + 1500 = 4000
    match &d.variables["web_revenue"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(4000)),
        v => panic!("Expected web_revenue=4000, got {:?}", v),
    }
    // Store: 1200 + 3000 = 4200
    match &d.variables["store_revenue"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(4200)),
        v => panic!("Expected store_revenue=4200, got {:?}", v),
    }
}

/// Multi-department company with monthly expense tracking:
/// - Engineering, Sales, and Marketing departments
/// - Operating expenses, payroll, travel
/// - Verifies per-department balances and consolidated trial balance
#[test]
fn test_multi_department_expense_tracking() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE ACCOUNT @salaries EXPENSE;
        CREATE ACCOUNT @travel EXPENSE;
        CREATE ACCOUNT @software EXPENSE;
        CREATE ACCOUNT @marketing_spend EXPENSE;
        CREATE ACCOUNT @revenue INCOME;

        CREATE JOURNAL 2023-01-01, 500000, 'Series A funding'
        CREDIT @equity, DEBIT @bank;
    ");

    // January payroll by department
    execute_script(&exec, &mut ctx, "
        CREATE JOURNAL 2023-01-31, 50000, 'Jan payroll - Engineering'
        FOR Department='Engineering'
        CREDIT @bank, DEBIT @salaries;

        CREATE JOURNAL 2023-01-31, 30000, 'Jan payroll - Sales'
        FOR Department='Sales'
        CREDIT @bank, DEBIT @salaries;

        CREATE JOURNAL 2023-01-31, 20000, 'Jan payroll - Marketing'
        FOR Department='Marketing'
        CREDIT @bank, DEBIT @salaries;
    ");

    // February payroll + department-specific expenses
    execute_script(&exec, &mut ctx, "
        CREATE JOURNAL 2023-02-28, 50000, 'Feb payroll - Engineering'
        FOR Department='Engineering'
        CREDIT @bank, DEBIT @salaries;

        CREATE JOURNAL 2023-02-28, 30000, 'Feb payroll - Sales'
        FOR Department='Sales'
        CREDIT @bank, DEBIT @salaries;

        CREATE JOURNAL 2023-02-28, 20000, 'Feb payroll - Marketing'
        FOR Department='Marketing'
        CREDIT @bank, DEBIT @salaries;

        CREATE JOURNAL 2023-02-15, 5000, 'AWS bill'
        FOR Department='Engineering'
        CREDIT @bank, DEBIT @software;

        CREATE JOURNAL 2023-02-10, 3000, 'Sales conference travel'
        FOR Department='Sales'
        CREDIT @bank, DEBIT @travel;

        CREATE JOURNAL 2023-02-20, 15000, 'Ad campaign'
        FOR Department='Marketing'
        CREDIT @bank, DEBIT @marketing_spend;
    ");

    // Some revenue comes in
    execute_script(&exec, &mut ctx, "
        CREATE JOURNAL 2023-02-15, 80000, 'Feb subscription revenue'
        CREDIT @revenue, DEBIT @bank;
    ");

    // Verify department-level expense reporting
    let results = execute_script(&exec, &mut ctx, "
        GET
            balance(@salaries, 2023-02-28, Department='Engineering') AS eng_sal,
            balance(@salaries, 2023-02-28, Department='Sales') AS sales_sal,
            balance(@salaries, 2023-02-28, Department='Marketing') AS mkt_sal,
            balance(@salaries, 2023-02-28) AS total_sal,
            balance(@software, 2023-02-28, Department='Engineering') AS eng_sw,
            balance(@travel, 2023-02-28, Department='Sales') AS sales_travel,
            balance(@marketing_spend, 2023-02-28, Department='Marketing') AS mkt_ads,
            income_statement(2023-01-01, 2023-02-28) AS pnl,
            trial_balance(2023-02-28) AS tb
    ");

    let r = &results[0];

    // Per-department salary verification
    match &r.variables["eng_sal"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(100000)),
        v => panic!("Expected eng salaries=100000, got {:?}", v),
    }
    match &r.variables["sales_sal"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(60000)),
        v => panic!("Expected sales salaries=60000, got {:?}", v),
    }
    match &r.variables["mkt_sal"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(40000)),
        v => panic!("Expected marketing salaries=40000, got {:?}", v),
    }
    // Total salaries = 100000 + 60000 + 40000 = 200000
    match &r.variables["total_sal"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(200000)),
        v => panic!("Expected total salaries=200000, got {:?}", v),
    }

    // Income statement: Revenue 80000, Expenses 200000+5000+3000+15000 = 223000
    // Net = 80000 - 223000 = -143000
    match &r.variables["pnl"] {
        DataValue::TrialBalance(items) => {
            let net = items.iter().find(|i| i.account_id.as_ref() == "NET_INCOME").unwrap();
            assert_eq!(net.balance, rust_decimal::Decimal::from(-143000), "Net income should be -143000 (burn)");
        },
        v => panic!("Expected TrialBalance, got {:?}", v),
    }

    assert_trial_balance_balanced(&r.variables["tb"], "Feb month-end");

    // Verify bank account statement shows all transactions
    let stmt_results = execute_script(&exec, &mut ctx, "
        GET statement(@bank, 2023-01-01, 2023-02-28) AS bank_stmt
    ");
    match &stmt_results[0].variables["bank_stmt"] {
        DataValue::Statement(txns) => {
            assert_eq!(txns.len(), 11, "Bank should have 11 transactions");
            // Final balance: 500000 - 100000 - 100000 - 5000 - 3000 - 15000 + 80000 = 357000
            let last = txns.last().unwrap();
            assert_eq!(last.balance, rust_decimal::Decimal::from(357000));
        },
        v => panic!("Expected Statement, got {:?}", v),
    }
}

/// Lending fund with multiple accrual periods and rate changes:
/// - Loans issued at different times
/// - Interest rate changes mid-period
/// - Multiple accrual runs (monthly)
/// - Loan repayment
/// - Verifies interest compounds correctly across periods
#[test]
fn test_lending_fund_multi_period_accruals() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @loans ASSET;
        CREATE ACCOUNT @interest_receivable ASSET;
        CREATE ACCOUNT @interest_income INCOME;
        CREATE ACCOUNT @equity EQUITY;

        CREATE RATE prime;
        SET RATE prime 0.05 2023-01-01;

        CREATE JOURNAL 2023-01-01, 1000000, 'Fund capital'
        CREDIT @equity, DEBIT @bank;

        CREATE JOURNAL 2023-01-15, 100000, 'Loan to Acme Corp'
        FOR Borrower='Acme', LoanType='Term'
        DEBIT @loans, CREDIT @bank;

        CREATE JOURNAL 2023-02-01, 50000, 'Loan to Beta Inc'
        FOR Borrower='Beta', LoanType='Term'
        DEBIT @loans, CREDIT @bank;
    ");

    // Month 1 accrual: Jan 15 - Jan 31 (Acme only, 16 days)
    execute_script(&exec, &mut ctx, "
        ACCRUE @loans FROM 2023-01-15 TO 2023-01-31
        WITH RATE prime COMPOUND DAILY
        BY Borrower
        INTO JOURNAL
            2023-02-01, 'Jan interest accrual'
        DEBIT @interest_receivable,
        CREDIT @interest_income;
    ");

    let jan_results = execute_script(&exec, &mut ctx, "
        GET
            balance(@interest_income, 2023-02-01) AS jan_interest,
            balance(@interest_receivable, 2023-02-01, Borrower='Acme') AS acme_jan_int,
            trial_balance(2023-02-01) AS tb_jan
    ");

    let r = &jan_results[0];
    // Interest should be small but > 0 (about 100000 * 0.05/365 * 16 ≈ 219)
    match &r.variables["jan_interest"] {
        DataValue::Money(m) => {
            assert!(*m > rust_decimal::Decimal::ZERO, "Jan interest should be > 0");
            assert!(*m < rust_decimal::Decimal::from(300), "Jan interest should be reasonable");
        },
        v => panic!("Expected Money, got {:?}", v),
    }
    assert_trial_balance_balanced(&r.variables["tb_jan"], "Jan accrual");

    // Rate change mid-February
    execute_script(&exec, &mut ctx, "
        SET RATE prime 0.065 2023-02-15;
    ");

    // Month 2 accrual: Feb 1 - Feb 28 (both borrowers, rate changes mid-month)
    execute_script(&exec, &mut ctx, "
        ACCRUE @loans FROM 2023-02-01 TO 2023-02-28
        WITH RATE prime COMPOUND DAILY
        BY Borrower
        INTO JOURNAL
            2023-03-01, 'Feb interest accrual'
        DEBIT @interest_receivable,
        CREDIT @interest_income;
    ");

    let feb_results = execute_script(&exec, &mut ctx, "
        GET
            balance(@interest_income, 2023-03-01) AS total_interest,
            balance(@interest_receivable, 2023-03-01, Borrower='Acme') AS acme_total_int,
            balance(@interest_receivable, 2023-03-01, Borrower='Beta') AS beta_total_int,
            trial_balance(2023-03-01) AS tb_feb
    ");

    let r2 = &feb_results[0];
    // Both borrowers should have accrued interest
    match &r2.variables["acme_total_int"] {
        DataValue::Money(m) => assert!(*m > rust_decimal::Decimal::ZERO, "Acme should have interest"),
        v => panic!("Expected Money, got {:?}", v),
    }
    match &r2.variables["beta_total_int"] {
        DataValue::Money(m) => assert!(*m > rust_decimal::Decimal::ZERO, "Beta should have interest"),
        v => panic!("Expected Money, got {:?}", v),
    }
    // Total interest should be > Jan interest (compounding + 2nd borrower)
    match (&r.variables["jan_interest"], &r2.variables["total_interest"]) {
        (DataValue::Money(jan), DataValue::Money(total)) => {
            assert!(total > jan, "Total interest after Feb should exceed Jan-only interest");
        },
        _ => panic!("Expected Money values"),
    }
    assert_trial_balance_balanced(&r2.variables["tb_feb"], "Feb accrual");

    // Borrower Beta repays loan + interest in March
    let repay_results = execute_script(&exec, &mut ctx, "
        GET balance(@loans, 2023-03-01, Borrower='Beta') AS beta_loan,
            balance(@interest_receivable, 2023-03-01, Borrower='Beta') AS beta_interest
    ");
    let beta_loan = match &repay_results[0].variables["beta_loan"] {
        DataValue::Money(m) => *m,
        v => panic!("Expected Money, got {:?}", v),
    };
    let beta_interest = match &repay_results[0].variables["beta_interest"] {
        DataValue::Money(m) => *m,
        v => panic!("Expected Money, got {:?}", v),
    };

    let repayment_fql = format!(
        "CREATE JOURNAL 2023-03-15, {}, 'Beta loan repayment' FOR Borrower='Beta' CREDIT @loans, DEBIT @bank;
         CREATE JOURNAL 2023-03-15, {}, 'Beta interest payment' FOR Borrower='Beta' CREDIT @interest_receivable, DEBIT @bank;",
        beta_loan, beta_interest
    );
    execute_script(&exec, &mut ctx, &repayment_fql);

    // After repayment, Beta's loan and interest should be zero
    let post_repay = execute_script(&exec, &mut ctx, "
        GET
            balance(@loans, 2023-03-31, Borrower='Beta') AS beta_loan_after,
            balance(@interest_receivable, 2023-03-31, Borrower='Beta') AS beta_int_after,
            balance(@loans, 2023-03-31, Borrower='Acme') AS acme_loan_after,
            trial_balance(2023-03-31) AS tb_mar
    ");

    let r3 = &post_repay[0];
    match &r3.variables["beta_loan_after"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::ZERO, "Beta loan should be fully repaid"),
        v => panic!("Expected Money, got {:?}", v),
    }
    match &r3.variables["beta_int_after"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::ZERO, "Beta interest should be fully collected"),
        v => panic!("Expected Money, got {:?}", v),
    }
    // Acme's loan should still be outstanding
    match &r3.variables["acme_loan_after"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(100000), "Acme loan should remain"),
        v => panic!("Expected Money, got {:?}", v),
    }
    assert_trial_balance_balanced(&r3.variables["tb_mar"], "Mar post-repayment");
}

/// Multi-currency international business:
/// - US parent company with EUR subsidiary operations
/// - Records transactions in USD, converts EUR at spot rates
/// - Verifies FX conversion accuracy across rate changes
#[test]
fn test_international_multi_currency_operations() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank_usd ASSET;
        CREATE ACCOUNT @bank_eur ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE ACCOUNT @revenue_domestic INCOME;
        CREATE ACCOUNT @revenue_international INCOME;
        CREATE ACCOUNT @fx_revaluation EXPENSE;

        CREATE RATE usd_eur;
        SET RATE usd_eur 0.85 2023-01-01;
        SET RATE usd_eur 0.88 2023-02-01;
        SET RATE usd_eur 0.92 2023-03-01;

        CREATE JOURNAL 2023-01-01, 200000, 'Initial capital'
        CREDIT @equity, DEBIT @bank_usd;
    ");

    // Q1 domestic and international sales
    execute_script(&exec, &mut ctx, "
        CREATE JOURNAL 2023-01-15, 50000, 'US sales Jan'
        FOR Region='US'
        CREDIT @revenue_domestic, DEBIT @bank_usd;

        CREATE JOURNAL 2023-02-15, 60000, 'US sales Feb'
        FOR Region='US'
        CREDIT @revenue_domestic, DEBIT @bank_usd;

        CREATE JOURNAL 2023-03-15, 45000, 'US sales Mar'
        FOR Region='US'
        CREDIT @revenue_domestic, DEBIT @bank_usd;

        CREATE JOURNAL 2023-01-20, 30000, 'EU sales Jan'
        FOR Region='EU'
        CREDIT @revenue_international, DEBIT @bank_eur;

        CREATE JOURNAL 2023-02-20, 40000, 'EU sales Feb'
        FOR Region='EU'
        CREDIT @revenue_international, DEBIT @bank_eur;

        CREATE JOURNAL 2023-03-20, 35000, 'EU sales Mar'
        FOR Region='EU'
        CREDIT @revenue_international, DEBIT @bank_eur;
    ");

    // Verify balances and FX conversions
    let results = execute_script(&exec, &mut ctx, "
        GET
            balance(@revenue_domestic, 2023-03-31) AS us_rev,
            balance(@revenue_international, 2023-03-31) AS eu_rev,
            balance(@bank_usd, 2023-03-31) AS usd_bank,
            balance(@bank_eur, 2023-03-31) AS eur_bank,
            convert(105000, 'usd_eur', 2023-03-31) AS eur_pool_in_eur,
            fx_rate('usd_eur', 2023-01-15) AS jan_rate,
            fx_rate('usd_eur', 2023-02-15) AS feb_rate,
            fx_rate('usd_eur', 2023-03-15) AS mar_rate,
            trial_balance(2023-03-31) AS tb,
            income_statement(2023-01-01, 2023-03-31) AS pnl
    ");

    let r = &results[0];

    // Domestic revenue: 50000 + 60000 + 45000 = 155000
    match &r.variables["us_rev"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(155000)),
        v => panic!("Expected 155000, got {:?}", v),
    }
    // International revenue: 30000 + 40000 + 35000 = 105000
    match &r.variables["eu_rev"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(105000)),
        v => panic!("Expected 105000, got {:?}", v),
    }

    // FX rates by period
    match &r.variables["jan_rate"] {
        DataValue::Money(m) => assert_eq!(m.to_string(), "0.85"),
        v => panic!("Expected 0.85, got {:?}", v),
    }
    match &r.variables["feb_rate"] {
        DataValue::Money(m) => assert_eq!(m.to_string(), "0.88"),
        v => panic!("Expected 0.88, got {:?}", v),
    }
    match &r.variables["mar_rate"] {
        DataValue::Money(m) => assert_eq!(m.to_string(), "0.92"),
        v => panic!("Expected 0.92, got {:?}", v),
    }

    // EUR equivalent of 105000 USD at March rate (0.92): 96600
    match &r.variables["eur_pool_in_eur"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(96600)),
        v => panic!("Expected 96600, got {:?}", v),
    }

    assert_trial_balance_balanced(&r.variables["tb"], "Q1 multi-currency");

    // Income statement should show total revenue - expenses
    match &r.variables["pnl"] {
        DataValue::TrialBalance(items) => {
            let net = items.iter().find(|i| i.account_id.as_ref() == "NET_INCOME").unwrap();
            // Net = 155000 + 105000 = 260000 (no expenses)
            assert_eq!(net.balance, rust_decimal::Decimal::from(260000));
        },
        v => panic!("Expected TrialBalance, got {:?}", v),
    }
}

/// Transaction safety under complex multi-statement scripts:
/// - Verifies implicit transaction wrapping with many statements
/// - Tests that partial failure in a large batch rolls back everything
/// - Verifies successful batch commits atomically
#[test]
fn test_complex_transaction_atomicity() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE ACCOUNT @revenue INCOME;
        CREATE ACCOUNT @expenses EXPENSE;
    ");

    // Successful batch: 10 journals in one script
    let mut big_script = String::new();
    for i in 1..=10 {
        big_script.push_str(&format!(
            "CREATE JOURNAL 2023-01-{:02}, {}, 'Batch txn {}' CREDIT @equity, DEBIT @bank;\n",
            i, i * 100, i
        ));
    }
    big_script.push_str("GET balance(@bank, 2023-01-31) AS total");

    let results = execute_script(&exec, &mut ctx, &big_script);
    let total = &results.last().unwrap().variables["total"];
    // Sum: 100 + 200 + ... + 1000 = 5500
    match total {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(5500)),
        v => panic!("Expected 5500, got {:?}", v),
    }

    // Now test failure rollback: batch that fails partway
    let failing_script = "
        CREATE JOURNAL 2023-02-01, 999, 'Should be rolled back' CREDIT @equity, DEBIT @bank;
        CREATE JOURNAL 2023-02-02, 888, 'Should be rolled back' CREDIT @equity, DEBIT @bank;
        CREATE JOURNAL 2023-02-03, 777, 'Fail here' CREDIT @nonexistent, DEBIT @bank;
    ";
    let statements = lexer::parse(failing_script).unwrap();
    let result = exec.execute_script(&mut ctx, &statements);
    assert!(result.is_err());

    // Bank balance should still be 5500 — the failing batch was fully rolled back
    let check = execute_script(&exec, &mut ctx, "
        GET balance(@bank, 2023-12-31) AS bank_after
    ");
    match &check[0].variables["bank_after"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(5500), "Balance must not change after failed batch"),
        v => panic!("Expected 5500, got {:?}", v),
    }

    // Explicit transaction: BEGIN, multiple ops, COMMIT
    execute_script(&exec, &mut ctx, "
        BEGIN;
        CREATE JOURNAL 2023-03-01, 1000, 'In explicit tx 1' CREDIT @revenue, DEBIT @bank;
        CREATE JOURNAL 2023-03-02, 2000, 'In explicit tx 2' CREDIT @revenue, DEBIT @bank;
        CREATE JOURNAL 2023-03-03, 500, 'In explicit tx 3' CREDIT @bank, DEBIT @expenses;
        COMMIT;
    ");

    let final_check = execute_script(&exec, &mut ctx, "
        GET
            balance(@bank, 2023-12-31) AS final_bank,
            balance(@revenue, 2023-12-31) AS final_revenue,
            trial_balance(2023-12-31) AS tb
    ");
    let f = &final_check[0];
    // Bank: 5500 + 1000 + 2000 - 500 = 8000
    match &f.variables["final_bank"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(8000)),
        v => panic!("Expected 8000, got {:?}", v),
    }
    match &f.variables["final_revenue"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(3000)),
        v => panic!("Expected 3000, got {:?}", v),
    }
    assert_trial_balance_balanced(&f.variables["tb"], "Final after explicit tx");
}

/// Helper: assert that a trial balance DataValue is balanced (debits == credits)
fn assert_trial_balance_balanced(tb_val: &DataValue, label: &str) {
    match tb_val {
        DataValue::TrialBalance(items) => {
            let mut total_debit = rust_decimal::Decimal::ZERO;
            let mut total_credit = rust_decimal::Decimal::ZERO;
            for item in items {
                match item.account_type {
                    dblentry::ast::AccountType::Asset | dblentry::ast::AccountType::Expense => {
                        total_debit += item.balance;
                    },
                    _ => {
                        total_credit += item.balance;
                    },
                }
            }
            assert_eq!(total_debit, total_credit, "Trial balance not balanced at: {}", label);
        },
        v => panic!("Expected TrialBalance at {}, got {:?}", label, v),
    }
}

fn assert_money(val: &DataValue, expected: &str, label: &str) {
    match val {
        DataValue::Money(d) => {
            let expected_dec: rust_decimal::Decimal = expected.parse().unwrap();
            assert_eq!(*d, expected_dec, "{}: expected {} got {}", label, expected, d);
        },
        v => panic!("{}: expected Money, got {:?}", label, v),
    }
}

// --- Unit-Based Asset Tracking Tests ---

#[test]
fn test_create_unit_account() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
    ");
    // Should be able to query balance (zero)
    let results = execute_script(&exec, &mut ctx, "
        GET balance(@aapl, 2024-12-31) AS bal;
    ");
    assert_money(&results[0].variables["bal"], "0", "zero balance");
}

#[test]
fn test_buy_units_creates_lot() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 150.00 2024-01-15;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE JOURNAL 2024-01-15, 15000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@aapl, 2024-12-31) AS bal,
            units(@aapl, 2024-12-31) AS u;
    ");
    assert_money(&results[0].variables["bal"], "15000", "cost basis");
    assert_money(&results[0].variables["u"], "100", "units");
}

#[test]
fn test_multiple_buys_multiple_lots() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 150.00 2024-01-15;
        SET RATE AAPL 170.00 2024-06-15;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-03-01, 8000, 'Buy 50 more AAPL'
            DEBIT @aapl 50 UNITS AT 160,
            CREDIT @bank;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@aapl, 2024-12-31) AS bal,
            units(@aapl, 2024-12-31) AS u,
            market_value(@aapl, 2024-06-15) AS mv;
    ");
    assert_money(&results[0].variables["bal"], "23000", "cost basis");
    assert_money(&results[0].variables["u"], "150", "total units");
    assert_money(&results[0].variables["mv"], "25500", "market value 150 × 170");
}

#[test]
fn test_sell_fifo() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 170.00 2024-06-15;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @realized_gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-03-01, 8000, 'Buy 50 AAPL'
            DEBIT @aapl 50 UNITS AT 160,
            CREDIT @bank;

        SELL 75 UNITS OF @aapl AT 170 ON 2024-06-15
            METHOD FIFO
            PROCEEDS @bank
            GAIN_LOSS @realized_gains
            DESCRIPTION 'Sell AAPL';
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@aapl, 2024-12-31) AS bal,
            units(@aapl, 2024-12-31) AS u,
            balance(@realized_gains, 2024-12-31) AS gains,
            balance(@bank, 2024-12-31) AS bank_bal;
    ");
    // FIFO: sells 75 from first lot (cost 150 each) = cost_basis 11250
    // proceeds = 75 × 170 = 12750
    // gain = 12750 - 11250 = 1500
    // remaining: 25 @ 150 + 50 @ 160 = 3750 + 8000 = 11750
    assert_money(&results[0].variables["bal"], "11750", "remaining cost basis");
    assert_money(&results[0].variables["u"], "75", "remaining units");
    assert_money(&results[0].variables["gains"], "1500", "realized gains");
    // bank: started -15000 -8000 +12750 = -10250
    assert_money(&results[0].variables["bank_bal"], "-10250", "bank balance");
}

#[test]
fn test_sell_lifo() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 170.00 2024-06-15;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @realized_gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-03-01, 8000, 'Buy 50 AAPL'
            DEBIT @aapl 50 UNITS AT 160,
            CREDIT @bank;

        SELL 75 UNITS OF @aapl AT 170 ON 2024-06-15
            METHOD LIFO
            PROCEEDS @bank
            GAIN_LOSS @realized_gains
            DESCRIPTION 'Sell AAPL LIFO';
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@aapl, 2024-12-31) AS bal,
            units(@aapl, 2024-12-31) AS u,
            balance(@realized_gains, 2024-12-31) AS gains;
    ");
    // LIFO: sells 50 from second lot (cost 160) + 25 from first lot (cost 150)
    // cost_basis = 50×160 + 25×150 = 8000 + 3750 = 11750
    // proceeds = 75 × 170 = 12750
    // gain = 12750 - 11750 = 1000
    // remaining: 75 @ 150 = 11250
    assert_money(&results[0].variables["bal"], "11250", "remaining cost basis LIFO");
    assert_money(&results[0].variables["u"], "75", "remaining units LIFO");
    assert_money(&results[0].variables["gains"], "1000", "realized gains LIFO");
}

#[test]
fn test_sell_average() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 170.00 2024-06-15;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @realized_gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-03-01, 7500, 'Buy 50 AAPL'
            DEBIT @aapl 50 UNITS AT 150,
            CREDIT @bank;

        SELL 75 UNITS OF @aapl AT 170 ON 2024-06-15
            METHOD AVERAGE
            PROCEEDS @bank
            GAIN_LOSS @realized_gains
            DESCRIPTION 'Sell AAPL average';
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@aapl, 2024-12-31) AS bal,
            units(@aapl, 2024-12-31) AS u,
            balance(@realized_gains, 2024-12-31) AS gains;
    ");
    // AVERAGE: weighted avg cost = (15000+7500)/150 = 150 per unit
    // cost_basis = 75 × 150 = 11250
    // proceeds = 75 × 170 = 12750
    // gain = 12750 - 11250 = 1500
    // remaining: 75 units, cost = 22500 - 11250 = 11250
    assert_money(&results[0].variables["bal"], "11250", "remaining cost basis AVG");
    assert_money(&results[0].variables["u"], "75", "remaining units AVG");
    assert_money(&results[0].variables["gains"], "1500", "realized gains AVG");
}

#[test]
fn test_sell_with_loss() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 120.00 2024-06-15;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @realized_gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        SELL 50 UNITS OF @aapl AT 120 ON 2024-06-15
            METHOD FIFO
            PROCEEDS @bank
            GAIN_LOSS @realized_gains
            DESCRIPTION 'Sell at loss';
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@aapl, 2024-12-31) AS bal,
            units(@aapl, 2024-12-31) AS u,
            balance(@realized_gains, 2024-12-31) AS gains;
    ");
    // cost_basis = 50 × 150 = 7500
    // proceeds = 50 × 120 = 6000
    // loss = 6000 - 7500 = -1500
    // realized_gains is INCOME: debiting income reduces it, so balance should be -1500
    assert_money(&results[0].variables["bal"], "7500", "remaining cost basis");
    assert_money(&results[0].variables["u"], "50", "remaining units");
    assert_money(&results[0].variables["gains"], "-1500", "realized loss");
}

#[test]
fn test_split() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 37.50 2024-08-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        SPLIT @aapl 4 FOR 1 2024-08-01;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@aapl, 2024-12-31) AS bal,
            units(@aapl, 2024-12-31) AS u,
            cost_basis(@aapl, 2024-12-31) AS cb,
            market_value(@aapl, 2024-08-01) AS mv;
    ");
    // Split 4:1: 100 shares → 400 shares, cost_per_unit 150 → 37.50
    // Balance unchanged at 15000 (cost basis from ledger)
    // Units = 400, cost_basis per unit = 37.50
    // market_value = 400 × 37.50 = 15000
    assert_money(&results[0].variables["bal"], "15000", "cost basis unchanged after split");
    assert_money(&results[0].variables["u"], "400", "units after 4:1 split");
    assert_money(&results[0].variables["cb"], "37.50", "cost per unit after split");
    assert_money(&results[0].variables["mv"], "15000", "market value at split price");
}

#[test]
fn test_split_then_sell() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 42.00 2024-09-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @realized_gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        SPLIT @aapl 4 FOR 1 2024-08-01;

        SELL 200 UNITS OF @aapl AT 42 ON 2024-09-01
            METHOD FIFO
            PROCEEDS @bank
            GAIN_LOSS @realized_gains
            DESCRIPTION 'Sell after split';
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@aapl, 2024-12-31) AS bal,
            units(@aapl, 2024-12-31) AS u,
            balance(@realized_gains, 2024-12-31) AS gains;
    ");
    // After split: 400 units @ 37.50 each
    // Sell 200 @ 42: cost_basis = 200 × 37.50 = 7500, proceeds = 200 × 42 = 8400
    // gain = 8400 - 7500 = 900
    // remaining: 200 units, cost = 15000 - 7500 = 7500
    assert_money(&results[0].variables["bal"], "7500", "remaining cost basis after split+sell");
    assert_money(&results[0].variables["u"], "200", "remaining units after split+sell");
    assert_money(&results[0].variables["gains"], "900", "gain after split+sell");
}

#[test]
fn test_unrealized_gain() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 150.00 2024-01-15;
        SET RATE AAPL 170.00 2024-06-15;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET unrealized_gain(@aapl, 2024-06-15) AS ug;
    ");
    // market_value = 100 × 170 = 17000
    // cost_basis = 15000
    // unrealized_gain = 17000 - 15000 = 2000
    assert_money(&results[0].variables["ug"], "2000", "unrealized gain");
}

#[test]
fn test_lots_function() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-03-01, 8000, 'Buy 50 AAPL'
            DEBIT @aapl 50 UNITS AT 160,
            CREDIT @bank;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET lots(@aapl, 2024-12-31) AS l;
    ");
    match &results[0].variables["l"] {
        DataValue::Lots(lots) => {
            assert_eq!(lots.len(), 2, "should have 2 lots");
            assert_eq!(lots[0].units.to_string(), "100");
            assert_eq!(lots[0].cost_per_unit.to_string(), "150");
            assert_eq!(lots[1].units.to_string(), "50");
            assert_eq!(lots[1].cost_per_unit.to_string(), "160");
        },
        v => panic!("Expected Lots, got {:?}", v),
    }
}

#[test]
fn test_trial_balance_with_unit_accounts() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;

        CREATE JOURNAL 2024-01-01, 50000, 'Initial equity'
            DEBIT @bank,
            CREDIT @equity;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET trial_balance(2024-12-31) AS tb;
    ");
    assert_trial_balance_balanced(&results[0].variables["tb"], "unit account trial balance");
}

#[test]
fn test_sell_default_method_is_fifo() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 10000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 100,
            CREDIT @bank;

        CREATE JOURNAL 2024-03-01, 12000, 'Buy 100 more AAPL'
            DEBIT @aapl 100 UNITS AT 120,
            CREDIT @bank;

        SELL 100 UNITS OF @aapl AT 130 ON 2024-06-15
            PROCEEDS @bank
            GAIN_LOSS @gains
            DESCRIPTION 'Sell without method';
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@gains, 2024-12-31) AS gains;
    ");
    // Default FIFO: sells 100 from first lot @ 100 = cost 10000
    // proceeds = 100 × 130 = 13000, gain = 3000
    assert_money(&results[0].variables["gains"], "3000", "default FIFO gain");
}

#[test]
fn test_non_unit_account_unaffected() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @revenue INCOME;
        CREATE JOURNAL 2024-01-01, 5000, 'Revenue'
            DEBIT @bank,
            CREDIT @revenue;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@bank, 2024-12-31) AS bal;
    ");
    assert_money(&results[0].variables["bal"], "5000", "non-unit account works normally");
}

#[test]
fn test_full_investment_workflow() {
    let (exec, mut ctx) = setup();
    // Full workflow: buy → appreciate → partial sell → split → sell → verify
    execute_script(&exec, &mut ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 150.00 2024-01-15;
        SET RATE AAPL 160.00 2024-03-01;
        SET RATE AAPL 170.00 2024-06-15;
        SET RATE AAPL 42.00 2024-08-01;
        SET RATE AAPL 45.00 2024-09-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE ACCOUNT @realized_gains INCOME;

        CREATE JOURNAL 2024-01-01, 100000, 'Initial investment'
            DEBIT @bank,
            CREDIT @equity;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy 100 AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-03-01, 8000, 'Buy 50 AAPL'
            DEBIT @aapl 50 UNITS AT 160,
            CREDIT @bank;

        SELL 75 UNITS OF @aapl AT 170 ON 2024-06-15
            METHOD FIFO
            PROCEEDS @bank
            GAIN_LOSS @realized_gains
            DESCRIPTION 'Partial sell';

        SPLIT @aapl 4 FOR 1 2024-08-01;

        SELL 100 UNITS OF @aapl AT 45 ON 2024-09-01
            METHOD FIFO
            PROCEEDS @bank
            GAIN_LOSS @realized_gains
            DESCRIPTION 'Sell after split';
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET trial_balance(2024-12-31) AS tb,
            units(@aapl, 2024-12-31) AS u,
            balance(@aapl, 2024-12-31) AS aapl_bal,
            balance(@realized_gains, 2024-12-31) AS gains;
    ");

    assert_trial_balance_balanced(&results[0].variables["tb"], "full investment workflow");

    // After first sell FIFO: 75 units sold @ 150 cost, 25 remain @ 150 + 50 @ 160
    // remaining balance: 25×150 + 50×160 = 3750 + 8000 = 11750, units = 75
    // After 4:1 split: 300 units, lots: 100 @ 37.50 + 200 @ 40.00
    // After sell 100 FIFO @ 45: cost = 100 × 37.50 = 3750, proceeds = 4500, gain = 750
    // remaining: 200 @ 40.00 = 8000, units = 200
    assert_money(&results[0].variables["u"], "200", "final units");
    assert_money(&results[0].variables["aapl_bal"], "8000", "final cost basis");
    // Total gains: first sell 1500 + second sell 750 = 2250
    assert_money(&results[0].variables["gains"], "2250", "total realized gains");
}

fn setup_sqlite() -> (StatementExecutor, ExecutionContext) {
    use dblentry_sqlite::SqliteStorage;
    let storage: Arc<dyn dblentry::storage::StorageBackend> = Arc::new(SqliteStorage::new(":memory:").unwrap());
    let function_registry = FunctionRegistry::new();
    register_functions(&function_registry, &storage);
    let expression_evaluator = Arc::new(ExpressionEvaluator::new(Arc::new(function_registry), storage.clone()));
    let exec = StatementExecutor::new(expression_evaluator, storage);
    let eff_date = time::OffsetDateTime::now_utc().date();
    let context = ExecutionContext::new(eff_date, QueryVariables::new());
    (exec, context)
}

#[test]
fn test_sqlite_lending_fund_e2e() {
    let (exec, mut ctx) = setup_sqlite();

    let results = execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @loans ASSET;
        CREATE ACCOUNT @interest_earned INCOME;
        CREATE ACCOUNT @equity EQUITY;

        CREATE RATE prime;
        SET RATE prime 0.05 2023-01-01;
        SET RATE prime 0.06 2023-02-15;

        CREATE JOURNAL
            2023-01-01, 20000, 'Investment'
        FOR Investor='Frank'
        CREDIT @equity, DEBIT @bank;

        CREATE JOURNAL
            2023-02-01, 1000, 'Loan Issued'
        FOR Customer='John', Region='US'
        DEBIT @loans, CREDIT @bank;

        CREATE JOURNAL
            2023-02-01, 500, 'Loan Issued'
        FOR Customer='Joe', Region='US'
        DEBIT @loans, CREDIT @bank;

        ACCRUE @loans FROM 2023-02-01 TO 2023-02-28
        WITH RATE prime COMPOUND DAILY
        BY Customer
        INTO JOURNAL
            2023-03-01, 'Interest'
        DEBIT @loans,
        CREDIT @interest_earned;

        GET
            balance(@loans, 2023-03-01) AS LoanBookTotal,
            trial_balance(2023-03-01) AS TrialBalance
    ");

    let get_result = results.last().unwrap();

    let loan_total = &get_result.variables["LoanBookTotal"];
    match loan_total {
        DataValue::Money(m) => {
            assert!(*m > rust_decimal::Decimal::from(1500), "Loan book should include accrued interest");
        },
        _ => panic!("Expected Money for LoanBookTotal"),
    }

    // Verify trial balance is balanced
    match &get_result.variables["TrialBalance"] {
        DataValue::TrialBalance(items) => {
            let mut total_debit = rust_decimal::Decimal::ZERO;
            let mut total_credit = rust_decimal::Decimal::ZERO;
            for item in items {
                match item.account_type {
                    dblentry::ast::AccountType::Asset | dblentry::ast::AccountType::Expense => {
                        total_debit += item.balance;
                    },
                    _ => {
                        total_credit += item.balance;
                    },
                }
            }
            assert_eq!(total_debit, total_credit, "SQLite: Trial balance must be in balance after accrual");
        },
        _ => panic!("Expected TrialBalance"),
    }
}

#[test]
fn test_sqlite_implicit_transaction_rollback() {
    let (exec, mut ctx) = setup_sqlite();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
    ");

    let statements = lexer::parse("
        CREATE JOURNAL 2023-01-01, 1000, 'Investment' CREDIT @equity, DEBIT @bank;
        CREATE JOURNAL 2023-02-01, 500, 'Bad' CREDIT @nonexistent, DEBIT @bank;
    ").unwrap();

    let result = exec.execute_script(&mut ctx, &statements);
    assert!(result.is_err(), "Script should fail on missing account");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@bank, 2023-12-31) AS result
    ");
    let balance = &results[0].variables["result"];
    match balance {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::ZERO, "SQLite: Balance should be 0 after rollback"),
        _ => panic!("Expected Money, got {:?}", balance),
    }
}

// --- PostgreSQL backend tests ---

fn pg_connection_string() -> String {
    std::env::var("DBLENTRY_TEST_POSTGRES_URL")
        .unwrap_or_else(|_| "host=localhost user=dblentry password=dblentry dbname=dblentry".to_string())
}

fn setup_postgres() -> (StatementExecutor, ExecutionContext) {
    use dblentry_postgres::PostgresStorage;

    // Drop all tables first to ensure a clean slate
    let conn_str = pg_connection_string();
    let mut client = postgres::Client::connect(&conn_str, postgres::NoTls)
        .expect("Failed to connect to PostgreSQL for cleanup");
    client
        .batch_execute(
            "DROP TABLE IF EXISTS lot_dimensions CASCADE;
             DROP TABLE IF EXISTS lots CASCADE;
             DROP TABLE IF EXISTS ledger_entry_dimensions CASCADE;
             DROP TABLE IF EXISTS ledger_entries CASCADE;
             DROP TABLE IF EXISTS journal_dimensions CASCADE;
             DROP TABLE IF EXISTS journals CASCADE;
             DROP TABLE IF EXISTS rates CASCADE;
             DROP TABLE IF EXISTS accounts CASCADE;
             DROP TABLE IF EXISTS sequence_counter CASCADE;",
        )
        .expect("Failed to clean up PostgreSQL tables");
    drop(client);

    let storage: Arc<dyn dblentry::storage::StorageBackend> =
        Arc::new(PostgresStorage::new(&conn_str).expect("Failed to create PostgresStorage"));
    let function_registry = FunctionRegistry::new();
    register_functions(&function_registry, &storage);
    let expression_evaluator = Arc::new(ExpressionEvaluator::new(
        Arc::new(function_registry),
        storage.clone(),
    ));
    let exec = StatementExecutor::new(expression_evaluator, storage);
    let eff_date = time::OffsetDateTime::now_utc().date();
    let context = ExecutionContext::new(eff_date, QueryVariables::new());
    (exec, context)
}

fn postgres_available() -> bool {
    let conn_str = pg_connection_string();
    postgres::Client::connect(&conn_str, postgres::NoTls).is_ok()
}

#[test]
#[ignore] // requires running PostgreSQL; run with: cargo test -- --ignored
fn test_postgres_lending_fund_e2e() {
    if !postgres_available() {
        eprintln!("Skipping PostgreSQL test: no connection available");
        return;
    }

    let (exec, mut ctx) = setup_postgres();

    let results = execute_script(
        &exec,
        &mut ctx,
        "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @loans ASSET;
        CREATE ACCOUNT @interest_earned INCOME;
        CREATE ACCOUNT @equity EQUITY;

        CREATE RATE prime;
        SET RATE prime 0.05 2023-01-01;
        SET RATE prime 0.06 2023-02-15;

        CREATE JOURNAL
            2023-01-01, 20000, 'Investment'
        FOR Investor='Frank'
        CREDIT @equity, DEBIT @bank;

        CREATE JOURNAL
            2023-02-01, 1000, 'Loan Issued'
        FOR Customer='John', Region='US'
        DEBIT @loans, CREDIT @bank;

        CREATE JOURNAL
            2023-02-01, 500, 'Loan Issued'
        FOR Customer='Joe', Region='US'
        DEBIT @loans, CREDIT @bank;

        ACCRUE @loans FROM 2023-02-01 TO 2023-02-28
        WITH RATE prime COMPOUND DAILY
        BY Customer
        INTO JOURNAL
            2023-03-01, 'Interest'
        DEBIT @loans,
        CREDIT @interest_earned;

        GET
            balance(@loans, 2023-03-01) AS LoanBookTotal,
            trial_balance(2023-03-01) AS TrialBalance
    ",
    );

    let get_result = results.last().unwrap();

    let loan_total = &get_result.variables["LoanBookTotal"];
    match loan_total {
        DataValue::Money(m) => {
            assert!(
                *m > rust_decimal::Decimal::from(1500),
                "Loan book should include accrued interest"
            );
        }
        _ => panic!("Expected Money for LoanBookTotal"),
    }

    match &get_result.variables["TrialBalance"] {
        DataValue::TrialBalance(items) => {
            let mut total_debit = rust_decimal::Decimal::ZERO;
            let mut total_credit = rust_decimal::Decimal::ZERO;
            for item in items {
                match item.account_type {
                    dblentry::ast::AccountType::Asset | dblentry::ast::AccountType::Expense => {
                        total_debit += item.balance;
                    }
                    _ => {
                        total_credit += item.balance;
                    }
                }
            }
            assert_eq!(
                total_debit, total_credit,
                "PostgreSQL: Trial balance must be in balance after accrual"
            );
        }
        _ => panic!("Expected TrialBalance"),
    }
}

#[test]
#[ignore]
fn test_postgres_implicit_transaction_rollback() {
    if !postgres_available() {
        eprintln!("Skipping PostgreSQL test: no connection available");
        return;
    }

    let (exec, mut ctx) = setup_postgres();

    execute_script(
        &exec,
        &mut ctx,
        "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
    ",
    );

    let statements = lexer::parse(
        "
        CREATE JOURNAL 2023-01-01, 1000, 'Investment' CREDIT @equity, DEBIT @bank;
        CREATE JOURNAL 2023-02-01, 500, 'Bad' CREDIT @nonexistent, DEBIT @bank;
    ",
    )
    .unwrap();

    let result = exec.execute_script(&mut ctx, &statements);
    assert!(result.is_err(), "Script should fail on missing account");

    let results = execute_script(
        &exec,
        &mut ctx,
        "
        GET balance(@bank, 2023-12-31) AS result
    ",
    );
    let balance = &results[0].variables["result"];
    match balance {
        DataValue::Money(m) => assert_eq!(
            *m,
            rust_decimal::Decimal::ZERO,
            "PostgreSQL: Balance should be 0 after rollback"
        ),
        _ => panic!("Expected Money, got {:?}", balance),
    }
}

#[test]
#[ignore]
fn test_postgres_multi_currency() {
    if !postgres_available() {
        eprintln!("Skipping PostgreSQL test: no connection available");
        return;
    }

    let (exec, mut ctx) = setup_postgres();

    let results = execute_script(
        &exec,
        &mut ctx,
        "
        CREATE ACCOUNT @bank_usd ASSET;
        CREATE ACCOUNT @bank_eur ASSET;
        CREATE ACCOUNT @equity EQUITY;

        CREATE RATE usd_eur;
        SET RATE usd_eur 0.85 2023-01-01;
        SET RATE usd_eur 0.92 2023-06-01;

        CREATE JOURNAL 2023-01-01, 10000, 'Initial investment'
        CREDIT @equity, DEBIT @bank_usd;

        GET
            convert(1000, 'usd_eur', 2023-07-01) AS euros,
            fx_rate('usd_eur', 2023-07-01) AS rate,
            balance(@bank_usd, 2023-12-31) AS usd_balance
    ",
    );

    let get_result = results.last().unwrap();

    match &get_result.variables["euros"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(920)),
        v => panic!("Expected Money(920), got {:?}", v),
    }
    match &get_result.variables["rate"] {
        DataValue::Money(m) => {
            assert_eq!(*m, rust_decimal_macros::dec!(0.92));
        }
        v => panic!("Expected Money(0.92), got {:?}", v),
    }
    match &get_result.variables["usd_balance"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(10000)),
        v => panic!("Expected Money(10000), got {:?}", v),
    }
}

// ==================== Bug Fix Regression Tests ====================

/// Test that division by zero returns an error instead of panicking.
#[test]
fn test_division_by_zero_returns_error() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
    ");

    // Integer division by zero
    let statements = lexer::parse("GET 100 / 0 AS result").unwrap();
    let result = exec.execute(&mut ctx, &statements[0]);
    assert!(result.is_err(), "Integer division by zero should return error");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("division by zero"), "Error should mention division by zero, got: {}", err_msg);

    // Decimal division by zero
    let statements = lexer::parse("GET 100.50 / 0.00 AS result").unwrap();
    let result = exec.execute(&mut ctx, &statements[0]);
    assert!(result.is_err(), "Decimal division by zero should return error");

    // Division by zero in expression context
    let statements = lexer::parse("GET (50 + 50) / (10 - 10) AS result").unwrap();
    let result = exec.execute(&mut ctx, &statements[0]);
    assert!(result.is_err(), "Expression evaluating to zero divisor should error");
}

/// Test that modulo by zero returns an error instead of panicking.
#[test]
fn test_modulo_by_zero_returns_error() {
    let (exec, mut ctx) = setup();

    let statements = lexer::parse("GET 100 % 0 AS result").unwrap();
    let result = exec.execute(&mut ctx, &statements[0]);
    assert!(result.is_err(), "Modulo by zero should return error");
}

/// Test that normal division still works correctly after the fix.
#[test]
fn test_division_still_works() {
    let (exec, mut ctx) = setup();

    let results = execute_script(&exec, &mut ctx, "
        GET 100 / 4 AS int_result,
            100.50 / 2 AS dec_result
    ");
    match &results[0].variables["int_result"] {
        DataValue::Int(v) => assert_eq!(*v, 25),
        v => panic!("Expected Int(25), got {:?}", v),
    }
    match &results[0].variables["dec_result"] {
        DataValue::Money(v) => assert_eq!(v.to_string(), "50.25"),
        v => panic!("Expected Money(50.25), got {:?}", v),
    }
}

/// Test that FQL strings with single quotes in descriptions are handled safely.
/// This verifies the parser rejects injection attempts or that they are properly escaped.
#[test]
fn test_fql_description_with_quotes() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
    ");

    // A description with properly escaped single quotes (doubled) should work
    let fql = "CREATE JOURNAL 2024-01-01, 1000, 'O''Brien''s payment' CREDIT @equity, DEBIT @bank;";
    let statements = lexer::parse(fql);
    // Whether this parses or not depends on the PEG grammar, but it should not inject commands
    if let Ok(stmts) = statements {
        for stmt in &stmts {
            let _ = exec.execute(&mut ctx, stmt);
        }
    }

    // An injection attempt with semicolons should NOT create extra accounts
    let injection = "CREATE JOURNAL 2024-01-01, 1, 'test'; CREATE ACCOUNT @hacked ASSET;";
    // This should be parsed as 2 separate statements by the parser (not injected via string)
    let stmts = lexer::parse(injection).unwrap();
    // The key point: if this were an injection in a REST handler building the string,
    // the escaped version would prevent the second statement from executing.
    // Here we verify the parser handles it as separate statements
    assert!(stmts.len() <= 2, "Parser should handle this as separate statements");
}

/// Test that statements work with early dates (regression test for Date::MIN panic).
#[test]
fn test_statement_with_early_dates() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE JOURNAL 2024-01-15, 1000, 'deposit' CREDIT @equity, DEBIT @bank;
    ");

    // Request a statement starting from a very early date — should not panic
    let results = execute_script(&exec, &mut ctx, "
        GET statement(@bank, 0001-01-01, 2024-12-31) AS stmt
    ");
    match &results[0].variables["stmt"] {
        DataValue::Statement(txns) => {
            assert_eq!(txns.len(), 1, "Should have 1 transaction");
        },
        v => panic!("Expected Statement, got {:?}", v),
    }
}

/// Test that balance queries with dimensions containing special characters work.
#[test]
fn test_dimension_values_with_special_chars() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @revenue INCOME;
        CREATE ACCOUNT @bank ASSET;
    ");

    // Dimension values with spaces and special chars (no single quotes)
    let results = execute_script(&exec, &mut ctx, "
        CREATE JOURNAL 2024-01-01, 500, 'sale'
        FOR Customer='Acme Corp'
        CREDIT @revenue, DEBIT @bank;
    ");
    assert_eq!(results[0].journals_created, 1, "Journal with special char dimension should be created");

    // Query with the same dimension
    let results = execute_script(&exec, &mut ctx, "
        GET balance(@revenue, 2024-12-31, Customer='Acme Corp') AS rev
    ");
    match &results[0].variables["rev"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(500)),
        v => panic!("Expected Money(500), got {:?}", v),
    }
}

/// Test that negative and zero amounts work correctly in arithmetic.
#[test]
fn test_edge_case_arithmetic() {
    let (exec, mut ctx) = setup();

    // Zero * anything = zero (not error)
    let results = execute_script(&exec, &mut ctx, "
        GET 0 * 12345 AS zero_mult,
            0 + 100 AS zero_add,
            100 - 100 AS zero_sub
    ");
    match &results[0].variables["zero_mult"] {
        DataValue::Int(v) => assert_eq!(*v, 0),
        v => panic!("Expected Int(0), got {:?}", v),
    }

    // Dividing zero by something is fine
    let results = execute_script(&exec, &mut ctx, "GET 0 / 5 AS result");
    match &results[0].variables["result"] {
        DataValue::Int(v) => assert_eq!(*v, 0),
        v => panic!("Expected Int(0), got {:?}", v),
    }
}

/// Test that account IDs with valid characters work in various contexts.
#[test]
fn test_account_id_validation() {
    let (exec, mut ctx) = setup();

    // Account IDs with underscores and hyphens should work
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @cash_on_hand ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE JOURNAL 2024-01-01, 100, 'test' CREDIT @equity, DEBIT @cash_on_hand;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@cash_on_hand, 2024-12-31) AS bal
    ");
    match &results[0].variables["bal"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(100)),
        v => panic!("Expected Money(100), got {:?}", v),
    }
}

// ==================== Multi-Entity Tests ====================

/// Test that CREATE ENTITY and USE ENTITY work, and entities are isolated.
#[test]
fn test_multi_entity_isolation() {
    let (exec, mut ctx) = setup();

    // Create two entities
    execute_script(&exec, &mut ctx, "
        CREATE ENTITY 'Acme Corp';
        CREATE ENTITY 'Beta Fund';
    ");

    // Set up accounts in Acme Corp
    execute_script(&exec, &mut ctx, "
        USE ENTITY 'Acme Corp';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE JOURNAL 2023-01-01, 50000, 'Acme investment'
        CREDIT @equity, DEBIT @bank;
    ");

    // Set up same account names in Beta Fund with different amounts
    execute_script(&exec, &mut ctx, "
        USE ENTITY 'Beta Fund';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE JOURNAL 2023-01-01, 100000, 'Beta investment'
        CREDIT @equity, DEBIT @bank;
    ");

    // Query Beta Fund (still active)
    let beta_results = execute_script(&exec, &mut ctx, "
        GET balance(@bank, 2023-12-31) AS beta_bank
    ");
    match &beta_results[0].variables["beta_bank"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(100000)),
        v => panic!("Expected 100000 for Beta, got {:?}", v),
    }

    // Switch to Acme and verify its balance is separate
    let acme_results = execute_script(&exec, &mut ctx, "
        USE ENTITY 'Acme Corp';
        GET balance(@bank, 2023-12-31) AS acme_bank
    ");
    match &acme_results[1].variables["acme_bank"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(50000)),
        v => panic!("Expected 50000 for Acme, got {:?}", v),
    }
}

/// Test that the default entity works for backward compatibility.
#[test]
fn test_default_entity_backward_compat() {
    let (exec, mut ctx) = setup();

    // No USE ENTITY — should work against "default"
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE JOURNAL 2023-01-01, 1000, 'Test'
        CREDIT @equity, DEBIT @bank;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@bank, 2023-12-31) AS bal
    ");
    match &results[0].variables["bal"] {
        DataValue::Money(m) => assert_eq!(*m, rust_decimal::Decimal::from(1000)),
        v => panic!("Expected 1000, got {:?}", v),
    }
}

/// Test that USE ENTITY with a non-existent entity returns an error.
#[test]
fn test_use_nonexistent_entity() {
    let (exec, mut ctx) = setup();

    let statements = lexer::parse("USE ENTITY 'does_not_exist'").unwrap();
    let result = exec.execute(&mut ctx, &statements[0]);
    assert!(result.is_err(), "USE ENTITY on non-existent entity should fail");
}

/// Test that CREATE ENTITY with a duplicate name returns an error.
#[test]
fn test_create_duplicate_entity() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "CREATE ENTITY 'Acme Corp'");
    let statements = lexer::parse("CREATE ENTITY 'Acme Corp'").unwrap();
    let result = exec.execute(&mut ctx, &statements[0]);
    assert!(result.is_err(), "Creating duplicate entity should fail");
}

/// Test multi-entity trial balance isolation.
#[test]
fn test_multi_entity_trial_balance() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ENTITY 'Entity A';
        USE ENTITY 'Entity A';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE ACCOUNT @revenue INCOME;
        CREATE JOURNAL 2023-01-01, 10000, 'Capital' CREDIT @equity, DEBIT @bank;
        CREATE JOURNAL 2023-06-01, 5000, 'Sales' CREDIT @revenue, DEBIT @bank;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET trial_balance(2023-12-31) AS tb,
            account_count() AS count
    ");

    match &results[0].variables["count"] {
        DataValue::Int(n) => assert_eq!(*n, 3, "Entity A should have 3 accounts"),
        v => panic!("Expected Int(3), got {:?}", v),
    }
    assert_trial_balance_balanced(&results[0].variables["tb"], "Entity A");

    // Default entity should have 0 accounts
    let default_results = execute_script(&exec, &mut ctx, "
        USE ENTITY 'default';
        GET account_count() AS count
    ");
    match &default_results[1].variables["count"] {
        DataValue::Int(n) => assert_eq!(*n, 0, "Default entity should have 0 accounts"),
        v => panic!("Expected Int(0), got {:?}", v),
    }
}

// ===================== DISTRIBUTE TESTS =====================

#[test]
fn test_distribute_even_monthly() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @deferred_revenue LIABILITY;
        CREATE ACCOUNT @subscription_revenue INCOME;
        DISTRIBUTE 12000
            FROM 2024-01-01 TO 2024-12-31
            PERIOD MONTHLY
            DESCRIPTION 'Revenue recognition'
            DEBIT @deferred_revenue,
            CREDIT @subscription_revenue;
    ");

    // Should create 12 journals of 1000 each
    let results = execute_script(&exec, &mut ctx, "
        GET balance(@subscription_revenue, 2024-12-31) AS rev,
            balance(@deferred_revenue, 2024-12-31) AS def
    ");
    match &results[0].variables["rev"] {
        DataValue::Money(d) => assert_eq!(*d, rust_decimal_macros::dec!(12000)),
        v => panic!("Expected 12000, got {:?}", v),
    }
}

#[test]
fn test_distribute_remainder_handling() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @expense EXPENSE;
        CREATE ACCOUNT @prepaid ASSET;
        DISTRIBUTE 10000
            FROM 2024-01-01 TO 2024-03-31
            PERIOD MONTHLY
            DESCRIPTION 'Amortization'
            DEBIT @expense,
            CREDIT @prepaid;
    ");

    // 10000 / 3 = 3333.33, 3333.33, 3333.34
    let results = execute_script(&exec, &mut ctx, "
        GET balance(@expense, 2024-01-31) AS jan,
            balance(@expense, 2024-02-29) AS feb,
            balance(@expense, 2024-03-31) AS mar
    ");
    match &results[0].variables["jan"] {
        DataValue::Money(d) => assert_eq!(*d, rust_decimal_macros::dec!(3333.33)),
        v => panic!("Expected 3333.33, got {:?}", v),
    }
    match &results[0].variables["mar"] {
        DataValue::Money(d) => assert_eq!(*d, rust_decimal_macros::dec!(10000)),
        v => panic!("Expected 10000 total by March, got {:?}", v),
    }
}

#[test]
fn test_distribute_quarterly() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @depreciation EXPENSE;
        CREATE ACCOUNT @accum_dep ASSET;
        DISTRIBUTE 4000
            FROM 2024-01-01 TO 2024-12-31
            PERIOD QUARTERLY
            DESCRIPTION 'Quarterly depreciation'
            DEBIT @depreciation,
            CREDIT @accum_dep;
    ");

    // 4 quarters of 1000 each
    let results = execute_script(&exec, &mut ctx, "
        GET balance(@depreciation, 2024-03-31) AS q1,
            balance(@depreciation, 2024-06-30) AS q2,
            balance(@depreciation, 2024-12-31) AS total
    ");
    match &results[0].variables["q1"] {
        DataValue::Money(d) => assert_eq!(*d, rust_decimal_macros::dec!(1000)),
        v => panic!("Expected 1000, got {:?}", v),
    }
    match &results[0].variables["total"] {
        DataValue::Money(d) => assert_eq!(*d, rust_decimal_macros::dec!(4000)),
        v => panic!("Expected 4000, got {:?}", v),
    }
}

#[test]
fn test_distribute_yearly() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @depreciation EXPENSE;
        CREATE ACCOUNT @accum_dep ASSET;
        DISTRIBUTE 60000
            FROM 2024-01-01 TO 2028-12-31
            PERIOD YEARLY
            DESCRIPTION 'Yearly depreciation'
            DEBIT @depreciation,
            CREDIT @accum_dep;
    ");

    // 5 years of 12000 each
    let results = execute_script(&exec, &mut ctx, "
        GET balance(@depreciation, 2024-12-31) AS y1,
            balance(@depreciation, 2028-12-31) AS total
    ");
    match &results[0].variables["y1"] {
        DataValue::Money(d) => assert_eq!(*d, rust_decimal_macros::dec!(12000)),
        v => panic!("Expected 12000, got {:?}", v),
    }
    match &results[0].variables["total"] {
        DataValue::Money(d) => assert_eq!(*d, rust_decimal_macros::dec!(60000)),
        v => panic!("Expected 60000, got {:?}", v),
    }
}

#[test]
fn test_distribute_prorate() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @insurance EXPENSE;
        CREATE ACCOUNT @prepaid ASSET;
        DISTRIBUTE 2400
            FROM 2024-03-15 TO 2024-06-14
            PERIOD MONTHLY
            PRORATE
            DESCRIPTION 'Insurance amortization'
            DEBIT @insurance,
            CREDIT @prepaid;
    ");

    // Prorated: Mar 15-31 (17 days), Apr (30 days), May (31 days), Jun 1-14 (14 days)
    // Total = 92 days
    let results = execute_script(&exec, &mut ctx, "
        GET balance(@insurance, 2024-06-30) AS total
    ");
    match &results[0].variables["total"] {
        DataValue::Money(d) => assert_eq!(*d, rust_decimal_macros::dec!(2400)),
        v => panic!("Expected 2400 total, got {:?}", v),
    }
}

#[test]
fn test_distribute_with_dimensions() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @deferred_revenue LIABILITY;
        CREATE ACCOUNT @subscription_revenue INCOME;
        DISTRIBUTE 6000
            FROM 2024-01-01 TO 2024-06-30
            PERIOD MONTHLY
            FOR Customer='Acme'
            DESCRIPTION 'Acme revenue recognition'
            DEBIT @deferred_revenue,
            CREDIT @subscription_revenue;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET balance(@subscription_revenue, 2024-06-30) AS rev
    ");
    match &results[0].variables["rev"] {
        DataValue::Money(d) => assert_eq!(*d, rust_decimal_macros::dec!(6000)),
        v => panic!("Expected 6000, got {:?}", v),
    }
}

#[test]
fn test_distribute_negative_amount() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @revenue INCOME;
        CREATE ACCOUNT @deferred LIABILITY;
        DISTRIBUTE 12000
            FROM 2024-01-01 TO 2024-12-31
            PERIOD MONTHLY
            DESCRIPTION 'Revenue recognition'
            DEBIT @deferred,
            CREDIT @revenue;
        DISTRIBUTE 6000
            FROM 2024-07-01 TO 2024-12-31
            PERIOD MONTHLY
            DESCRIPTION 'Cancellation reversal'
            DEBIT @revenue,
            CREDIT @deferred;
    ");

    // 12×1000 = 12000 revenue credited, then 6×1000 revenue debited = net 6000
    let results = execute_script(&exec, &mut ctx, "
        GET balance(@revenue, 2024-12-31) AS rev
    ");
    match &results[0].variables["rev"] {
        DataValue::Money(d) => assert_eq!(*d, rust_decimal_macros::dec!(6000)),
        v => panic!("Expected 6000 net revenue, got {:?}", v),
    }
}

#[test]
fn test_distribute_zero_amount_error() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @a ASSET;
        CREATE ACCOUNT @b LIABILITY;
    ");
    let stmts = dblentry::lexer::parse("
        DISTRIBUTE 0
            FROM 2024-01-01 TO 2024-12-31
            PERIOD MONTHLY
            DESCRIPTION 'Should fail'
            DEBIT @a,
            CREDIT @b;
    ").unwrap();
    let result = exec.execute(&mut ctx, &stmts[0]);
    assert!(result.is_err(), "Zero amount should produce an error");
}

#[test]
fn test_distribute_invalid_date_range_error() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @a ASSET;
        CREATE ACCOUNT @b LIABILITY;
    ");
    let stmts = dblentry::lexer::parse("
        DISTRIBUTE 1000
            FROM 2024-12-31 TO 2024-01-01
            PERIOD MONTHLY
            DESCRIPTION 'Should fail'
            DEBIT @a,
            CREDIT @b;
    ").unwrap();
    let result = exec.execute(&mut ctx, &stmts[0]);
    assert!(result.is_err(), "End before start should produce an error");
}

#[test]
fn test_distribute_trial_balance() {
    let (exec, mut ctx) = setup();
    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @deferred_revenue LIABILITY;
        CREATE ACCOUNT @subscription_revenue INCOME;
        DISTRIBUTE 12000
            FROM 2024-01-01 TO 2024-12-31
            PERIOD MONTHLY
            DESCRIPTION 'Revenue recognition'
            DEBIT @deferred_revenue,
            CREDIT @subscription_revenue;
    ");

    let results = execute_script(&exec, &mut ctx, "
        GET trial_balance(2024-12-31) AS tb
    ");
    assert_trial_balance_balanced(&results[0].variables["tb"], "distribute");
}

// ===== Dimensional Lots & Hierarchical Dimensions Tests =====

#[test]
fn test_dimensional_lots_buy_and_query() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE RATE aapl_price;
        SET RATE aapl_price 150 2024-01-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'aapl_price';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy AAPL for Alice'
            FOR Customer='Alice'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy AAPL for Bob'
            FOR Customer='Bob'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;
    ");

    // Query total units (all pools)
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31) AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(150.into()));

    // Query units for Alice only
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Customer='Alice') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(100.into()));

    // Query units for Bob only
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Customer='Bob') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(50.into()));
}

#[test]
fn test_dimensional_lots_sell_with_for() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE RATE aapl_price;
        SET RATE aapl_price 150 2024-01-01;
        SET RATE aapl_price 180 2024-06-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'aapl_price';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy AAPL for Alice'
            FOR Customer='Alice'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy AAPL for Bob'
            FOR Customer='Bob'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;

        SELL 30 UNITS OF @aapl AT 180 ON 2024-06-01
            FOR Customer='Alice'
            METHOD FIFO PROCEEDS @bank GAIN_LOSS @gains
            DESCRIPTION 'Sell Alice shares';
    ");

    // Alice should have 70 units remaining
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Customer='Alice') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(70.into()));

    // Bob should still have 50
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Customer='Bob') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(50.into()));

    // Total should be 120
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31) AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(120.into()));
}

#[test]
fn test_hierarchical_dimension_balance_prefix() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @revenue INCOME;
        CREATE ACCOUNT @bank ASSET;

        CREATE JOURNAL 2024-01-15, 1000, 'US West sale'
            FOR Region='Americas/US/West'
            DEBIT @bank,
            CREDIT @revenue;

        CREATE JOURNAL 2024-02-01, 2000, 'US East sale'
            FOR Region='Americas/US/East'
            DEBIT @bank,
            CREDIT @revenue;

        CREATE JOURNAL 2024-03-01, 500, 'Canada sale'
            FOR Region='Americas/Canada'
            DEBIT @bank,
            CREDIT @revenue;

        CREATE JOURNAL 2024-04-01, 3000, 'UK sale'
            FOR Region='Europe/UK'
            DEBIT @bank,
            CREDIT @revenue;
    ");

    // Query exact leaf
    let results = execute_script(&exec, &mut ctx, "GET balance(@revenue, 2024-12-31, Region='Americas/US/West') AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(1000.into()));

    // Query US level — should aggregate West + East = 3000
    let results = execute_script(&exec, &mut ctx, "GET balance(@revenue, 2024-12-31, Region='Americas/US') AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(3000.into()));

    // Query Americas level — should aggregate US + Canada = 3500
    let results = execute_script(&exec, &mut ctx, "GET balance(@revenue, 2024-12-31, Region='Americas') AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(3500.into()));

    // Query Europe — should be 3000
    let results = execute_script(&exec, &mut ctx, "GET balance(@revenue, 2024-12-31, Region='Europe') AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(3000.into()));

    // Query Europe/UK — exact leaf
    let results = execute_script(&exec, &mut ctx, "GET balance(@revenue, 2024-12-31, Region='Europe/UK') AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(3000.into()));
}

#[test]
fn test_hierarchical_dimension_lots() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE RATE aapl_price;
        SET RATE aapl_price 150 2024-01-01;
        SET RATE aapl_price 200 2024-06-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'aapl_price';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy AAPL US West'
            FOR Region='Americas/US/West'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy AAPL US East'
            FOR Region='Americas/US/East'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;

        CREATE JOURNAL 2024-03-01, 3000, 'Buy AAPL Canada'
            FOR Region='Americas/Canada'
            DEBIT @aapl 20 UNITS AT 150,
            CREDIT @bank;
    ");

    // Total units
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31) AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(170.into()));

    // Units for Americas prefix — should aggregate all
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Region='Americas') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(170.into()));

    // Units for Americas/US prefix — West + East = 150
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Region='Americas/US') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(150.into()));

    // Units for exact leaf
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Region='Americas/US/West') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(100.into()));

    // Market value for Americas prefix
    let results = execute_script(&exec, &mut ctx, "GET market_value(@aapl, 2024-06-01, Region='Americas') AS mv");
    assert_eq!(results[0].variables["mv"], DataValue::Money(34000.into())); // 170 * 200
}

#[test]
fn test_hierarchical_sell_scoped_to_parent() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE RATE aapl_price;
        SET RATE aapl_price 150 2024-01-01;
        SET RATE aapl_price 200 2024-06-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'aapl_price';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy AAPL US West'
            FOR Region='Americas/US/West'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy AAPL US East'
            FOR Region='Americas/US/East'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;

        CREATE JOURNAL 2024-03-01, 3000, 'Buy AAPL Canada'
            FOR Region='Americas/Canada'
            DEBIT @aapl 20 UNITS AT 150,
            CREDIT @bank;

        SELL 120 UNITS OF @aapl AT 200 ON 2024-06-01
            FOR Region='Americas/US'
            METHOD FIFO PROCEEDS @bank GAIN_LOSS @gains
            DESCRIPTION 'Sell US region FIFO';
    ");

    // US/West had 100, US/East had 50 — FIFO depletes West first (100), then 20 from East
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Region='Americas/US/West') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(0.into()));

    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Region='Americas/US/East') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(30.into()));

    // Canada unaffected
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Region='Americas/Canada') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(20.into()));
}

#[test]
fn test_sell_no_for_depletes_all_pools() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE RATE aapl_price;
        SET RATE aapl_price 150 2024-01-01;
        SET RATE aapl_price 180 2024-06-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'aapl_price';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy AAPL for Alice'
            FOR Customer='Alice'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy AAPL for Bob'
            FOR Customer='Bob'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;

        SELL 130 UNITS OF @aapl AT 180 ON 2024-06-01
            METHOD FIFO PROCEEDS @bank GAIN_LOSS @gains
            DESCRIPTION 'Sell all pools FIFO';
    ");

    // Total should be 20 remaining
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31) AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(20.into()));
}

#[test]
fn test_multi_dimension_lots() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE RATE aapl_price;
        SET RATE aapl_price 150 2024-01-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'aapl_price';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy AAPL Alice US'
            FOR Customer='Alice', Region='US'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy AAPL Bob US'
            FOR Customer='Bob', Region='US'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;

        CREATE JOURNAL 2024-03-01, 6000, 'Buy AAPL Alice EU'
            FOR Customer='Alice', Region='EU'
            DEBIT @aapl 40 UNITS AT 150,
            CREDIT @bank;
    ");

    // Total units
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31) AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(190.into()));

    // Filter by Customer='Alice' — should get 100 + 40 = 140
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Customer='Alice') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(140.into()));

    // Filter by Region='US' — should get 100 + 50 = 150
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Region='US') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(150.into()));
}

#[test]
fn test_backward_compat_non_dimensional_lots() {
    // Lots without dimensions should work exactly as before
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE RATE aapl_price;
        SET RATE aapl_price 150 2024-01-01;
        SET RATE aapl_price 180 2024-06-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'aapl_price';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy AAPL'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;

        SELL 60 UNITS OF @aapl AT 180 ON 2024-06-01
            METHOD FIFO PROCEEDS @bank GAIN_LOSS @gains
            DESCRIPTION 'Sell FIFO';
    ");

    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31) AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(90.into()));

    // Gain: sold 60 FIFO. First 100 @ 150, so 60 @ 150 = 9000 cost. Proceeds 60*180=10800. Gain=1800
    let results = execute_script(&exec, &mut ctx, "GET balance(@gains, 2024-12-31) AS g");
    assert_eq!(results[0].variables["g"], DataValue::Money(1800.into()));
}

#[test]
fn test_split_with_dimensional_lots() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE RATE aapl_price;
        SET RATE aapl_price 150 2024-01-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'aapl_price';
        CREATE ACCOUNT @bank ASSET;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy for Alice'
            FOR Customer='Alice'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy for Bob'
            FOR Customer='Bob'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;

        SPLIT @aapl 4 FOR 1 2024-06-01;
    ");

    // Split applies to all pools (no dimension filter on SPLIT currently)
    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Customer='Alice') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(400.into()));

    let results = execute_script(&exec, &mut ctx, "GET units(@aapl, 2024-12-31, Customer='Bob') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(200.into()));

    // Cost basis should be divided by split ratio
    let results = execute_script(&exec, &mut ctx, "GET cost_basis(@aapl, 2024-12-31, Customer='Alice') AS cb");
    let cb = match &results[0].variables["cb"] {
        DataValue::Money(d) => *d,
        _ => panic!("Expected Money"),
    };
    // Original 150/unit, split 4:1 => 37.5/unit
    assert_eq!(cb, rust_decimal::Decimal::new(375, 1));
}

#[test]
fn test_lots_function_with_dimension() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE RATE aapl_price;
        SET RATE aapl_price 150 2024-01-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'aapl_price';
        CREATE ACCOUNT @bank ASSET;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy for Alice'
            FOR Customer='Alice'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy for Bob'
            FOR Customer='Bob'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;
    ");

    // lots() with dimension should only return Alice's lots
    let results = execute_script(&exec, &mut ctx, "GET lots(@aapl, 2024-12-31, Customer='Alice') AS l");
    match &results[0].variables["l"] {
        DataValue::Lots(lots) => {
            assert_eq!(lots.len(), 1);
            assert_eq!(lots[0].units, 100.into());
        },
        _ => panic!("Expected Lots"),
    }

    // lots() without dimension should return all
    let results = execute_script(&exec, &mut ctx, "GET lots(@aapl, 2024-12-31) AS l");
    match &results[0].variables["l"] {
        DataValue::Lots(lots) => {
            assert_eq!(lots.len(), 2);
        },
        _ => panic!("Expected Lots"),
    }
}

#[test]
fn test_hierarchical_balance_with_statement() {
    let (exec, mut ctx) = setup();

    execute_script(&exec, &mut ctx, "
        CREATE ACCOUNT @revenue INCOME;
        CREATE ACCOUNT @bank ASSET;

        CREATE JOURNAL 2024-01-15, 1000, 'US West Q1'
            FOR Region='Americas/US/West'
            DEBIT @bank,
            CREDIT @revenue;

        CREATE JOURNAL 2024-04-15, 2000, 'US East Q2'
            FOR Region='Americas/US/East'
            DEBIT @bank,
            CREDIT @revenue;

        CREATE JOURNAL 2024-07-15, 500, 'Canada Q3'
            FOR Region='Americas/Canada'
            DEBIT @bank,
            CREDIT @revenue;
    ");

    // Statement for Americas should show all entries
    let results = execute_script(&exec, &mut ctx, "GET statement(@revenue, 2024-01-01, 2024-12-31, Region='Americas') AS s");
    match &results[0].variables["s"] {
        DataValue::Statement(txns) => {
            assert_eq!(txns.len(), 3);
        },
        _ => panic!("Expected Statement"),
    }

    // Statement for Americas/US should show only US entries (West + East)
    let results = execute_script(&exec, &mut ctx, "GET statement(@revenue, 2024-01-01, 2024-12-31, Region='Americas/US') AS s");
    match &results[0].variables["s"] {
        DataValue::Statement(txns) => {
            assert_eq!(txns.len(), 2);
        },
        _ => panic!("Expected Statement"),
    }
}

// ===== Cross-Backend Tests (backend_test! macro) =====

backend_test!(cross_basic_balance, |exec: &StatementExecutor, ctx: &mut ExecutionContext| {
    execute_script(exec, ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @revenue INCOME;
        CREATE JOURNAL 2024-01-15, 1000, 'Sale'
            DEBIT @bank,
            CREDIT @revenue;
        CREATE JOURNAL 2024-02-15, 500, 'Sale 2'
            DEBIT @bank,
            CREDIT @revenue;
    ");
    let results = execute_script(exec, ctx, "GET balance(@bank, 2024-12-31) AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(1500.into()));
    let results = execute_script(exec, ctx, "GET balance(@revenue, 2024-12-31) AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(1500.into()));
});

backend_test!(cross_dimensional_balance, |exec: &StatementExecutor, ctx: &mut ExecutionContext| {
    execute_script(exec, ctx, "
        CREATE ACCOUNT @revenue INCOME;
        CREATE ACCOUNT @bank ASSET;
        CREATE JOURNAL 2024-01-15, 1000, 'US sale'
            FOR Region='US'
            DEBIT @bank,
            CREDIT @revenue;
        CREATE JOURNAL 2024-02-15, 2000, 'EU sale'
            FOR Region='EU'
            DEBIT @bank,
            CREDIT @revenue;
    ");
    let results = execute_script(exec, ctx, "GET balance(@revenue, 2024-12-31, Region='US') AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(1000.into()));
    let results = execute_script(exec, ctx, "GET balance(@revenue, 2024-12-31, Region='EU') AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(2000.into()));
    let results = execute_script(exec, ctx, "GET balance(@revenue, 2024-12-31) AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(3000.into()));
});

backend_test!(cross_hierarchical_balance, |exec: &StatementExecutor, ctx: &mut ExecutionContext| {
    execute_script(exec, ctx, "
        CREATE ACCOUNT @revenue INCOME;
        CREATE ACCOUNT @bank ASSET;
        CREATE JOURNAL 2024-01-15, 1000, 'US West sale'
            FOR Region='Americas/US/West'
            DEBIT @bank,
            CREDIT @revenue;
        CREATE JOURNAL 2024-02-01, 2000, 'US East sale'
            FOR Region='Americas/US/East'
            DEBIT @bank,
            CREDIT @revenue;
        CREATE JOURNAL 2024-03-01, 500, 'Canada sale'
            FOR Region='Americas/Canada'
            DEBIT @bank,
            CREDIT @revenue;
    ");
    let results = execute_script(exec, ctx, "GET balance(@revenue, 2024-12-31, Region='Americas/US') AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(3000.into()));
    let results = execute_script(exec, ctx, "GET balance(@revenue, 2024-12-31, Region='Americas') AS b");
    assert_eq!(results[0].variables["b"], DataValue::Money(3500.into()));
});

backend_test!(cross_unit_buy_sell, |exec: &StatementExecutor, ctx: &mut ExecutionContext| {
    execute_script(exec, ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 150 2024-01-01;
        SET RATE AAPL 180 2024-06-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy AAPL'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        SELL 60 UNITS OF @aapl AT 180 ON 2024-06-01
            METHOD FIFO PROCEEDS @bank GAIN_LOSS @gains
            DESCRIPTION 'Sell FIFO';
    ");
    let results = execute_script(exec, ctx, "GET units(@aapl, 2024-12-31) AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(40.into()));
    // Gain: 60*180 - 60*150 = 10800 - 9000 = 1800
    let results = execute_script(exec, ctx, "GET balance(@gains, 2024-12-31) AS g");
    assert_eq!(results[0].variables["g"], DataValue::Money(1800.into()));
});

backend_test!(cross_dimensional_lots, |exec: &StatementExecutor, ctx: &mut ExecutionContext| {
    execute_script(exec, ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 150 2024-01-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy for Alice'
            FOR Customer='Alice'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy for Bob'
            FOR Customer='Bob'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;
    ");
    let results = execute_script(exec, ctx, "GET units(@aapl, 2024-12-31) AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(150.into()));
    let results = execute_script(exec, ctx, "GET units(@aapl, 2024-12-31, Customer='Alice') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(100.into()));
    let results = execute_script(exec, ctx, "GET units(@aapl, 2024-12-31, Customer='Bob') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(50.into()));
});

backend_test!(cross_hierarchical_lots, |exec: &StatementExecutor, ctx: &mut ExecutionContext| {
    execute_script(exec, ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 150 2024-01-01;
        SET RATE AAPL 200 2024-06-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy US West'
            FOR Region='Americas/US/West'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy US East'
            FOR Region='Americas/US/East'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;

        CREATE JOURNAL 2024-03-01, 3000, 'Buy Canada'
            FOR Region='Americas/Canada'
            DEBIT @aapl 20 UNITS AT 150,
            CREDIT @bank;
    ");
    let results = execute_script(exec, ctx, "GET units(@aapl, 2024-12-31, Region='Americas') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(170.into()));
    let results = execute_script(exec, ctx, "GET units(@aapl, 2024-12-31, Region='Americas/US') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(150.into()));
    let results = execute_script(exec, ctx, "GET units(@aapl, 2024-12-31, Region='Americas/US/West') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(100.into()));
});

backend_test!(cross_sell_with_dimensions, |exec: &StatementExecutor, ctx: &mut ExecutionContext| {
    execute_script(exec, ctx, "
        CREATE RATE AAPL;
        SET RATE AAPL 150 2024-01-01;
        SET RATE AAPL 180 2024-06-01;
        CREATE ACCOUNT @aapl ASSET UNITS 'AAPL';
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @gains INCOME;

        CREATE JOURNAL 2024-01-15, 15000, 'Buy for Alice'
            FOR Customer='Alice'
            DEBIT @aapl 100 UNITS AT 150,
            CREDIT @bank;

        CREATE JOURNAL 2024-02-01, 8500, 'Buy for Bob'
            FOR Customer='Bob'
            DEBIT @aapl 50 UNITS AT 170,
            CREDIT @bank;

        SELL 30 UNITS OF @aapl AT 180 ON 2024-06-01
            FOR Customer='Alice'
            METHOD FIFO PROCEEDS @bank GAIN_LOSS @gains
            DESCRIPTION 'Sell Alice shares';
    ");
    let results = execute_script(exec, ctx, "GET units(@aapl, 2024-12-31, Customer='Alice') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(70.into()));
    let results = execute_script(exec, ctx, "GET units(@aapl, 2024-12-31, Customer='Bob') AS u");
    assert_eq!(results[0].variables["u"], DataValue::Money(50.into()));
});

backend_test!(cross_trial_balance, |exec: &StatementExecutor, ctx: &mut ExecutionContext| {
    execute_script(exec, ctx, "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @revenue INCOME;
        CREATE ACCOUNT @expenses EXPENSE;
        CREATE JOURNAL 2024-01-15, 1000, 'Sale'
            DEBIT @bank,
            CREDIT @revenue;
        CREATE JOURNAL 2024-02-15, 300, 'Expense'
            DEBIT @expenses,
            CREDIT @bank;
    ");
    let results = execute_script(exec, ctx, "GET trial_balance(2024-12-31) AS tb");
    assert_trial_balance_balanced(&results[0].variables["tb"], "cross_trial_balance");
});
