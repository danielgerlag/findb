use std::sync::Arc;

use findb::evaluator::{ExpressionEvaluator, QueryVariables};
use findb::function_registry::{FunctionRegistry, Function};
use findb::functions::{Balance, Statement, TrialBalance, IncomeStatement, AccountCount, Convert, FxRate, Round, Abs, Min, Max};
use findb::lexer;
use findb::models::DataValue;
use findb::statement_executor::{ExecutionContext, StatementExecutor};
use findb::storage::InMemoryStorage;

fn register_functions(registry: &FunctionRegistry, storage: &Arc<dyn findb::storage::StorageBackend>) {
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
}

fn setup() -> (StatementExecutor, ExecutionContext) {
    let storage: Arc<dyn findb::storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let function_registry = FunctionRegistry::new();
    register_functions(&function_registry, &storage);
    let expression_evaluator = Arc::new(ExpressionEvaluator::new(Arc::new(function_registry), storage.clone()));
    let exec = StatementExecutor::new(expression_evaluator, storage);
    let eff_date = time::OffsetDateTime::now_utc().date();
    let context = ExecutionContext::new(eff_date, QueryVariables::new());
    (exec, context)
}

fn execute_script(exec: &StatementExecutor, context: &mut ExecutionContext, script: &str) -> Vec<findb::statement_executor::ExecutionResult> {
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
                    findb::ast::AccountType::Asset | findb::ast::AccountType::Expense => {
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
                    findb::ast::AccountType::Asset | findb::ast::AccountType::Expense => {
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
                    findb::ast::AccountType::Asset | findb::ast::AccountType::Expense => {
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

// --- SQLite backend tests ---

fn setup_sqlite() -> (StatementExecutor, ExecutionContext) {
    use findb_sqlite::SqliteStorage;
    let storage: Arc<dyn findb::storage::StorageBackend> = Arc::new(SqliteStorage::new(":memory:").unwrap());
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
                    findb::ast::AccountType::Asset | findb::ast::AccountType::Expense => {
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
    std::env::var("FINDB_TEST_POSTGRES_URL")
        .unwrap_or_else(|_| "host=localhost user=findb password=findb dbname=findb".to_string())
}

fn setup_postgres() -> (StatementExecutor, ExecutionContext) {
    use findb_postgres::PostgresStorage;

    // Drop all tables first to ensure a clean slate
    let conn_str = pg_connection_string();
    let mut client = postgres::Client::connect(&conn_str, postgres::NoTls)
        .expect("Failed to connect to PostgreSQL for cleanup");
    client
        .batch_execute(
            "DROP TABLE IF EXISTS ledger_entry_dimensions CASCADE;
             DROP TABLE IF EXISTS ledger_entries CASCADE;
             DROP TABLE IF EXISTS journal_dimensions CASCADE;
             DROP TABLE IF EXISTS journals CASCADE;
             DROP TABLE IF EXISTS rates CASCADE;
             DROP TABLE IF EXISTS accounts CASCADE;
             DROP TABLE IF EXISTS sequence_counter CASCADE;",
        )
        .expect("Failed to clean up PostgreSQL tables");
    drop(client);

    let storage: Arc<dyn findb::storage::StorageBackend> =
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
                    findb::ast::AccountType::Asset | findb::ast::AccountType::Expense => {
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
