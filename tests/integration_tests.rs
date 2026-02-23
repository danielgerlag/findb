use std::sync::Arc;

use findb::evaluator::{ExpressionEvaluator, QueryVariables};
use findb::function_registry::{FunctionRegistry, Function};
use findb::functions::{Balance, Statement, TrialBalance, IncomeStatement, AccountCount};
use findb::lexer;
use findb::models::DataValue;
use findb::statement_executor::{ExecutionContext, StatementExecutor};
use findb::storage::InMemoryStorage;

fn setup() -> (StatementExecutor, ExecutionContext) {
    let storage = Arc::new(InMemoryStorage::new());
    let function_registry = FunctionRegistry::new();
    function_registry.register_function("balance", Function::Scalar(Arc::new(Balance::new(storage.clone()))));
    function_registry.register_function("statement", Function::Scalar(Arc::new(Statement::new(storage.clone()))));
    function_registry.register_function("trial_balance", Function::Scalar(Arc::new(TrialBalance::new(storage.clone()))));
    function_registry.register_function("income_statement", Function::Scalar(Arc::new(IncomeStatement::new(storage.clone()))));
    function_registry.register_function("account_count", Function::Scalar(Arc::new(AccountCount::new(storage.clone()))));
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

// --- SQLite backend tests ---

fn setup_sqlite() -> (StatementExecutor, ExecutionContext) {
    use findb::sqlite_storage::SqliteStorage;
    use findb::storage::StorageBackend;
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::new(":memory:").unwrap());
    let function_registry = FunctionRegistry::new();
    function_registry.register_function("balance", Function::Scalar(Arc::new(Balance::new(storage.clone()))));
    function_registry.register_function("statement", Function::Scalar(Arc::new(Statement::new(storage.clone()))));
    function_registry.register_function("trial_balance", Function::Scalar(Arc::new(TrialBalance::new(storage.clone()))));
    function_registry.register_function("income_statement", Function::Scalar(Arc::new(IncomeStatement::new(storage.clone()))));
    function_registry.register_function("account_count", Function::Scalar(Arc::new(AccountCount::new(storage.clone()))));
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
