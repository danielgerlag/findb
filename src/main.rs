use std::sync::Arc;

use functions::Statement;
use models::DataValue;
use time::{Date, Month};

use crate::{statement_executor::{StatementExecutor, ExecutionContext}, storage::Storage, evaluator::{ExpressionEvaluator, QueryVariables}, function_registry::{FunctionRegistry, Function}, functions::Balance};

pub mod ast;
pub mod lexer;
pub mod evaluator;
pub mod statement_executor;
pub mod models;
pub mod storage;
pub mod function_registry;
pub mod functions;

fn main() {

    env_logger::init_from_env(env_logger::Env::new().default_filter_or("debug"));

    let storage = Arc::new(Storage::new());
    let function_registry = FunctionRegistry::new();
    function_registry.register_function("balance", Function::Scalar(Arc::new(Balance::new(storage.clone()))));
    function_registry.register_function("statement", Function::Scalar(Arc::new(Statement::new(storage.clone()))));
    let expression_evaluator = Arc::new(ExpressionEvaluator::new(Arc::new(function_registry), storage.clone()));
    let exec = StatementExecutor::new(expression_evaluator, storage);


    let query = 
    "CREATE ACCOUNT @bank ASSET;
    CREATE ACCOUNT @loans ASSET;
    CREATE ACCOUNT @interest_earned INCOME;
    CREATE ACCOUNT @equity EQUITY;
    

    CREATE RATE prime;
    SET RATE prime 0.05 2023-01-01;
    
    CREATE JOURNAL 
        2023-01-01, 20000, 'Investment'
    FOR
        Investor='John Doe'
    CREDIT @equity,
    DEBIT @bank;

    CREATE JOURNAL 
        2023-02-01, 1000, 'Loan Issued'
    FOR
        Customer='John Doe',
        Region='US'
    DEBIT @loans,
    CREDIT @bank;

    CREATE JOURNAL 
        2023-02-01, 500, 'Loan Issued'
    FOR
        Customer='Joe Soap',
        Region='US'
    DEBIT @loans,
    CREDIT @bank;

    ACCRUE @loans FROM 2023-02-01 TO 2023-02-28
    WITH RATE prime COMPOUND DAILY
    BY Customer
    INTO JOURNAL
        2023-03-01, 'Interest'
    DEBIT @loans,
    CREDIT @interest_earned;
    

    GET 
        statement(@loans, 2023-02-01, 2023-03-01, Customer='John Doe') as John,
        statement(@loans, 2023-02-01, 2023-03-01, Customer='Joe Soap') as Joe,
        balance(@loans, 2023-03-01) AS Total
    ";
    // "GET 
    //     balance(@bank, $date, Customer='Frank Doe') AS Frank,
    //     balance(@bank, $date, Customer='John Doe') AS John,
    //     balance(@bank, $date, Region='US') AS US,
    //     balance(@bank, $date) AS Total

    //      statement(@bank, 2023-05-01, 2023-06-01, Region='US') as John,
    // ";
    
    let statements = lexer::parse(query).unwrap();
    
    //println!("{:#?}", statements);
    
    let eff_date = Date::from_calendar_date(2020, Month::January, 1).unwrap();
    let mut context = ExecutionContext::new(eff_date, QueryVariables::new());
    context.variables.insert("date".into(), DataValue::Date(Date::from_calendar_date(2023, Month::May, 20).unwrap()));
    
    for statement in statements.iter() {
        let result = exec.execute(&mut context, statement).unwrap();

        println!("{:#?}", result)
    }
    

    
}
