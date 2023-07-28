use std::sync::Arc;

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
    let expression_evaluator = Arc::new(ExpressionEvaluator::new(Arc::new(function_registry), storage.clone()));
    let exec = StatementExecutor::new(expression_evaluator, storage);


    let query = 
    "CREATE ACCOUNT @bank ASSET;
    CREATE ACCOUNT @cash ASSET;

    CREATE RATE prime;
    SET RATE prime 0.05 2023-01-01;
    SET RATE prime 0.2 2023-06-01;
    
    CREATE JOURNAL 
        2023-05-15, 100, 'Test'
    FOR
        Customer='John Doe',
        Region='US'
    DEBIT @bank|
    CREDIT @cash 100;

    CREATE JOURNAL 
        2023-05-17, 50, 'Test'
    FOR
        Customer='Frank Doe',
        Region='US'
    DEBIT @bank WITH RATE prime |
    CREDIT @cash 50;

    GET 
        balance(@bank, $date, Customer='Frank Doe') AS Frank,
        balance(@bank, $date, Customer='John Doe') AS John,
        balance(@bank, $date, Region='US') AS US,
        balance(@bank, $date) AS Total
    ";
    
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
