use std::sync::Arc;

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

    let storage = Arc::new(Storage::new());
    let function_registry = FunctionRegistry::new();
    function_registry.register_function("balance", Function::Scalar(Arc::new(Balance::new(storage.clone()))));
    let expression_evaluator = Arc::new(ExpressionEvaluator::new(Arc::new(function_registry)));
    let exec = StatementExecutor::new(expression_evaluator, storage);


    let query = 
    "CREATE JOURNAL 
        2020-01-01, 100, 'Test'
    FOR
        Customer='John Doe',
        Region='US'
    DEBIT bank 100
    CREDIT cash 100
    ";
    
    let statements = lexer::parse(query).unwrap();
    
    //println!("{:#?}", statements);
    
    let eff_date = Date::from_calendar_date(2020, Month::January, 1).unwrap();
    let mut context = ExecutionContext::new(eff_date, QueryVariables::new());
    
    for statement in statements.iter() {
        let result = exec.execute(&mut context, statement).unwrap();

        println!("{:#?}", result)
    }
    

    
}
