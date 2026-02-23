use std::sync::Arc;

use axum::{Router, routing::post, extract::State, response::IntoResponse};
use findb::functions::{Statement, TrialBalance};
use findb::{statement_executor::{StatementExecutor, ExecutionContext}, storage::InMemoryStorage, evaluator::{ExpressionEvaluator, QueryVariables}, function_registry::{FunctionRegistry, Function}, functions::Balance, lexer};

#[tokio::main]
async fn main() {

    env_logger::init_from_env(env_logger::Env::new().default_filter_or("debug"));

    let storage = Arc::new(InMemoryStorage::new());
    let function_registry = FunctionRegistry::new();
    function_registry.register_function("balance", Function::Scalar(Arc::new(Balance::new(storage.clone()))));
    function_registry.register_function("statement", Function::Scalar(Arc::new(Statement::new(storage.clone()))));
    function_registry.register_function("trial_balance", Function::Scalar(Arc::new(TrialBalance::new(storage.clone()))));
    let expression_evaluator = Arc::new(ExpressionEvaluator::new(Arc::new(function_registry), storage.clone()));
    let exec = StatementExecutor::new(expression_evaluator, storage);
    
    let app = Router::new()
        .route("/", post(handler))
        .with_state(Arc::new(exec));

    log::info!("API listening on port 3000");

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}


async fn handler(
    State(exec): State<Arc<StatementExecutor>>,
    query: String
) -> impl IntoResponse {
    let mut results = String::new();
    let statements = match lexer::parse(&query) {
        Ok(s) => s,
        Err(e) => return format!("Parse error: {}", e),
    };

    let eff_date = time::OffsetDateTime::now_utc().date();
    let mut context = ExecutionContext::new(eff_date, QueryVariables::new());
    
    for statement in statements.iter() {
        match exec.execute(&mut context, statement) {
            Ok(result) => {
                let result_str = result.to_string();
                results.push_str(result_str.as_str());
                results.push_str("\n");
            },
            Err(e) => {
                results.push_str(&format!("Error: {:?}\n", e));
                break;
            }
        }
    }

    results
}
