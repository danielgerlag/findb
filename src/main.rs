use std::sync::Arc;

use axum::{Router, routing::{post, get}, extract::State, response::IntoResponse, http::StatusCode, Json};
use clap::Parser;
use findb::config::{CliArgs, Config};
use findb::functions::{Statement, TrialBalance};
use findb::{statement_executor::{StatementExecutor, ExecutionContext}, storage::InMemoryStorage, evaluator::{ExpressionEvaluator, QueryVariables}, function_registry::{FunctionRegistry, Function}, functions::Balance, lexer};
use serde::Serialize;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Serialize)]
struct FqlResponse {
    success: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    results: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    metadata: FqlMetadata,
}

#[derive(Serialize)]
struct FqlMetadata {
    statements_executed: usize,
    journals_created: usize,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
}

#[tokio::main]
async fn main() {
    let cli = CliArgs::parse();
    let config = Config::load(&cli);

    // Initialize tracing
    let filter = EnvFilter::try_new(&config.logging.level)
        .unwrap_or_else(|_| EnvFilter::new("info"));

    if config.logging.json {
        fmt().json().with_env_filter(filter).init();
    } else {
        fmt().with_env_filter(filter).init();
    }

    let storage = Arc::new(InMemoryStorage::new());
    let function_registry = FunctionRegistry::new();
    function_registry.register_function("balance", Function::Scalar(Arc::new(Balance::new(storage.clone()))));
    function_registry.register_function("statement", Function::Scalar(Arc::new(Statement::new(storage.clone()))));
    function_registry.register_function("trial_balance", Function::Scalar(Arc::new(TrialBalance::new(storage.clone()))));
    let expression_evaluator = Arc::new(ExpressionEvaluator::new(Arc::new(function_registry), storage.clone()));
    let exec = StatementExecutor::new(expression_evaluator, storage);
    
    let app = Router::new()
        .route("/fql", post(fql_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(health_handler))
        .route("/", post(fql_handler))
        .with_state(Arc::new(exec));

    let addr = config.listen_addr();
    tracing::info!("FinanceDB listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

async fn fql_handler(
    State(exec): State<Arc<StatementExecutor>>,
    query: String
) -> impl IntoResponse {
    let start = std::time::Instant::now();
    
    let statements = match lexer::parse(&query) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("FQL parse error: {}", e);
            let resp = FqlResponse {
                success: false,
                results: vec![],
                error: Some(format!("Parse error: {}", e)),
                metadata: FqlMetadata { statements_executed: 0, journals_created: 0 },
            };
            return (StatusCode::BAD_REQUEST, Json(resp));
        }
    };

    let eff_date = time::OffsetDateTime::now_utc().date();
    let mut context = ExecutionContext::new(eff_date, QueryVariables::new());
    let mut results = Vec::new();
    let mut total_journals = 0usize;
    let mut executed = 0usize;
    
    for statement in statements.iter() {
        match exec.execute(&mut context, statement) {
            Ok(result) => {
                total_journals += result.journals_created;
                let result_str = result.to_string();
                if !result_str.trim().is_empty() {
                    results.push(result_str);
                }
                executed += 1;
            },
            Err(e) => {
                tracing::error!("FQL execution error: {}", e);
                let resp = FqlResponse {
                    success: false,
                    results,
                    error: Some(format!("{}", e)),
                    metadata: FqlMetadata { statements_executed: executed, journals_created: total_journals },
                };
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(resp));
            }
        }
    }

    let duration = start.elapsed();
    tracing::debug!(
        statements = executed,
        journals = total_journals,
        duration_ms = duration.as_millis() as u64,
        "FQL query executed"
    );

    let resp = FqlResponse {
        success: true,
        results,
        error: None,
        metadata: FqlMetadata { statements_executed: executed, journals_created: total_journals },
    };
    (StatusCode::OK, Json(resp))
}
