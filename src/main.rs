use std::sync::Arc;

use axum::{Router, routing::{post, get}, extract::{State, Path, Query}, response::IntoResponse, http::StatusCode, Json, middleware, Extension};
use clap::Parser;
use findb::auth::auth_middleware;
use findb::config::{CliArgs, Config};
use findb::functions::{Statement, TrialBalance};
use findb::{statement_executor::{StatementExecutor, ExecutionContext}, storage::InMemoryStorage, evaluator::{ExpressionEvaluator, QueryVariables}, function_registry::{FunctionRegistry, Function}, functions::Balance, lexer};
use metrics::{counter, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::{Serialize, Deserialize};
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

    // Install Prometheus metrics recorder
    let prom_handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");

    let function_registry = FunctionRegistry::new();
    function_registry.register_function("balance", Function::Scalar(Arc::new(Balance::new(storage.clone()))));
    function_registry.register_function("statement", Function::Scalar(Arc::new(Statement::new(storage.clone()))));
    function_registry.register_function("trial_balance", Function::Scalar(Arc::new(TrialBalance::new(storage.clone()))));
    let expression_evaluator = Arc::new(ExpressionEvaluator::new(Arc::new(function_registry), storage.clone()));
    let exec = StatementExecutor::new(expression_evaluator, storage);
    let state = Arc::new(exec);
    
    let auth_config = Arc::new(config.auth.clone());

    // Protected routes (auth middleware applied)
    let protected = Router::new()
        .route("/fql", post(fql_handler))
        .route("/api/accounts", post(rest_create_account).get(rest_list_accounts))
        .route("/api/accounts/:id/balance", get(rest_get_balance))
        .route("/api/accounts/:id/statement", get(rest_get_statement))
        .route("/api/rates", post(rest_create_rate))
        .route("/api/rates/:id", post(rest_set_rate))
        .route("/api/journals", post(rest_create_journal))
        .route("/api/trial-balance", get(rest_trial_balance))
        .route("/", post(fql_handler))
        .with_state(state)
        .layer(Extension(auth_config))
        .layer(middleware::from_fn(auth_middleware));

    // Public routes (no auth)
    let public = Router::new()
        .route("/health", get(health_handler))
        .route("/ready", get(health_handler))
        .route("/metrics", get({
            let handle = prom_handle;
            move || std::future::ready(handle.render())
        }));

    let app = public.merge(protected);

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
    counter!("fql_requests_total", 1);
    let start = std::time::Instant::now();
    
    let statements = match lexer::parse(&query) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("FQL parse error: {}", e);
            counter!("fql_errors_total", 1, "type" => "parse");
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
    
    match exec.execute_script(&mut context, &statements) {
        Ok(script_results) => {
            let mut results = Vec::new();
            let mut total_journals = 0usize;
            for result in &script_results {
                total_journals += result.journals_created;
                let result_str = result.to_string();
                if !result_str.trim().is_empty() {
                    results.push(result_str);
                }
            }

            let duration = start.elapsed();
            histogram!("fql_request_duration_seconds", duration.as_secs_f64());
            counter!("fql_statements_total", script_results.len() as u64);
            counter!("fql_journals_created_total", total_journals as u64);

            tracing::debug!(
                statements = script_results.len(),
                journals = total_journals,
                duration_ms = duration.as_millis() as u64,
                "FQL query executed"
            );

            let resp = FqlResponse {
                success: true,
                results,
                error: None,
                metadata: FqlMetadata { statements_executed: script_results.len(), journals_created: total_journals },
            };
            (StatusCode::OK, Json(resp))
        }
        Err(e) => {
            tracing::error!("FQL execution error: {}", e);
            counter!("fql_errors_total", 1, "type" => "execution");
            let duration = start.elapsed();
            histogram!("fql_request_duration_seconds", duration.as_secs_f64());

            let resp = FqlResponse {
                success: false,
                results: vec![],
                error: Some(format!("{}", e)),
                metadata: FqlMetadata { statements_executed: 0, journals_created: 0 },
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(resp))
        }
    }
}

// --- REST API types ---

#[derive(Deserialize)]
struct CreateAccountRequest {
    id: String,
    account_type: String,
}

#[derive(Deserialize)]
struct CreateRateRequest {
    id: String,
}

#[derive(Deserialize)]
struct SetRateRequest {
    rate: String,
    date: String,
}

#[derive(Deserialize)]
struct CreateJournalRequest {
    date: String,
    amount: String,
    description: String,
    #[serde(default)]
    dimensions: std::collections::HashMap<String, String>,
    operations: Vec<JournalOperationRequest>,
}

#[derive(Deserialize)]
struct JournalOperationRequest {
    #[serde(rename = "type")]
    op_type: String,
    account: String,
    #[serde(default)]
    amount: Option<String>,
}

#[derive(Deserialize)]
struct BalanceQuery {
    date: String,
    #[serde(default)]
    dimension_key: Option<String>,
    #[serde(default)]
    dimension_value: Option<String>,
}

#[derive(Deserialize)]
struct StatementQuery {
    from: String,
    to: String,
    #[serde(default)]
    dimension_key: Option<String>,
    #[serde(default)]
    dimension_value: Option<String>,
}

#[derive(Deserialize)]
struct TrialBalanceQuery {
    date: String,
}

#[derive(Serialize)]
struct RestResponse<T: Serialize> {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn rest_ok<T: Serialize>(data: T) -> (StatusCode, Json<RestResponse<T>>) {
    (StatusCode::OK, Json(RestResponse { success: true, data: Some(data), error: None }))
}

fn rest_created<T: Serialize>(data: T) -> (StatusCode, Json<RestResponse<T>>) {
    (StatusCode::CREATED, Json(RestResponse { success: true, data: Some(data), error: None }))
}

fn rest_err<T: Serialize>(status: StatusCode, msg: String) -> (StatusCode, Json<RestResponse<T>>) {
    (status, Json(RestResponse { success: false, data: None, error: Some(msg) }))
}

// --- REST API handlers ---

async fn rest_create_account(
    State(exec): State<Arc<StatementExecutor>>,
    Json(req): Json<CreateAccountRequest>,
) -> impl IntoResponse {
    let fql = format!("CREATE ACCOUNT @{} {}", req.id, req.account_type.to_uppercase());
    execute_fql_rest(&exec, &fql).await
}

async fn rest_list_accounts(
    State(exec): State<Arc<StatementExecutor>>,
) -> impl IntoResponse {
    let fql = "GET trial_balance(2099-12-31) AS accounts";
    // Use a simple FQL to list - returns the accounts via trial balance
    execute_fql_rest(&exec, fql).await
}

async fn rest_create_rate(
    State(exec): State<Arc<StatementExecutor>>,
    Json(req): Json<CreateRateRequest>,
) -> impl IntoResponse {
    let fql = format!("CREATE RATE {}", req.id);
    execute_fql_rest(&exec, &fql).await
}

async fn rest_set_rate(
    State(exec): State<Arc<StatementExecutor>>,
    Path(id): Path<String>,
    Json(req): Json<SetRateRequest>,
) -> impl IntoResponse {
    let fql = format!("SET RATE {} {} {}", id, req.rate, req.date);
    execute_fql_rest(&exec, &fql).await
}

async fn rest_create_journal(
    State(exec): State<Arc<StatementExecutor>>,
    Json(req): Json<CreateJournalRequest>,
) -> impl IntoResponse {
    let mut fql = format!("CREATE JOURNAL {}, {}, '{}'", req.date, req.amount, req.description);
    
    if !req.dimensions.is_empty() {
        let dims: Vec<String> = req.dimensions.iter()
            .map(|(k, v)| format!("{}='{}'", k, v))
            .collect();
        fql.push_str(&format!(" FOR {}", dims.join(", ")));
    }
    
    let ops: Vec<String> = req.operations.iter().map(|op| {
        let mut s = format!("{} @{}", op.op_type.to_uppercase(), op.account);
        if let Some(ref amt) = op.amount {
            s.push_str(&format!(" {}", amt));
        }
        s
    }).collect();
    fql.push_str(&format!(" {}", ops.join(", ")));
    
    execute_fql_rest(&exec, &fql).await
}

async fn rest_get_balance(
    State(exec): State<Arc<StatementExecutor>>,
    Path(id): Path<String>,
    Query(params): Query<BalanceQuery>,
) -> impl IntoResponse {
    let dim = match (&params.dimension_key, &params.dimension_value) {
        (Some(k), Some(v)) => format!(", {}='{}'", k, v),
        _ => String::new(),
    };
    let fql = format!("GET balance(@{}, {}{}) AS result", id, params.date, dim);
    execute_fql_rest(&exec, &fql).await
}

async fn rest_get_statement(
    State(exec): State<Arc<StatementExecutor>>,
    Path(id): Path<String>,
    Query(params): Query<StatementQuery>,
) -> impl IntoResponse {
    let dim = match (&params.dimension_key, &params.dimension_value) {
        (Some(k), Some(v)) => format!(", {}='{}'", k, v),
        _ => String::new(),
    };
    let fql = format!("GET statement(@{}, {}, {}{}) AS result", id, params.from, params.to, dim);
    execute_fql_rest(&exec, &fql).await
}

async fn rest_trial_balance(
    State(exec): State<Arc<StatementExecutor>>,
    Query(params): Query<TrialBalanceQuery>,
) -> impl IntoResponse {
    let fql = format!("GET trial_balance({}) AS result", params.date);
    execute_fql_rest(&exec, &fql).await
}

async fn execute_fql_rest(
    exec: &StatementExecutor,
    fql: &str,
) -> (StatusCode, Json<RestResponse<String>>) {
    let statements = match lexer::parse(fql) {
        Ok(s) => s,
        Err(e) => return rest_err(StatusCode::BAD_REQUEST, format!("Internal FQL error: {}", e)),
    };

    let eff_date = time::OffsetDateTime::now_utc().date();
    let mut context = ExecutionContext::new(eff_date, QueryVariables::new());
    let mut output = String::new();

    for statement in &statements {
        match exec.execute(&mut context, statement) {
            Ok(result) => {
                let s = result.to_string();
                if !s.trim().is_empty() {
                    output.push_str(&s);
                }
            },
            Err(e) => return rest_err(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)),
        }
    }

    if output.is_empty() {
        rest_created("ok".to_string())
    } else {
        rest_ok(output)
    }
}
