use std::sync::Arc;

use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use metrics::{counter, histogram};

use crate::{
    api::{TextFqlResponse, TextFqlMetadata},
    evaluator::QueryVariables,
    lexer,
    statement_executor::{ExecutionContext, StatementExecutor},
};

use super::mappers;

fn wants_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("application/json"))
        .unwrap_or(false)
}

pub async fn fql_handler_v1(
    State(exec): State<Arc<StatementExecutor>>,
    headers: HeaderMap,
    query: String,
) -> impl IntoResponse {
    counter!("fql_requests_total", 1);
    let start = std::time::Instant::now();

    let statements = match lexer::parse(&query) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("FQL parse error: {}", e);
            counter!("fql_errors_total", 1, "type" => "parse");
            let duration = start.elapsed();
            histogram!("fql_request_duration_seconds", duration.as_secs_f64());

            if wants_json(&headers) {
                let resp = mappers::error_response(format!("Parse error: {}", e));
                return (StatusCode::BAD_REQUEST, Json(serde_json::to_value(resp).unwrap())).into_response();
            } else {
                let resp = TextFqlResponse {
                    success: false,
                    results: vec![],
                    error: Some(format!("Parse error: {}", e)),
                    metadata: TextFqlMetadata { statements_executed: 0, journals_created: 0 },
                };
                return (StatusCode::BAD_REQUEST, Json(resp)).into_response();
            }
        }
    };

    let eff_date = time::OffsetDateTime::now_utc().date();
    let mut context = ExecutionContext::new(eff_date, QueryVariables::new());

    match exec.execute_script(&mut context, &statements) {
        Ok(script_results) => {
            let duration = start.elapsed();
            histogram!("fql_request_duration_seconds", duration.as_secs_f64());

            let total_journals: usize = script_results.iter().map(|r| r.journals_created).sum();
            counter!("fql_statements_total", script_results.len() as u64);
            counter!("fql_journals_created_total", total_journals as u64);

            tracing::debug!(
                statements = script_results.len(),
                journals = total_journals,
                duration_ms = duration.as_millis() as u64,
                "FQL query executed"
            );

            if wants_json(&headers) {
                let resp = mappers::map_execution_results(&script_results);
                (StatusCode::OK, Json(serde_json::to_value(resp).unwrap())).into_response()
            } else {
                // Text format — existing behavior
                let mut results = Vec::new();
                for result in &script_results {
                    let result_str = result.to_string();
                    if !result_str.trim().is_empty() {
                        results.push(result_str);
                    }
                }
                let resp = TextFqlResponse {
                    success: true,
                    results,
                    error: None,
                    metadata: TextFqlMetadata {
                        statements_executed: script_results.len(),
                        journals_created: total_journals,
                    },
                };
                (StatusCode::OK, Json(resp)).into_response()
            }
        }
        Err(e) => {
            tracing::error!("FQL execution error: {}", e);
            counter!("fql_errors_total", 1, "type" => "execution");
            let duration = start.elapsed();
            histogram!("fql_request_duration_seconds", duration.as_secs_f64());

            if wants_json(&headers) {
                let resp = mappers::error_response(format!("{}", e));
                (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::to_value(resp).unwrap())).into_response()
            } else {
                let resp = TextFqlResponse {
                    success: false,
                    results: vec![],
                    error: Some(format!("{}", e)),
                    metadata: TextFqlMetadata { statements_executed: 0, journals_created: 0 },
                };
                (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response()
            }
        }
    }
}
