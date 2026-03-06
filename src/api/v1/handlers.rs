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

use super::types::{BatchFqlRequest, BatchFqlResponse, BatchResultEntry, FqlMetadataDto, ResultEntryDto};

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

pub async fn batch_fql_handler(
    State(exec): State<Arc<StatementExecutor>>,
    Json(req): Json<BatchFqlRequest>,
) -> impl IntoResponse {
    counter!("fql_requests_total", 1);
    let start = std::time::Instant::now();

    if req.statements.len() > 100 {
        return (StatusCode::BAD_REQUEST, Json(BatchFqlResponse {
            success: false,
            results: vec![],
            error: Some("Batch size exceeds maximum of 100 statements".to_string()),
            metadata: FqlMetadataDto { statements_executed: 0, journals_created: 0 },
        }));
    }

    let eff_date = time::OffsetDateTime::now_utc().date();
    let mut context = ExecutionContext::new(eff_date, QueryVariables::new());
    let mut results = Vec::new();
    let mut total_journals = 0usize;
    let mut total_statements = 0usize;
    let mut all_success = true;

    if req.transaction {
        // Transactional mode: parse all first, then execute as one script
        let mut parsed_batches: Vec<(String, Vec<crate::ast::Statement>)> = Vec::new();
        for entry in &req.statements {
            match lexer::parse(&entry.fql) {
                Ok(stmts) => parsed_batches.push((entry.id.clone(), stmts)),
                Err(e) => {
                    return (StatusCode::BAD_REQUEST, Json(BatchFqlResponse {
                        success: false,
                        results: vec![BatchResultEntry {
                            id: entry.id.clone(),
                            success: false,
                            data: vec![],
                            error: Some(format!("Parse error: {}", e)),
                        }],
                        error: Some(format!("Parse error in statement '{}': {}", entry.id, e)),
                        metadata: FqlMetadataDto { statements_executed: 0, journals_created: 0 },
                    }));
                }
            }
        }

        let mut all_stmts = Vec::new();
        let mut stmt_boundaries: Vec<(String, usize, usize)> = Vec::new();
        for (id, stmts) in &parsed_batches {
            let start_idx = all_stmts.len();
            let count = stmts.len();
            all_stmts.extend(stmts.iter().cloned());
            stmt_boundaries.push((id.clone(), start_idx, count));
        }

        match exec.execute_script(&mut context, &all_stmts) {
            Ok(script_results) => {
                let mut result_idx = 0;
                for (id, _, count) in &stmt_boundaries {
                    let mut entry_data = Vec::new();
                    let mut entry_journals = 0;
                    for _ in 0..*count {
                        if result_idx < script_results.len() {
                            let r = &script_results[result_idx];
                            entry_journals += r.journals_created;
                            for (name, value) in &r.variables {
                                entry_data.push(ResultEntryDto {
                                    name: name.to_string(),
                                    value: mappers::map_data_value(value),
                                });
                            }
                            result_idx += 1;
                        }
                    }
                    total_journals += entry_journals;
                    total_statements += *count;
                    results.push(BatchResultEntry {
                        id: id.clone(),
                        success: true,
                        data: entry_data,
                        error: None,
                    });
                }
            }
            Err(e) => {
                let duration = start.elapsed();
                histogram!("fql_request_duration_seconds", duration.as_secs_f64());
                counter!("fql_errors_total", 1, "type" => "execution");
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(BatchFqlResponse {
                    success: false,
                    results: vec![],
                    error: Some(format!("{}", e)),
                    metadata: FqlMetadataDto { statements_executed: 0, journals_created: 0 },
                }));
            }
        }
    } else {
        // Non-transactional mode: execute each entry independently
        for entry in &req.statements {
            match lexer::parse(&entry.fql) {
                Ok(stmts) => {
                    let mut entry_context = ExecutionContext::new(eff_date, context.variables.clone());
                    match exec.execute_script(&mut entry_context, &stmts) {
                        Ok(script_results) => {
                            let mut entry_data = Vec::new();
                            let mut entry_journals = 0;
                            for r in &script_results {
                                entry_journals += r.journals_created;
                                for (name, value) in &r.variables {
                                    entry_data.push(ResultEntryDto {
                                        name: name.to_string(),
                                        value: mappers::map_data_value(value),
                                    });
                                }
                            }
                            total_journals += entry_journals;
                            total_statements += script_results.len();
                            context.variables.extend(entry_context.variables);
                            results.push(BatchResultEntry {
                                id: entry.id.clone(),
                                success: true,
                                data: entry_data,
                                error: None,
                            });
                        }
                        Err(e) => {
                            all_success = false;
                            results.push(BatchResultEntry {
                                id: entry.id.clone(),
                                success: false,
                                data: vec![],
                                error: Some(format!("{}", e)),
                            });
                        }
                    }
                }
                Err(e) => {
                    all_success = false;
                    results.push(BatchResultEntry {
                        id: entry.id.clone(),
                        success: false,
                        data: vec![],
                        error: Some(format!("Parse error: {}", e)),
                    });
                }
            }
        }
    }

    let duration = start.elapsed();
    histogram!("fql_request_duration_seconds", duration.as_secs_f64());
    counter!("fql_statements_total", total_statements as u64);
    counter!("fql_journals_created_total", total_journals as u64);

    (StatusCode::OK, Json(BatchFqlResponse {
        success: all_success,
        results,
        error: None,
        metadata: FqlMetadataDto {
            statements_executed: total_statements,
            journals_created: total_journals,
        },
    }))
}
