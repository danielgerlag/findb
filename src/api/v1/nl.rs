use axum::{
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

#[cfg(feature = "nl")]
use std::sync::Arc;
#[cfg(feature = "nl")]
use axum::{extract::State, Extension};
#[cfg(feature = "nl")]
use crate::{
    display::format_execution_result,
    config::NlConfig,
    evaluator::QueryVariables,
    lexer,
    statement_executor::{ExecutionContext, StatementExecutor},
};
#[cfg(feature = "nl")]
use super::schema::SchemaState;

#[cfg(feature = "nl")]
const FQL_REFERENCE: &str = include_str!("../../../docs/ai/fql-reference.md");

#[derive(Deserialize)]
pub struct NlRequest {
    /// Natural language description of the accounting operation
    pub prompt: String,
    /// If true, only generate FQL without executing it
    #[serde(default)]
    pub dry_run: bool,
    /// Optional entity to operate on
    #[serde(default)]
    pub entity: Option<String>,
}

#[derive(Serialize)]
pub struct NlResponse {
    pub success: bool,
    /// The generated FQL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fql: Option<String>,
    /// Execution result (if not dry_run)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    /// Brief explanation of what was done
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explanation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct NlState {
    pub config: crate::config::NlConfig,
}

#[cfg(feature = "nl")]
fn build_system_prompt(schema_state: &SchemaState, entity: Option<&str>) -> String {
    let entity_id = entity.unwrap_or("default");

    let mut schema_info = String::new();
    if schema_state.storage.entity_exists(entity_id) {
        let accounts = schema_state.storage.list_accounts(entity_id);
        if !accounts.is_empty() {
            schema_info.push_str("\n## Current Accounts\n");
            for (id, acct_type) in &accounts {
                schema_info.push_str(&format!(
                    "- @{} ({})\n",
                    id,
                    format!("{:?}", acct_type).to_lowercase()
                ));
            }
        }
        let rates = schema_state.storage.list_rates(entity_id);
        if !rates.is_empty() {
            schema_info.push_str("\n## Current Rates\n");
            for r in &rates {
                schema_info.push_str(&format!("- {}\n", r));
            }
        }
        let entities = schema_state.storage.list_entities();
        if entities.len() > 1 {
            schema_info.push_str("\n## Entities\n");
            for e in &entities {
                schema_info.push_str(&format!("- {}\n", e));
            }
        }
    }

    format!(
        r#"You are an FQL (Financial Query Language) code generator for DblEntry, a double-entry bookkeeping database.

Your job is to translate natural language accounting instructions into valid FQL code.

RULES:
1. Output ONLY valid FQL code - no explanations, no markdown, no comments
2. Use only existing accounts when possible. Create new accounts if they don't exist yet.
3. All monetary values must balance in journal entries (debits = credits)
4. Use SET DATE before journal entries when a specific date is mentioned
5. Use dimension tags (FOR Key='Value') when the user mentions categories, departments, regions, etc.
6. Always use single quotes for string literals in FQL

{fql_reference}
{schema_info}"#,
        fql_reference = FQL_REFERENCE,
        schema_info = schema_info,
    )
}

#[cfg(feature = "nl")]
pub async fn nl_handler(
    State(exec): State<Arc<StatementExecutor>>,
    Extension(nl_state): Extension<NlState>,
    Extension(schema_state): Extension<SchemaState>,
    Json(req): Json<NlRequest>,
) -> impl IntoResponse {
    if !nl_state.config.enabled {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(NlResponse {
                success: false,
                fql: None,
                result: None,
                explanation: None,
                error: Some(
                    "Natural language endpoint is not enabled. Set [nl] enabled = true in config."
                        .to_string(),
                ),
            }),
        );
    }

    if nl_state.config.api_key.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(NlResponse {
                success: false,
                fql: None,
                result: None,
                explanation: None,
                error: Some("No API key configured for NL provider.".to_string()),
            }),
        );
    }

    let system_prompt = build_system_prompt(&schema_state, req.entity.as_deref());

    let fql = match call_llm(&nl_state.config, &system_prompt, &req.prompt).await {
        Ok(fql) => fql,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(NlResponse {
                    success: false,
                    fql: None,
                    result: None,
                    explanation: None,
                    error: Some(format!("LLM error: {}", e)),
                }),
            );
        }
    };

    // Clean up the FQL (remove markdown code fences if present)
    let clean_fql = fql
        .trim()
        .strip_prefix("```fql")
        .or_else(|| fql.trim().strip_prefix("```"))
        .unwrap_or(fql.trim())
        .strip_suffix("```")
        .unwrap_or(fql.trim())
        .trim()
        .to_string();

    if req.dry_run {
        match lexer::parse(&clean_fql) {
            Ok(_) => (
                StatusCode::OK,
                Json(NlResponse {
                    success: true,
                    fql: Some(clean_fql),
                    result: None,
                    explanation: Some(
                        "FQL generated successfully (dry run — not executed).".to_string(),
                    ),
                    error: None,
                }),
            ),
            Err(e) => (
                StatusCode::OK,
                Json(NlResponse {
                    success: false,
                    fql: Some(clean_fql),
                    result: None,
                    explanation: None,
                    error: Some(format!("Generated FQL has parse error: {}", e)),
                }),
            ),
        }
    } else {
        match lexer::parse(&clean_fql) {
            Ok(statements) => {
                let eff_date = time::OffsetDateTime::now_utc().date();
                let mut context = ExecutionContext::new(eff_date, QueryVariables::new());
                if let Some(ref entity) = req.entity {
                    context.entity_id = Arc::from(entity.as_str());
                }

                match exec.execute_script(&mut context, &statements) {
                    Ok(results) => {
                        let mut output = String::new();
                        let mut total_journals = 0;
                        for r in &results {
                            total_journals += r.journals_created;
                            let s = format_execution_result(r);
                            if !s.trim().is_empty() {
                                output.push_str(&s);
                                output.push('\n');
                            }
                        }

                        let explanation = format!(
                            "Executed {} statement(s), created {} journal(s).",
                            results.len(),
                            total_journals
                        );

                        (
                            StatusCode::OK,
                            Json(NlResponse {
                                success: true,
                                fql: Some(clean_fql),
                                result: if output.is_empty() {
                                    None
                                } else {
                                    Some(output)
                                },
                                explanation: Some(explanation),
                                error: None,
                            }),
                        )
                    }
                    Err(e) => (
                        StatusCode::OK,
                        Json(NlResponse {
                            success: false,
                            fql: Some(clean_fql),
                            result: None,
                            explanation: None,
                            error: Some(format!("Execution error: {}", e)),
                        }),
                    ),
                }
            }
            Err(e) => (
                StatusCode::OK,
                Json(NlResponse {
                    success: false,
                    fql: Some(clean_fql),
                    result: None,
                    explanation: None,
                    error: Some(format!("Generated FQL has parse error: {}", e)),
                }),
            ),
        }
    }
}

#[cfg(feature = "nl")]
async fn call_llm(
    config: &NlConfig,
    system_prompt: &str,
    user_prompt: &str,
) -> Result<String, String> {
    let client = reqwest::Client::new();

    match config.provider.as_str() {
        "openai" | "azure_openai" => {
            let url = if config.provider == "azure_openai" && !config.api_base.is_empty() {
                format!(
                    "{}/openai/deployments/{}/chat/completions?api-version=2024-02-01",
                    config.api_base, config.model
                )
            } else {
                "https://api.openai.com/v1/chat/completions".to_string()
            };

            let mut req = client
                .post(&url)
                .header("Content-Type", "application/json");

            if config.provider == "azure_openai" {
                req = req.header("api-key", &config.api_key);
            } else {
                req = req.header("Authorization", format!("Bearer {}", config.api_key));
            }

            let body = serde_json::json!({
                "model": config.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.1
            });

            let resp = req
                .json(&body)
                .send()
                .await
                .map_err(|e| format!("HTTP error: {}", e))?;
            let status = resp.status();
            let text = resp
                .text()
                .await
                .map_err(|e| format!("Response read error: {}", e))?;

            if !status.is_success() {
                tracing::error!("LLM API error ({}): {}", status, text);
                return Err(format!("LLM provider returned HTTP {}", status.as_u16()));
            }

            let json: serde_json::Value =
                serde_json::from_str(&text).map_err(|e| format!("JSON parse error: {}", e))?;
            json["choices"][0]["message"]["content"]
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| "No content in LLM response".to_string())
        }
        "anthropic" => {
            let url = "https://api.anthropic.com/v1/messages";
            let body = serde_json::json!({
                "model": config.model,
                "max_tokens": 4096,
                "system": system_prompt,
                "messages": [
                    {"role": "user", "content": user_prompt}
                ]
            });

            let resp = client
                .post(url)
                .header("Content-Type", "application/json")
                .header("x-api-key", &config.api_key)
                .header("anthropic-version", "2023-06-01")
                .json(&body)
                .send()
                .await
                .map_err(|e| format!("HTTP error: {}", e))?;

            let status = resp.status();
            let text = resp
                .text()
                .await
                .map_err(|e| format!("Response read error: {}", e))?;

            if !status.is_success() {
                tracing::error!("LLM API error ({}): {}", status, text);
                return Err(format!("LLM provider returned HTTP {}", status.as_u16()));
            }

            let json: serde_json::Value =
                serde_json::from_str(&text).map_err(|e| format!("JSON parse error: {}", e))?;
            json["content"][0]["text"]
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| "No content in LLM response".to_string())
        }
        _ => Err(format!(
            "Unsupported NL provider: {}. Use 'openai', 'anthropic', or 'azure_openai'.",
            config.provider
        )),
    }
}

#[cfg(not(feature = "nl"))]
pub async fn nl_handler(Json(_req): Json<NlRequest>) -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(NlResponse {
            success: false,
            fql: None,
            result: None,
            explanation: None,
            error: Some(
                "Natural language endpoint requires the 'nl' feature. Rebuild with: cargo build --features nl"
                    .to_string(),
            ),
        }),
    )
}
