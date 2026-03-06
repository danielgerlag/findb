use std::sync::Arc;

use axum::{
    extract::Path,
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use serde::Serialize;

use crate::function_registry::FunctionRegistry;
use crate::storage::StorageBackend;

#[derive(Clone)]
pub struct SchemaState {
    pub storage: Arc<dyn StorageBackend>,
    pub function_registry: Arc<FunctionRegistry>,
}

#[derive(Serialize)]
pub struct SchemaResponse {
    pub entities: Vec<String>,
    pub functions: Vec<FunctionInfo>,
}

#[derive(Serialize)]
pub struct EntitySchemaResponse {
    pub entity_id: String,
    pub accounts: Vec<AccountInfo>,
    pub rates: Vec<String>,
}

#[derive(Serialize)]
pub struct AccountInfo {
    pub id: String,
    pub account_type: String,
    pub is_unit_account: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit_rate_id: Option<String>,
}

#[derive(Serialize)]
pub struct FunctionInfo {
    pub name: String,
    pub signature: String,
    pub description: String,
}

fn get_function_info(name: &str) -> FunctionInfo {
    let (signature, description) = match name {
        "balance" => ("balance(@account, date, [dimension])", "Get account balance at a date"),
        "statement" => ("statement(@account, from, to, [dimension])", "Get transaction statement for a period"),
        "trial_balance" => ("trial_balance(date)", "Get all account balances at a date"),
        "income_statement" => ("income_statement(from, to)", "Get income and expense totals for a period"),
        "account_count" => ("account_count()", "Get total number of accounts"),
        "convert" => ("convert(amount, rate_id, date)", "Convert amount using a rate at a date"),
        "fx_rate" => ("fx_rate(rate_id, date)", "Get rate value at a date"),
        "round" => ("round(value, [decimal_places])", "Round to N decimal places (default 2)"),
        "abs" => ("abs(value)", "Absolute value"),
        "min" => ("min(a, b)", "Minimum of two values"),
        "max" => ("max(a, b)", "Maximum of two values"),
        "units" => ("units(@account, date, [dimension])", "Get total units held in account"),
        "market_value" => ("market_value(@account, date, [dimension])", "Get market value (units × rate)"),
        "unrealized_gain" => ("unrealized_gain(@account, date, [dimension])", "Get unrealized gain (market value - cost basis)"),
        "cost_basis" => ("cost_basis(@account, date, [dimension])", "Get weighted average cost per unit"),
        "lots" => ("lots(@account, date, [dimension])", "Get individual lot positions"),
        _ => (name, "Custom function"),
    };
    FunctionInfo {
        name: name.to_string(),
        signature: signature.to_string(),
        description: description.to_string(),
    }
}

pub async fn schema_overview(
    Extension(schema_state): Extension<SchemaState>,
) -> impl IntoResponse {
    let entities: Vec<String> = schema_state
        .storage
        .list_entities()
        .iter()
        .map(|e| e.to_string())
        .collect();

    let functions: Vec<FunctionInfo> = schema_state
        .function_registry
        .list_functions()
        .iter()
        .map(|name| get_function_info(name))
        .collect();

    Json(SchemaResponse {
        entities,
        functions,
    })
}

pub async fn schema_entity(
    Extension(schema_state): Extension<SchemaState>,
    Path(entity_id): Path<String>,
) -> impl IntoResponse {
    if !schema_state.storage.entity_exists(&entity_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "error": format!("Entity not found: {}", entity_id)
            })),
        )
            .into_response();
    }

    let accounts: Vec<AccountInfo> = schema_state
        .storage
        .list_accounts(&entity_id)
        .iter()
        .map(|(id, account_type)| {
            let id_str = id.to_string();
            let is_unit = schema_state.storage.is_unit_account(&entity_id, &id_str);
            let unit_rate_id = schema_state
                .storage
                .get_unit_rate_id(&entity_id, &id_str)
                .map(|r| r.to_string());
            AccountInfo {
                id: id_str,
                account_type: format!("{:?}", account_type).to_lowercase(),
                is_unit_account: is_unit,
                unit_rate_id,
            }
        })
        .collect();

    let rates: Vec<String> = schema_state
        .storage
        .list_rates(&entity_id)
        .iter()
        .map(|r| r.to_string())
        .collect();

    Json(serde_json::json!(EntitySchemaResponse {
        entity_id,
        accounts,
        rates,
    }))
    .into_response()
}
