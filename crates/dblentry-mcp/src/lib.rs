use std::sync::Arc;

use dblentry_core::storage::StorageBackend;
use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{tool, tool_handler, tool_router, ServerHandler, ServiceExt};
use schemars::JsonSchema;
use serde::Deserialize;

const FQL_REFERENCE: &str = include_str!("../../../docs/ai/fql-reference.md");

/// Result of executing a single FQL statement.
#[derive(Debug)]
pub struct FqlResult {
    pub output: String,
    pub journals_created: usize,
}

/// Trait for executing FQL queries. Implemented by the main dblentry crate
/// to avoid a cyclic dependency.
pub trait FqlEngine: Send + Sync {
    fn execute_fql(&self, fql: &str, entity: Option<&str>) -> Result<Vec<FqlResult>, String>;
}

#[derive(Clone)]
pub struct DblEntryMcp {
    engine: Arc<dyn FqlEngine>,
    storage: Arc<dyn StorageBackend>,
    tool_router: ToolRouter<Self>,
}

impl DblEntryMcp {
    pub fn new(engine: Arc<dyn FqlEngine>, storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            engine,
            storage,
            tool_router: Self::tool_router(),
        }
    }
}

#[derive(Deserialize, JsonSchema)]
struct ExecuteFqlInput {
    /// FQL statements to execute. Multiple statements can be separated by semicolons.
    fql: String,
    /// Optional entity to execute against (defaults to "default")
    #[serde(default)]
    entity: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct GetBalanceInput {
    /// Account ID (without the @ prefix)
    account: String,
    /// Date in YYYY-MM-DD format
    date: String,
    /// Optional entity (defaults to "default")
    #[serde(default)]
    entity: Option<String>,
    /// Optional dimension filter in format "Key=Value"
    #[serde(default)]
    dimension: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct GetStatementInput {
    /// Account ID (without the @ prefix)
    account: String,
    /// Start date in YYYY-MM-DD format
    from: String,
    /// End date in YYYY-MM-DD format
    to: String,
    /// Optional entity
    #[serde(default)]
    entity: Option<String>,
    /// Optional dimension filter "Key=Value"
    #[serde(default)]
    dimension: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct GetTrialBalanceInput {
    /// Date in YYYY-MM-DD format
    date: String,
    /// Optional entity
    #[serde(default)]
    entity: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct GetIncomeStatementInput {
    /// Start date in YYYY-MM-DD format
    from: String,
    /// End date in YYYY-MM-DD format
    to: String,
    /// Optional entity
    #[serde(default)]
    entity: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct EntityQuery {
    /// Entity ID to query
    entity: String,
}

impl DblEntryMcp {
    fn run_fql_sync(
        &self,
        fql: &str,
        entity: Option<&str>,
    ) -> Result<CallToolResult, ErrorData> {
        match self.engine.execute_fql(fql, entity) {
            Ok(results) => {
                let mut output = String::new();
                let mut total_journals = 0;
                for r in &results {
                    total_journals += r.journals_created;
                    if !r.output.trim().is_empty() {
                        output.push_str(&r.output);
                        output.push('\n');
                    }
                }
                if output.is_empty() {
                    output = format!(
                        "OK. {} statement(s) executed, {} journal(s) created.",
                        results.len(),
                        total_journals
                    );
                }
                Ok(CallToolResult::success(vec![Content::text(output)]))
            }
            Err(e) => Ok(CallToolResult::error(vec![Content::text(e)])),
        }
    }
}

#[tool_router]
impl DblEntryMcp {
    #[tool(description = "Execute one or more FQL (Financial Query Language) statements against the DblEntry ledger. Use this for creating accounts, recording journals, querying balances, and all other bookkeeping operations. Separate multiple statements with semicolons.")]
    fn execute_fql(
        &self,
        Parameters(input): Parameters<ExecuteFqlInput>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_fql_sync(&input.fql, input.entity.as_deref())
    }

    #[tool(description = "Get the balance of an account at a specific date.")]
    fn get_balance(
        &self,
        Parameters(input): Parameters<GetBalanceInput>,
    ) -> Result<CallToolResult, ErrorData> {
        let dim = input
            .dimension
            .as_deref()
            .map(|d| format!(", {}", d))
            .unwrap_or_default();
        let fql = format!(
            "GET balance(@{}, {}{}) AS result",
            input.account, input.date, dim
        );
        self.run_fql_sync(&fql, input.entity.as_deref())
    }

    #[tool(description = "Get transaction statement for an account over a date range.")]
    fn get_statement(
        &self,
        Parameters(input): Parameters<GetStatementInput>,
    ) -> Result<CallToolResult, ErrorData> {
        let dim = input
            .dimension
            .as_deref()
            .map(|d| format!(", {}", d))
            .unwrap_or_default();
        let fql = format!(
            "GET statement(@{}, {}, {}{}) AS result",
            input.account, input.from, input.to, dim
        );
        self.run_fql_sync(&fql, input.entity.as_deref())
    }

    #[tool(description = "Get trial balance (all account balances) at a specific date.")]
    fn get_trial_balance(
        &self,
        Parameters(input): Parameters<GetTrialBalanceInput>,
    ) -> Result<CallToolResult, ErrorData> {
        let fql = format!("GET trial_balance({}) AS result", input.date);
        self.run_fql_sync(&fql, input.entity.as_deref())
    }

    #[tool(description = "Get income statement (revenue and expenses) for a date range.")]
    fn get_income_statement(
        &self,
        Parameters(input): Parameters<GetIncomeStatementInput>,
    ) -> Result<CallToolResult, ErrorData> {
        let fql = format!(
            "GET income_statement({}, {}) AS result",
            input.from, input.to
        );
        self.run_fql_sync(&fql, input.entity.as_deref())
    }

    #[tool(description = "Get the FQL language reference documentation. Use this to learn FQL syntax before writing queries.")]
    fn get_fql_spec(&self) -> String {
        FQL_REFERENCE.to_string()
    }

    #[tool(description = "List all entities in the system.")]
    fn list_entities(&self) -> String {
        let entities: Vec<String> = self
            .storage
            .list_entities()
            .iter()
            .map(|e| e.to_string())
            .collect();
        serde_json::to_string_pretty(&entities).unwrap()
    }

    #[tool(description = "Get schema information for an entity: its accounts (with types) and defined rates.")]
    fn get_schema(
        &self,
        Parameters(input): Parameters<EntityQuery>,
    ) -> Result<CallToolResult, ErrorData> {
        if !self.storage.entity_exists(&input.entity) {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Entity not found: {}",
                input.entity
            ))]));
        }
        let accounts: Vec<serde_json::Value> = self
            .storage
            .list_accounts(&input.entity)
            .iter()
            .map(|(id, acct_type)| {
                let id_str = id.to_string();
                serde_json::json!({
                    "id": id_str,
                    "type": format!("{:?}", acct_type).to_lowercase(),
                    "is_unit_account": self.storage.is_unit_account(&input.entity, &id_str),
                    "unit_rate_id": self.storage.get_unit_rate_id(&input.entity, &id_str).map(|r| r.to_string()),
                })
            })
            .collect();
        let rates: Vec<String> = self
            .storage
            .list_rates(&input.entity)
            .iter()
            .map(|r| r.to_string())
            .collect();
        let schema = serde_json::json!({
            "entity": input.entity,
            "accounts": accounts,
            "rates": rates,
        });
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&schema).unwrap(),
        )]))
    }
}

#[tool_handler]
impl ServerHandler for DblEntryMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(
                "DblEntry is a Layer 2 database for double-entry bookkeeping. \
                 Use the get_fql_spec tool first to learn the FQL query language, \
                 then use execute_fql or the convenience tools to interact with the ledger.",
            )
    }
}

/// Run DblEntry as an MCP server over stdio.
pub async fn run_mcp_stdio(
    engine: Arc<dyn FqlEngine>,
    storage: Arc<dyn StorageBackend>,
) -> Result<(), Box<dyn std::error::Error>> {
    let server = DblEntryMcp::new(engine, storage);
    let service = server.serve(rmcp::transport::io::stdio()).await?;
    service.waiting().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockEngine;

    impl FqlEngine for MockEngine {
        fn execute_fql(&self, fql: &str, _entity: Option<&str>) -> Result<Vec<FqlResult>, String> {
            if fql.contains("ERROR") {
                return Err("Test error".to_string());
            }
            Ok(vec![FqlResult {
                output: format!("Executed: {}", fql),
                journals_created: 1,
            }])
        }
    }

    #[test]
    fn test_fql_engine_success() {
        let engine = MockEngine;
        let result = engine.execute_fql("CREATE ACCOUNT @bank ASSET", None);
        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].journals_created, 1);
        assert!(results[0].output.contains("CREATE ACCOUNT"));
    }

    #[test]
    fn test_fql_engine_error() {
        let engine = MockEngine;
        let result = engine.execute_fql("ERROR", None);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Test error");
    }

    #[test]
    fn test_fql_engine_with_entity() {
        let engine = MockEngine;
        let result = engine.execute_fql("GET balance(@bank, 2024-01-01) AS b", Some("corp"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_fql_reference_included() {
        assert!(!FQL_REFERENCE.is_empty());
        assert!(FQL_REFERENCE.contains("CREATE ACCOUNT"));
    }

    #[test]
    fn test_fql_result_fields() {
        let result = FqlResult {
            output: "test output".to_string(),
            journals_created: 5,
        };
        assert_eq!(result.output, "test output");
        assert_eq!(result.journals_created, 5);
    }
}
