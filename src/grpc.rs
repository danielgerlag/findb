use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::{
    evaluator::QueryVariables,
    lexer,
    models::DataValue,
    statement_executor::{ExecutionContext, StatementExecutor},
};

pub mod pb {
    tonic::include_proto!("findb.v1");
}

use pb::finance_db_server::FinanceDb;

/// Escape a string value for safe interpolation into FQL single-quoted literals.
fn escape_fql(s: &str) -> String {
    s.replace('\'', "''")
}

/// Validate that a value contains only safe identifier characters.
fn is_safe_identifier(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-')
}

#[allow(clippy::result_large_err)]
fn validate_identifier(s: &str, field: &str) -> Result<(), Status> {
    if !is_safe_identifier(s) {
        return Err(Status::invalid_argument(format!("Invalid {}: must be alphanumeric", field)));
    }
    Ok(())
}

pub struct FinanceDbService {
    executor: Arc<StatementExecutor>,
}

impl FinanceDbService {
    pub fn new(executor: Arc<StatementExecutor>) -> Self {
        Self { executor }
    }

    #[allow(clippy::result_large_err)]
    fn execute_fql(&self, fql: &str) -> Result<Vec<crate::statement_executor::ExecutionResult>, Status> {
        let statements = lexer::parse(fql)
            .map_err(|e| Status::invalid_argument(format!("Parse error: {}", e)))?;

        let eff_date = time::OffsetDateTime::now_utc().date();
        let mut context = ExecutionContext::new(eff_date, QueryVariables::new());

        self.executor
            .execute_script(&mut context, &statements)
            .map_err(|e| Status::internal(format!("{}", e)))
    }
}

#[tonic::async_trait]
impl FinanceDb for FinanceDbService {
    async fn execute_fql(
        &self,
        request: Request<pb::ExecuteFqlRequest>,
    ) -> Result<Response<pb::ExecuteFqlResponse>, Status> {
        let query = &request.into_inner().query;

        let statements = match lexer::parse(query) {
            Ok(s) => s,
            Err(e) => {
                return Ok(Response::new(pb::ExecuteFqlResponse {
                    success: false,
                    results: vec![],
                    error: format!("Parse error: {}", e),
                    statements_executed: 0,
                    journals_created: 0,
                }));
            }
        };

        let eff_date = time::OffsetDateTime::now_utc().date();
        let mut context = ExecutionContext::new(eff_date, QueryVariables::new());

        match self.executor.execute_script(&mut context, &statements) {
            Ok(script_results) => {
                let mut results = Vec::new();
                let mut total_journals = 0i32;
                for result in &script_results {
                    total_journals += result.journals_created as i32;
                    let result_str = result.to_string();
                    if !result_str.trim().is_empty() {
                        results.push(result_str);
                    }
                }
                Ok(Response::new(pb::ExecuteFqlResponse {
                    success: true,
                    results,
                    error: String::new(),
                    statements_executed: script_results.len() as i32,
                    journals_created: total_journals,
                }))
            }
            Err(e) => Ok(Response::new(pb::ExecuteFqlResponse {
                success: false,
                results: vec![],
                error: format!("{}", e),
                statements_executed: 0,
                journals_created: 0,
            })),
        }
    }

    async fn create_account(
        &self,
        request: Request<pb::CreateAccountRequest>,
    ) -> Result<Response<pb::CreateAccountResponse>, Status> {
        let req = request.into_inner();
        validate_identifier(&req.id, "account ID")?;
        validate_identifier(&req.account_type, "account type")?;
        let fql = format!("CREATE ACCOUNT @{} {}", req.id, req.account_type.to_uppercase());
        self.execute_fql(&fql)?;
        Ok(Response::new(pb::CreateAccountResponse { success: true }))
    }

    async fn list_accounts(
        &self,
        _request: Request<pb::ListAccountsRequest>,
    ) -> Result<Response<pb::ListAccountsResponse>, Status> {
        let fql = "GET trial_balance(2099-12-31) AS accounts";
        let results = self.execute_fql(fql)?;

        let mut accounts = Vec::new();
        if let Some(result) = results.last() {
            if let Some(DataValue::TrialBalance(items)) = result.variables.get("accounts") {
                for item in items {
                    accounts.push(pb::AccountInfo {
                        id: item.account_id.to_string(),
                        account_type: format!("{:?}", item.account_type),
                    });
                }
            }
        }

        Ok(Response::new(pb::ListAccountsResponse { accounts }))
    }

    async fn get_balance(
        &self,
        request: Request<pb::GetBalanceRequest>,
    ) -> Result<Response<pb::GetBalanceResponse>, Status> {
        let req = request.into_inner();
        validate_identifier(&req.account_id, "account ID")?;
        let dim = match (&req.dimension_key, &req.dimension_value) {
            (Some(k), Some(v)) => {
                validate_identifier(k, "dimension key")?;
                format!(", {}='{}'", k, escape_fql(v))
            }
            _ => String::new(),
        };
        let fql = format!("GET balance(@{}, {}{}) AS result", req.account_id, req.date, dim);
        let results = self.execute_fql(&fql)?;

        let balance = results
            .last()
            .and_then(|r| r.variables.get("result"))
            .map(|v| match v {
                DataValue::Money(m) => m.to_string(),
                other => format!("{}", other),
            })
            .unwrap_or_else(|| "0".to_string());

        Ok(Response::new(pb::GetBalanceResponse { balance }))
    }

    async fn get_statement(
        &self,
        request: Request<pb::GetStatementRequest>,
    ) -> Result<Response<pb::GetStatementResponse>, Status> {
        let req = request.into_inner();
        validate_identifier(&req.account_id, "account ID")?;
        let dim = match (&req.dimension_key, &req.dimension_value) {
            (Some(k), Some(v)) => {
                validate_identifier(k, "dimension key")?;
                format!(", {}='{}'", k, escape_fql(v))
            }
            _ => String::new(),
        };
        let fql = format!(
            "GET statement(@{}, {}, {}{}) AS result",
            req.account_id, req.from_date, req.to_date, dim
        );
        let results = self.execute_fql(&fql)?;

        let mut transactions = Vec::new();
        if let Some(result) = results.last() {
            if let Some(DataValue::Statement(txns)) = result.variables.get("result") {
                for txn in txns {
                    transactions.push(pb::StatementTransaction {
                        date: format!("{}", txn.date),
                        description: txn.description.to_string(),
                        amount: txn.amount.to_string(),
                        balance: txn.balance.to_string(),
                    });
                }
            }
        }

        Ok(Response::new(pb::GetStatementResponse { transactions }))
    }

    async fn get_trial_balance(
        &self,
        request: Request<pb::GetTrialBalanceRequest>,
    ) -> Result<Response<pb::GetTrialBalanceResponse>, Status> {
        let req = request.into_inner();
        let fql = format!("GET trial_balance({}) AS result", req.date);
        let results = self.execute_fql(fql.as_str())?;

        let mut items = Vec::new();
        if let Some(result) = results.last() {
            if let Some(DataValue::TrialBalance(tb_items)) = result.variables.get("result") {
                for item in tb_items {
                    items.push(pb::TrialBalanceItem {
                        account_id: item.account_id.to_string(),
                        account_type: format!("{:?}", item.account_type),
                        balance: item.balance.to_string(),
                    });
                }
            }
        }

        Ok(Response::new(pb::GetTrialBalanceResponse { items }))
    }

    async fn create_rate(
        &self,
        request: Request<pb::CreateRateRequest>,
    ) -> Result<Response<pb::CreateRateResponse>, Status> {
        let req = request.into_inner();
        validate_identifier(&req.id, "rate ID")?;
        let fql = format!("CREATE RATE {}", req.id);
        self.execute_fql(&fql)?;
        Ok(Response::new(pb::CreateRateResponse { success: true }))
    }

    async fn set_rate(
        &self,
        request: Request<pb::SetRateRequest>,
    ) -> Result<Response<pb::SetRateResponse>, Status> {
        let req = request.into_inner();
        validate_identifier(&req.rate_id, "rate ID")?;
        let fql = format!("SET RATE {} {} {}", req.rate_id, req.value, req.date);
        self.execute_fql(&fql)?;
        Ok(Response::new(pb::SetRateResponse { success: true }))
    }

    async fn create_journal(
        &self,
        request: Request<pb::CreateJournalRequest>,
    ) -> Result<Response<pb::CreateJournalResponse>, Status> {
        let req = request.into_inner();
        for op in &req.operations {
            validate_identifier(&op.account, "account ID")?;
            validate_identifier(&op.op_type, "operation type")?;
        }
        for k in req.dimensions.keys() {
            validate_identifier(k, "dimension key")?;
        }
        let mut fql = format!(
            "CREATE JOURNAL {}, {}, '{}'",
            req.date, req.amount, escape_fql(&req.description)
        );

        if !req.dimensions.is_empty() {
            let dims: Vec<String> = req
                .dimensions
                .iter()
                .map(|(k, v)| format!("{}='{}'", k, escape_fql(v)))
                .collect();
            fql.push_str(&format!(" FOR {}", dims.join(", ")));
        }

        let ops: Vec<String> = req
            .operations
            .iter()
            .map(|op| {
                let mut s = format!("{} @{}", op.op_type.to_uppercase(), op.account);
                if let Some(ref amt) = op.amount {
                    s.push_str(&format!(" {}", amt));
                }
                s
            })
            .collect();
        fql.push_str(&format!(" {}", ops.join(", ")));

        self.execute_fql(&fql)?;
        Ok(Response::new(pb::CreateJournalResponse { success: true }))
    }

    async fn health(
        &self,
        _request: Request<pb::HealthRequest>,
    ) -> Result<Response<pb::HealthResponse>, Status> {
        Ok(Response::new(pb::HealthResponse {
            status: "ok".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }
}
