use std::{sync::Arc, ops::Bound};

use rust_decimal::Decimal;

use crate::{ast::AccountType, function_registry::ScalarFunction, models::{DataValue, TrialBalanceItem}, evaluator::{ExpressionEvaluationContext, EvaluationError}, storage::StorageBackend};



pub struct Balance {
    storage: Arc<dyn StorageBackend>,
}

impl Balance {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            storage,
        }
    }
}


impl ScalarFunction for Balance {
    fn call(&self, _context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        let account_id = match args.get(0) {
            Some(DataValue::AccountId(id)) => id,
            _ => return Err(EvaluationError::InvalidArgument("account_id".to_string())),
        };

        let effective_date = match args.get(1) {
            Some(DataValue::Date(date)) => date,
            _ => return Err(EvaluationError::InvalidArgument("effective_date".to_string())),
        };

        let dimension = match args.get(2) {
            Some(DataValue::Dimension(dimension)) => Some(dimension),
            None => None,
            _ => return Err(EvaluationError::InvalidArgument("dimension".to_string())),
        };

        let result = self.storage.get_balance(&account_id, *effective_date, dimension)?;

        Ok(DataValue::Money(result))
    }
}


pub struct Statement {
    storage: Arc<dyn StorageBackend>,
}

impl Statement {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            storage,
        }
    }
}


impl ScalarFunction for Statement {
    fn call(&self, _context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        let account_id = match args.get(0) {
            Some(DataValue::AccountId(id)) => id,
            _ => return Err(EvaluationError::InvalidArgument("account_id".to_string())),
        };

        let from = match args.get(1) {
            Some(DataValue::Date(date)) => date,
            _ => return Err(EvaluationError::InvalidArgument("from".to_string())),
        };

        let to = match args.get(2) {
            Some(DataValue::Date(date)) => date,
            _ => return Err(EvaluationError::InvalidArgument("to".to_string())),
        };

        let dimension = match args.get(3) {
            Some(DataValue::Dimension(dimension)) => Some(dimension),
            None => None,
            _ => return Err(EvaluationError::InvalidArgument("dimension".to_string())),
        };

        let result = self.storage.get_statement(&account_id, Bound::Included(*from), Bound::Included(*to), dimension)?;

        Ok(result)
    }
}

pub struct TrialBalance {
    storage: Arc<dyn StorageBackend>,
}

impl TrialBalance {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            storage,
        }
    }
}

impl ScalarFunction for TrialBalance {
    fn call(&self, _context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        let effective_date = match args.get(0) {
            Some(DataValue::Date(dt)) => dt,
            _ => return Err(EvaluationError::InvalidArgument("date".to_string())),
        };

        let accounts = self.storage.list_accounts();
        let mut result = Vec::new();
        for (account_id, account_type) in accounts {
            let balance = self.storage.get_balance(&account_id, *effective_date, None)?;
            result.push(TrialBalanceItem {
                account_id,
                account_type,
                balance,
            });
        }

        Ok(DataValue::TrialBalance(result))
    }
}

/// income_statement(from_date, to_date) — Returns net income/expense for the period.
/// Calculates change in income and expense account balances between two dates.
pub struct IncomeStatement {
    storage: Arc<dyn StorageBackend>,
}

impl IncomeStatement {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }
}

impl ScalarFunction for IncomeStatement {
    fn call(&self, _context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        let from = match args.get(0) {
            Some(DataValue::Date(dt)) => *dt,
            _ => return Err(EvaluationError::InvalidArgument("from_date".to_string())),
        };

        let to = match args.get(1) {
            Some(DataValue::Date(dt)) => *dt,
            _ => return Err(EvaluationError::InvalidArgument("to_date".to_string())),
        };

        let accounts = self.storage.list_accounts();
        let mut total_income = Decimal::ZERO;
        let mut total_expenses = Decimal::ZERO;
        let mut items = Vec::new();

        for (account_id, account_type) in &accounts {
            match account_type {
                AccountType::Income | AccountType::Expense => {
                    let bal_from = self.storage.get_balance(account_id, from, None)?;
                    let bal_to = self.storage.get_balance(account_id, to, None)?;
                    let change = bal_to - bal_from;
                    if change != Decimal::ZERO {
                        items.push(TrialBalanceItem {
                            account_id: account_id.clone(),
                            account_type: account_type.clone(),
                            balance: change,
                        });
                        match account_type {
                            AccountType::Income => total_income += change,
                            AccountType::Expense => total_expenses += change,
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        // Net income = income - expenses
        items.push(TrialBalanceItem {
            account_id: "NET_INCOME".into(),
            account_type: AccountType::Income,
            balance: total_income - total_expenses,
        });

        Ok(DataValue::TrialBalance(items))
    }
}

/// account_count() — Returns the number of accounts.
pub struct AccountCount {
    storage: Arc<dyn StorageBackend>,
}

impl AccountCount {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }
}

impl ScalarFunction for AccountCount {
    fn call(&self, _context: &ExpressionEvaluationContext, _args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        let count = self.storage.list_accounts().len();
        Ok(DataValue::Int(count as i64))
    }
}