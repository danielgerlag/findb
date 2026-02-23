use std::{sync::Arc, ops::Bound};

use crate::{function_registry::ScalarFunction, models::{DataValue, TrialBalanceItem}, evaluator::{ExpressionEvaluationContext, EvaluationError}, storage::StorageBackend};



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