use std::{sync::Arc, ops::Bound};

use ordered_float::OrderedFloat;

use crate::{function_registry::ScalarFunction, models::{DataValue, BalanceSheetItem}, evaluator::{ExpressionEvaluationContext, EvaluationError}, storage::Storage};



pub struct Balance {
    storage: Arc<Storage>,
}

impl Balance {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
        }
    }
}


impl ScalarFunction for Balance {
    fn call(&self, context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
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

        let result = self.storage.get_balance(&account_id, *effective_date, dimension);

        Ok(DataValue::Money(OrderedFloat::from(result)))
    }
}


pub struct Statement {
    storage: Arc<Storage>,
}

impl Statement {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
        }
    }
}


impl ScalarFunction for Statement {
    fn call(&self, context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
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

        let result = self.storage.get_statement(&account_id, Bound::Included(*from), Bound::Included(*to), dimension);

        Ok(DataValue::List(result))
    }
}

pub struct BalanceSheet {
    storage: Arc<Storage>,
}

impl BalanceSheet {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
        }
    }
}

impl ScalarFunction for BalanceSheet {
    fn call(&self, context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        let effective_date = match args.get(0) {
            Some(DataValue::Date(dt)) => dt,
            _ => return Err(EvaluationError::InvalidArgument("date".to_string())),
        };

        let accounts = self.storage.list_accounts();
        let mut result = Vec::new();
        for (account_id, account_type) in accounts {
            let balance = self.storage.get_balance(&account_id, *effective_date, None);
            result.push(DataValue::BalanceSheetItem(BalanceSheetItem {
                account_id,
                account_type,
                balance: OrderedFloat::from(balance),
            }));
        }

        Ok(DataValue::List(result))
    }
}