use std::sync::Arc;

use ordered_float::OrderedFloat;

use crate::{function_registry::ScalarFunction, models::DataValue, evaluator::{ExpressionEvaluationContext, EvaluationError}, storage::Storage};



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
            Some(DataValue::Dimension(dimension)) => dimension,
            _ => return Err(EvaluationError::InvalidArgument("dimension".to_string())),
        };

        let result = self.storage.get_balance(&account_id, *effective_date, &(dimension.0.clone(), dimension.1.clone()));

        Ok(DataValue::Money(OrderedFloat::from(result)))
    }
}