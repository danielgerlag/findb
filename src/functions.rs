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
        let account_id = match args.first() {
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

        let result = self.storage.get_balance(account_id, *effective_date, dimension)?;

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
        let account_id = match args.first() {
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

        let result = self.storage.get_statement(account_id, Bound::Included(*from), Bound::Included(*to), dimension)?;

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
        let effective_date = match args.first() {
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
        let from = match args.first() {
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

/// convert(amount, rate_name, date) — Converts an amount using an FX rate.
/// Example: convert(1000, usd_eur, 2023-07-01) multiplies 1000 by the usd_eur rate.
pub struct Convert {
    storage: Arc<dyn StorageBackend>,
}

impl Convert {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }
}

impl ScalarFunction for Convert {
    fn call(&self, _context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        let amount = match args.first() {
            Some(DataValue::Money(m)) => *m,
            Some(DataValue::Int(i)) => Decimal::from(*i),
            _ => return Err(EvaluationError::InvalidArgument("amount".to_string())),
        };

        let rate_id = match args.get(1) {
            Some(DataValue::String(s)) => s.clone(),
            Some(DataValue::AccountId(s)) => s.clone(),
            _ => return Err(EvaluationError::InvalidArgument("rate_name".to_string())),
        };

        let date = match args.get(2) {
            Some(DataValue::Date(d)) => *d,
            _ => return Err(EvaluationError::InvalidArgument("date".to_string())),
        };

        let rate = self.storage.get_rate(&rate_id, date)?;
        Ok(DataValue::Money(amount * rate))
    }
}

/// fx_rate(rate_name, date) — Returns the FX rate value at a given date.
pub struct FxRate {
    storage: Arc<dyn StorageBackend>,
}

impl FxRate {
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }
}

impl ScalarFunction for FxRate {
    fn call(&self, _context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        let rate_id = match args.first() {
            Some(DataValue::String(s)) => s.clone(),
            Some(DataValue::AccountId(s)) => s.clone(),
            _ => return Err(EvaluationError::InvalidArgument("rate_name".to_string())),
        };

        let date = match args.get(1) {
            Some(DataValue::Date(d)) => *d,
            _ => return Err(EvaluationError::InvalidArgument("date".to_string())),
        };

        let rate = self.storage.get_rate(&rate_id, date)?;
        Ok(DataValue::Money(rate))
    }
}

/// round(value, decimal_places) — Rounds a monetary value to N decimal places.
pub struct Round;

impl ScalarFunction for Round {
    fn call(&self, _context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        let value = match args.first() {
            Some(DataValue::Money(m)) => *m,
            Some(DataValue::Int(i)) => Decimal::from(*i),
            _ => return Err(EvaluationError::InvalidArgument("value".to_string())),
        };

        let places = match args.get(1) {
            Some(DataValue::Int(n)) => *n as u32,
            None => 2, // default to 2 decimal places
            _ => return Err(EvaluationError::InvalidArgument("decimal_places".to_string())),
        };

        Ok(DataValue::Money(value.round_dp(places)))
    }
}

/// abs(value) — Returns absolute value.
pub struct Abs;

impl ScalarFunction for Abs {
    fn call(&self, _context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        match args.first() {
            Some(DataValue::Money(m)) => Ok(DataValue::Money(m.abs())),
            Some(DataValue::Int(i)) => Ok(DataValue::Int(i.abs())),
            _ => Err(EvaluationError::InvalidArgument("value".to_string())),
        }
    }
}

/// min(a, b) — Returns the smaller of two values.
pub struct Min;

impl ScalarFunction for Min {
    fn call(&self, _context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        let a = match args.first() {
            Some(DataValue::Money(m)) => *m,
            Some(DataValue::Int(i)) => Decimal::from(*i),
            _ => return Err(EvaluationError::InvalidArgument("a".to_string())),
        };
        let b = match args.get(1) {
            Some(DataValue::Money(m)) => *m,
            Some(DataValue::Int(i)) => Decimal::from(*i),
            _ => return Err(EvaluationError::InvalidArgument("b".to_string())),
        };
        Ok(DataValue::Money(a.min(b)))
    }
}

/// max(a, b) — Returns the larger of two values.
pub struct Max;

impl ScalarFunction for Max {
    fn call(&self, _context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError> {
        let a = match args.first() {
            Some(DataValue::Money(m)) => *m,
            Some(DataValue::Int(i)) => Decimal::from(*i),
            _ => return Err(EvaluationError::InvalidArgument("a".to_string())),
        };
        let b = match args.get(1) {
            Some(DataValue::Money(m)) => *m,
            Some(DataValue::Int(i)) => Decimal::from(*i),
            _ => return Err(EvaluationError::InvalidArgument("b".to_string())),
        };
        Ok(DataValue::Money(a.max(b)))
    }
}