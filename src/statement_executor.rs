use std::{sync::Arc, collections::{BTreeMap, HashMap}};

use serde::__private::de;
use time::Date;

use crate::{evaluator::{ExpressionEvaluator, QueryVariables, EvaluationError, ExpressionEvaluationContext}, ast::{Statement, JournalExpression, CreateCommand, LedgerOperationData, self, AccountExpression, GetExpression, CreateRateExpression, SetCommand, SetRateExpression, AccrueCommand, Compounding, LedgerOperation}, storage::Storage, models::{write::{CreateJournalCommand, LedgerEntryCommand, CreateRateCommand, SetRateCommand}, DataValue}};

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionContext {
    pub effective_date: Date,
    pub variables: QueryVariables,
}

impl ExecutionContext {
    pub fn new(effective_date: Date, variables: QueryVariables) -> Self {
        Self {
            effective_date,
            variables,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionResult {
    pub variables: QueryVariables,
    pub journals_created: usize,
}

impl ExecutionResult {
    pub fn new() -> Self {
        Self {
            variables: QueryVariables::new(),
            journals_created: 0,
        }
    }
}

impl Into<ExpressionEvaluationContext> for &ExecutionContext {
    fn into(self) -> ExpressionEvaluationContext {
        ExpressionEvaluationContext::new(self.effective_date, self.variables.clone())
    }
}

pub struct StatementExecutor {
    expression_evaluator: Arc<ExpressionEvaluator>,
    storage: Arc<Storage>,
}

impl StatementExecutor {
    pub fn new(expression_evaluator: Arc<ExpressionEvaluator>, storage: Arc<Storage>) -> Self {
        Self {
            expression_evaluator,
            storage,
        }
    }

    pub fn execute(&self, context: &mut ExecutionContext, statement: &Statement) -> Result<ExecutionResult, EvaluationError> {
        Ok(match statement {
            Statement::Create(c) => match c {
                CreateCommand::Account(a) => self.create_account(context, a)?,
                CreateCommand::Journal(j) => self.create_journal(context, j)?,
                CreateCommand::Rate(r) => self.create_rate(context, r)?,
            },
            Statement::Get(get) => self.get(context, get)?,
            Statement::Accrue(accrue) => self.accrue(context, accrue)?,
            Statement::Set(s) => match s {
                SetCommand::Rate(r) => self.set_rate(context, r)?,
            },
        })
    }

    fn create_journal(&self, context: &ExecutionContext, journal: &JournalExpression) -> Result<ExecutionResult, EvaluationError> {
        let mut eval_ctx : ExpressionEvaluationContext = context.into();

        let date = match self.expression_evaluator.evaluate_expression(&eval_ctx, &journal.date)? {
            DataValue::Date(d) => d,
            _ => return Err(EvaluationError::InvalidType),
        };

        eval_ctx.set_effective_date(date);
        
        let journal_amount = match self.expression_evaluator.evaluate_expression(&eval_ctx, &journal.amount)? {
            DataValue::Money(d) => d.0,
            DataValue::Int(i) => i as f64,
            _ => return Err(EvaluationError::InvalidType),
        };
        
        let command = CreateJournalCommand {
            date,
            description: match self.expression_evaluator.evaluate_expression(&eval_ctx, &journal.description)? {
                DataValue::String(s) => s.into(),
                _ => return Err(EvaluationError::InvalidType),
            },
            amount: journal_amount,
            dimensions: {
                let mut dimensions = BTreeMap::new();
                for (k, v) in journal.dimensions.iter() {
                    dimensions.insert(k.clone(), Arc::new(self.expression_evaluator.evaluate_expression(&eval_ctx, v)?)); 
                }
                dimensions
            },
            ledger_entries: {
                self.build_ledger_entries(&eval_ctx, &journal.operations, journal_amount)?
            },
        };

        self.storage.create_journal(&command)?;
        log::debug!("Created journal: {:?}", command);

        let mut result = ExecutionResult::new();        
        result.journals_created += 1;
        Ok(result)
    }

    fn build_ledger_entries(&self, eval_ctx: &ExpressionEvaluationContext, operations: &Vec<LedgerOperation>, journal_amount: f64) -> Result<Vec<LedgerEntryCommand>, EvaluationError> {
        let mut entries = Vec::new();
        for op in operations {
            let cmd = match op {
                ast::LedgerOperation::Debit(op) => {
                    LedgerEntryCommand::Debit {
                        account_id: op.account.clone(),
                        amount: match &op.amount {
                            Some(amount) => match self.expression_evaluator.evaluate_expression(eval_ctx, &amount)? {
                                DataValue::Money(d) => d.0,
                                DataValue::Int(i) => i as f64,
                                DataValue::Percentage(p) => journal_amount * p.0,
                                _ => return Err(EvaluationError::InvalidType),
                            },
                            None => journal_amount,
                        }
                    }
                },
                ast::LedgerOperation::Credit(op) => {
                    LedgerEntryCommand::Credit {
                        account_id: op.account.clone(),
                        amount: match &op.amount {
                            Some(amount) => match self.expression_evaluator.evaluate_expression(eval_ctx, &amount)? {
                                DataValue::Money(d) => d.0,
                                DataValue::Int(i) => i as f64,
                                DataValue::Percentage(p) => journal_amount * p.0,
                                _ => return Err(EvaluationError::InvalidType),
                            },
                            None => journal_amount,
                        }
                    }
                }
            };

            entries.push(cmd);
        }
        Ok(entries)
    }

    fn create_account(&self, context: &ExecutionContext, account: &AccountExpression) -> Result<ExecutionResult, EvaluationError> {
        //let mut eval_ctx : ExpressionEvaluationContext = context.into();

        self.storage.create_account(account)?;

        log::debug!("Created account: {:?}", account);

        Ok(ExecutionResult::new())
    }

    fn create_rate(&self, context: &ExecutionContext, rate: &CreateRateExpression) -> Result<ExecutionResult, EvaluationError> {
        let cmd = CreateRateCommand {
            id: rate.id.clone(),
        };
        self.storage.create_rate(&cmd)?;
        log::debug!("Created rate: {:?}", rate);

        Ok(ExecutionResult::new())
    }

    fn set_rate(&self, context: &ExecutionContext, rate: &SetRateExpression) -> Result<ExecutionResult, EvaluationError> {
        let mut eval_ctx : ExpressionEvaluationContext = context.into();

        let date = match self.expression_evaluator.evaluate_expression(&eval_ctx, &rate.date)? {
            DataValue::Date(d) => d,
            _ => return Err(EvaluationError::InvalidType),
        };

        eval_ctx.set_effective_date(date);

        let cmd = SetRateCommand {
            id: rate.id.clone(),
            date,
            rate: match self.expression_evaluator.evaluate_expression(&eval_ctx, &rate.rate)? {
                DataValue::Money(d) => d.0,
                DataValue::Int(i) => i as f64,
                DataValue::Percentage(p) => p.0,
                _ => return Err(EvaluationError::InvalidType),
            },
        };
        self.storage.set_rate(&cmd)?;
        log::debug!("Set rate: {:?}", rate);

        Ok(ExecutionResult::new())
    }
    
    fn get(&self, context: &ExecutionContext, get: &GetExpression) -> Result<ExecutionResult, EvaluationError> {
        let eval_ctx : ExpressionEvaluationContext = context.into();
        let mut result = ExecutionResult::new();

        for expr in &get.elements {
            let (key, value) = self.expression_evaluator.evaluate_projection_field(&eval_ctx, &expr)?;
            result.variables.insert(key.into(), value);
        }

        Ok(result)
    }

    fn accrue(&self, context: &ExecutionContext, accrue: &AccrueCommand) -> Result<ExecutionResult, EvaluationError> {
        let mut eval_ctx : ExpressionEvaluationContext = context.into();
        let mut result = ExecutionResult::new();

        let start_date = match self.expression_evaluator.evaluate_expression(&eval_ctx, &accrue.start_date)? {
            DataValue::Date(d) => d,
            _ => return Err(EvaluationError::InvalidType),
        };

        let end_date = match self.expression_evaluator.evaluate_expression(&eval_ctx, &accrue.end_date)? {
            DataValue::Date(d) => d,
            _ => return Err(EvaluationError::InvalidType),
        };

        let effective_date = match self.expression_evaluator.evaluate_expression(&eval_ctx, &accrue.into_journal.date)? {
            DataValue::Date(d) => d,
            _ => return Err(EvaluationError::InvalidType),
        };

        let description = match self.expression_evaluator.evaluate_expression(&eval_ctx, &accrue.into_journal.description)? {
            DataValue::String(s) => s,
            _ => return Err(EvaluationError::InvalidType),
        };
        

        eval_ctx.set_effective_date(effective_date);

        let dimension_values = self.storage.get_dimension_values(&accrue.account_id, accrue.by_dimension.clone(), start_date, end_date);
        let mut amounts = HashMap::new();
        
        let mut dt = start_date;
        while dt <= end_date {
            
            let rate = self.storage.get_rate(&accrue.rate_id, dt)?;
            
            for dimension_value in &dimension_values {
                let dim = (accrue.by_dimension.clone() ,dimension_value.clone());
                let open = self.storage.get_balance(&accrue.account_id, dt, Some(&dim));
                
                let accural = match amounts.get(dimension_value) {
                    Some(pv) => *pv,
                    None => 0.0,
                };
                let delta = calc_daily_accural_amount(rate, open + accural, &accrue.compounding);
                
                amounts.insert(dimension_value.clone(), accural + delta);
            }
            
            dt = match dt.next_day() {
                Some(d) => d,
                None => break,
            };
        }

        for (dimension_value, amount) in amounts {

            let amount = (amount * 100.0).round() / 100.0;
            let dimensions = {
                let mut dimensions = BTreeMap::new();
                dimensions.insert(accrue.by_dimension.clone(), dimension_value.into());
                dimensions
            };
            
            let journal = CreateJournalCommand { 
                date: effective_date, 
                description: description.clone(), 
                amount, 
                ledger_entries: self.build_ledger_entries(&eval_ctx, &accrue.into_journal.operations, amount)?, 
                dimensions 
            };
            self.storage.create_journal(&journal)?;
            result.journals_created += 1;
        }

        Ok(result)
    }
}

fn calc_daily_accural_amount(rate: f64, pv: f64, compounding: &Option<Compounding>) -> f64 {
    match compounding {
        Some(Compounding::Continuous) => pv * rate,
        Some(Compounding::Daily) => pv * rate / 365.0,
        None => pv * rate,
    }
    // match compounding {
    //     Some(Compounding::Continuous) => pv * (1.0 + rate).exp(),
    //     Some(Compounding::Daily) => pv * (1.0 + rate / 365.0).powf(365.0),
    //     None => pv * (1.0 + rate),
    // }
}