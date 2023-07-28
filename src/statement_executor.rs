use std::{sync::Arc, collections::BTreeMap};

use serde::__private::de;
use time::Date;

use crate::{evaluator::{ExpressionEvaluator, QueryVariables, EvaluationError, ExpressionEvaluationContext}, ast::{Statement, JournalExpression, CreateCommand, LedgerOperationData, self, AccountExpression, GetExpression}, storage::Storage, models::{write::{CreateJournalCommand, LedgerEntryCommand}, DataValue}};

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
}

impl ExecutionResult {
    pub fn new() -> Self {
        Self {
            variables: QueryVariables::new(),
        }
    }
}

impl Into<ExpressionEvaluationContext> for &ExecutionContext {
    fn into(self) -> ExpressionEvaluationContext {
        ExpressionEvaluationContext::new(Some(self.effective_date), self.variables.clone())
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
                CreateCommand::Rate => todo!(),
            },
            Statement::Get(get) => self.get(context, get)?,
            Statement::Accrue => todo!(),
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
                let mut entries = Vec::new();
                for op in &journal.operations {
                    let cmd = match op {
                        ast::LedgerOperation::Debit(op) => {
                            LedgerEntryCommand::Debit {
                                account_id: op.account.clone(),
                                amount: match &op.amount {
                                    Some(amount) => match self.expression_evaluator.evaluate_expression(&eval_ctx, &amount)? {
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
                                    Some(amount) => match self.expression_evaluator.evaluate_expression(&eval_ctx, &amount)? {
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

                entries
            },
        };

        self.storage.create_journal(&command)?;

        Ok(ExecutionResult::new())
    }

    fn create_account(&self, context: &ExecutionContext, account: &AccountExpression) -> Result<ExecutionResult, EvaluationError> {
        //let mut eval_ctx : ExpressionEvaluationContext = context.into();

        self.storage.create_account(account)?;

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
}

