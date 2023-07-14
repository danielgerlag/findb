use std::sync::Arc;

use crate::{evaluator::{ExpressionEvaluator, QueryVariables, EvaluationError}, ast::{Statement, JournalExpression, CreateCommand}, storage::Storage, models::write::CreateJournalCommand};

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionContext {
    pub variables: QueryVariables,
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

    pub fn execute(&self, context: ExecutionContext, statement: &Statement) -> Result<ExecutionContext, EvaluationError> {
        match statement {
            Statement::Create(c) => match c {
                CreateCommand::Account => todo!(),
                CreateCommand::Journal(j) => self.create_journal(context, j),
                CreateCommand::Rate => todo!(),
            },
            Statement::Select => todo!(),
            Statement::Accrue => todo!(),
        }
    }

    fn create_journal(&self, context: ExecutionContext, journal: &JournalExpression) -> Result<ExecutionContext, EvaluationError> {
        let command = CreateJournalCommand {
            date: self.expression_evaluator.evaluate_date(&context.variables, &journal.date)?,
            description: self.expression_evaluator.evaluate_string(&context.variables, &journal.description)?,
            amount: self.expression_evaluator.evaluate_decimal(&context.variables, &journal.amount)?,
            dimensions: self.expression_evaluator.evaluate_dimensions(&context.variables, &journal.dimensions)?,
            ledger_entries: self.expression_evaluator.evaluate_ledger_entries(&context.variables, &journal.operations)?,
        };
    }

}