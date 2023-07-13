use std::sync::Arc;

use crate::{evaluator::{ExpressionEvaluator, QueryVariables, EvaluationError}, ast::{Statement, JournalExpression, CreateCommand}, storage::Storage};

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
        
    }

}