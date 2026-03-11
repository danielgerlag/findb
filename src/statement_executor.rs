use std::{sync::Arc, collections::{BTreeMap, HashMap}, fmt::Display};

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use time::Date;

use crate::{evaluator::{ExpressionEvaluator, QueryVariables, EvaluationError, ExpressionEvaluationContext}, ast::{Statement, JournalExpression, CreateCommand, self, AccountExpression, GetExpression, CreateRateExpression, SetCommand, SetRateExpression, AccrueCommand, Compounding, LedgerOperation, DistributeCommand, Period, SellCommand, SplitCommand}, storage::{StorageBackend, TransactionId, DEFAULT_ENTITY}, models::{write::{CreateJournalCommand, LedgerEntryCommand, CreateRateCommand, SetRateCommand}, DataValue}};

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionContext {
    pub effective_date: Date,
    pub variables: QueryVariables,
    pub transaction_id: Option<TransactionId>,
    pub entity_id: Arc<str>,
}

impl ExecutionContext {
    pub fn new(effective_date: Date, variables: QueryVariables) -> Self {
        Self {
            effective_date,
            variables,
            transaction_id: None,
            entity_id: Arc::from(DEFAULT_ENTITY),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionResult {
    pub variables: QueryVariables,
    pub journals_created: usize,
}

impl Default for ExecutionResult {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionResult {
    pub fn new() -> Self {
        Self {
            variables: QueryVariables::new(),
            journals_created: 0,
        }
    }
}

impl Display for ExecutionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut result = String::new();
        for (key, value) in &self.variables {
            result.push_str(&format!("{}: {}\n", key, value));
        }
        if self.journals_created > 0 {
            result.push_str(&format!("journals_created: {}", self.journals_created));
        }
        f.write_str(&result)
    }
}

impl From<&ExecutionContext> for ExpressionEvaluationContext {
    fn from(val: &ExecutionContext) -> Self {
        ExpressionEvaluationContext::new(val.effective_date, val.variables.clone(), val.entity_id.clone())
    }
}

pub struct StatementExecutor {
    expression_evaluator: Arc<ExpressionEvaluator>,
    storage: Arc<dyn StorageBackend>,
}

impl StatementExecutor {
    pub fn new(expression_evaluator: Arc<ExpressionEvaluator>, storage: Arc<dyn StorageBackend>) -> Self {
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
                CreateCommand::Entity(name) => {
                    self.storage.create_entity(name)?;
                    tracing::debug!("Created entity: {}", name);
                    ExecutionResult::new()
                },
            },
            Statement::Get(get) => self.get(context, get)?,
            Statement::Accrue(accrue) => self.accrue(context, accrue)?,
            Statement::Distribute(distribute) => self.distribute(context, distribute)?,
            Statement::Sell(sell) => self.sell(context, sell)?,
            Statement::Split(split) => self.split(context, split)?,
            Statement::Set(s) => match s {
                SetCommand::Rate(r) => self.set_rate(context, r)?,
            },
            Statement::UseEntity(name) => {
                if !self.storage.entity_exists(name) {
                    return Err(EvaluationError::StorageError(
                        crate::storage::StorageError::EntityNotFound(name.to_string())
                    ));
                }
                context.entity_id = name.clone();
                tracing::debug!("Switched to entity: {}", name);
                ExecutionResult::new()
            },
            Statement::Begin => {
                let tx_id = self.storage.begin_transaction()?;
                context.transaction_id = Some(tx_id);
                ExecutionResult::new()
            },
            Statement::Commit => {
                if let Some(tx_id) = context.transaction_id.take() {
                    self.storage.commit_transaction(tx_id)?;
                }
                ExecutionResult::new()
            },
            Statement::Rollback => {
                if let Some(tx_id) = context.transaction_id.take() {
                    self.storage.rollback_transaction(tx_id)?;
                }
                ExecutionResult::new()
            },
        })
    }

    /// Execute a batch of statements within an implicit transaction.
    /// On any error, the entire batch is rolled back.
    pub fn execute_script(&self, context: &mut ExecutionContext, statements: &[Statement]) -> Result<Vec<ExecutionResult>, EvaluationError> {
        let tx_id = self.storage.begin_transaction()?;
        context.transaction_id = Some(tx_id);

        let mut results = Vec::new();
        for statement in statements {
            match self.execute(context, statement) {
                Ok(result) => results.push(result),
                Err(e) => {
                    // If explicit transaction commands changed the tx_id, use whatever is current
                    if let Some(active_tx) = context.transaction_id.take() {
                        let _ = self.storage.rollback_transaction(active_tx);
                    } else {
                        // The implicit transaction may have been committed/replaced by explicit tx commands
                        // Try to rollback the original implicit tx
                        let _ = self.storage.rollback_transaction(tx_id);
                    }
                    return Err(e);
                }
            }
        }

        // Commit the implicit transaction if still active
        if let Some(active_tx) = context.transaction_id.take() {
            self.storage.commit_transaction(active_tx)?;
        }

        Ok(results)
    }

    fn create_journal(&self, context: &ExecutionContext, journal: &JournalExpression) -> Result<ExecutionResult, EvaluationError> {
        let mut eval_ctx : ExpressionEvaluationContext = context.into();

        let date = match self.expression_evaluator.evaluate_expression(&eval_ctx, &journal.date)? {
            DataValue::Date(d) => d,
            _ => return Err(EvaluationError::InvalidType),
        };

        eval_ctx.set_effective_date(date);
        
        let journal_amount = match self.expression_evaluator.evaluate_expression(&eval_ctx, &journal.amount)? {
            DataValue::Money(d) => d,
            DataValue::Int(i) => Decimal::from(i),
            _ => return Err(EvaluationError::InvalidType),
        };
        
        let command = CreateJournalCommand {
            date,
            description: match self.expression_evaluator.evaluate_expression(&eval_ctx, &journal.description)? {
                DataValue::String(s) => s,
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
                let entries = self.build_ledger_entries(&eval_ctx, &journal.operations, journal_amount)?;

                // Validate that total debits == total credits when all operations have explicit amounts
                let all_explicit = journal.operations.iter().all(|op| {
                    match op {
                        ast::LedgerOperation::Debit(d) => d.amount.is_some() || d.unit_spec.is_some(),
                        ast::LedgerOperation::Credit(c) => c.amount.is_some() || c.unit_spec.is_some(),
                    }
                });

                if all_explicit {
                    let mut total_debits = Decimal::ZERO;
                    let mut total_credits = Decimal::ZERO;
                    for entry in &entries {
                        match entry {
                            LedgerEntryCommand::Debit { amount, .. } => total_debits += amount,
                            LedgerEntryCommand::Credit { amount, .. } => total_credits += amount,
                        }
                    }
                    if total_debits != total_credits {
                        return Err(EvaluationError::General(
                            format!("unbalanced journal: total debits ({}) != total credits ({})", total_debits, total_credits)
                        ));
                    }
                }

                entries
            },
        };

        self.storage.create_journal(&context.entity_id, &command)?;
        tracing::debug!("Created journal: {:?}", command);

        let mut result = ExecutionResult::new();        
        result.journals_created += 1;
        Ok(result)
    }

    fn build_ledger_entries(&self, eval_ctx: &ExpressionEvaluationContext, operations: &Vec<LedgerOperation>, journal_amount: Decimal) -> Result<Vec<LedgerEntryCommand>, EvaluationError> {
        let mut entries = Vec::new();
        for op in operations {
            let cmd = match op {
                ast::LedgerOperation::Debit(op) => {
                    if let Some(us) = &op.unit_spec {
                        let units = match self.expression_evaluator.evaluate_expression(eval_ctx, &us.units)? {
                            DataValue::Money(d) => d,
                            DataValue::Int(i) => Decimal::from(i),
                            _ => return Err(EvaluationError::InvalidType),
                        };
                        let price = match self.expression_evaluator.evaluate_expression(eval_ctx, &us.price)? {
                            DataValue::Money(d) => d,
                            DataValue::Int(i) => Decimal::from(i),
                            _ => return Err(EvaluationError::InvalidType),
                        };
                        LedgerEntryCommand::Debit {
                            account_id: op.account.clone(),
                            amount: units * price,
                            units: Some(units),
                        }
                    } else {
                        LedgerEntryCommand::Debit {
                            account_id: op.account.clone(),
                            amount: match &op.amount {
                                Some(amount) => match self.expression_evaluator.evaluate_expression(eval_ctx, amount)? {
                                    DataValue::Money(d) => d,
                                    DataValue::Int(i) => Decimal::from(i),
                                    DataValue::Percentage(p) => journal_amount * p,
                                    _ => return Err(EvaluationError::InvalidType),
                                },
                                None => journal_amount,
                            },
                            units: None,
                        }
                    }
                },
                ast::LedgerOperation::Credit(op) => {
                    if let Some(us) = &op.unit_spec {
                        let units = match self.expression_evaluator.evaluate_expression(eval_ctx, &us.units)? {
                            DataValue::Money(d) => d,
                            DataValue::Int(i) => Decimal::from(i),
                            _ => return Err(EvaluationError::InvalidType),
                        };
                        let price = match self.expression_evaluator.evaluate_expression(eval_ctx, &us.price)? {
                            DataValue::Money(d) => d,
                            DataValue::Int(i) => Decimal::from(i),
                            _ => return Err(EvaluationError::InvalidType),
                        };
                        LedgerEntryCommand::Credit {
                            account_id: op.account.clone(),
                            amount: units * price,
                            units: Some(units),
                        }
                    } else {
                        LedgerEntryCommand::Credit {
                            account_id: op.account.clone(),
                            amount: match &op.amount {
                                Some(amount) => match self.expression_evaluator.evaluate_expression(eval_ctx, amount)? {
                                    DataValue::Money(d) => d,
                                    DataValue::Int(i) => Decimal::from(i),
                                    DataValue::Percentage(p) => journal_amount * p,
                                    _ => return Err(EvaluationError::InvalidType),
                                },
                                None => journal_amount,
                            },
                            units: None,
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

        self.storage.create_account(&context.entity_id, account)?;

        tracing::debug!("Created account: {:?}", account);

        Ok(ExecutionResult::new())
    }

    fn create_rate(&self, context: &ExecutionContext, rate: &CreateRateExpression) -> Result<ExecutionResult, EvaluationError> {
        let cmd = CreateRateCommand {
            id: rate.id.clone(),
        };
        self.storage.create_rate(&context.entity_id, &cmd)?;
        tracing::debug!("Created rate: {:?}", rate);

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
                DataValue::Money(d) => d,
                DataValue::Int(i) => Decimal::from(i),
                DataValue::Percentage(p) => p,
                _ => return Err(EvaluationError::InvalidType),
            },
        };
        self.storage.set_rate(&context.entity_id, &cmd)?;
        tracing::debug!("Set rate: {:?}", rate);

        Ok(ExecutionResult::new())
    }
    
    fn get(&self, context: &ExecutionContext, get: &GetExpression) -> Result<ExecutionResult, EvaluationError> {
        let eval_ctx : ExpressionEvaluationContext = context.into();
        let mut result = ExecutionResult::new();

        for expr in &get.elements {
            let (key, value) = self.expression_evaluator.evaluate_projection_field(&eval_ctx, expr)?;
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

        let dimension_values = self.storage.get_dimension_values(&context.entity_id, &accrue.account_id, accrue.by_dimension.clone(), start_date, end_date)?;
        let mut amounts = HashMap::new();
        
        let mut dt = start_date;
        while dt <= end_date {
            
            let rate = self.storage.get_rate(&context.entity_id, &accrue.rate_id, dt)?;
            
            for dimension_value in &dimension_values {
                let dim = (accrue.by_dimension.clone() ,dimension_value.clone());
                let open = self.storage.get_balance(&context.entity_id, &accrue.account_id, dt, Some(&dim))?;
                
                let accural = match amounts.get(dimension_value) {
                    Some(pv) => *pv,
                    None => Decimal::ZERO,
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

            let amount = amount.round_dp(2);
            let dimensions = {
                let mut dimensions = BTreeMap::new();
                dimensions.insert(accrue.by_dimension.clone(), dimension_value);
                dimensions
            };
            
            let journal = CreateJournalCommand { 
                date: effective_date, 
                description: description.clone(), 
                amount, 
                ledger_entries: self.build_ledger_entries(&eval_ctx, &accrue.into_journal.operations, amount)?, 
                dimensions 
            };
            self.storage.create_journal(&context.entity_id, &journal)?;
            result.journals_created += 1;
        }

        Ok(result)
    }
    fn distribute(&self, context: &ExecutionContext, cmd: &DistributeCommand) -> Result<ExecutionResult, EvaluationError> {
        let eval_ctx: ExpressionEvaluationContext = context.into();
        let mut result = ExecutionResult::new();

        let start_date = match self.expression_evaluator.evaluate_expression(&eval_ctx, &cmd.start_date)? {
            DataValue::Date(d) => d,
            _ => return Err(EvaluationError::InvalidType),
        };

        let end_date = match self.expression_evaluator.evaluate_expression(&eval_ctx, &cmd.end_date)? {
            DataValue::Date(d) => d,
            _ => return Err(EvaluationError::InvalidType),
        };

        if end_date < start_date {
            return Err(EvaluationError::General("DISTRIBUTE: end date must be on or after start date".into()));
        }

        let total_amount = match self.expression_evaluator.evaluate_expression(&eval_ctx, &cmd.amount)? {
            DataValue::Money(d) => d,
            DataValue::Int(i) => Decimal::from(i),
            _ => return Err(EvaluationError::InvalidType),
        };

        if total_amount == Decimal::ZERO {
            return Err(EvaluationError::General("DISTRIBUTE: amount must not be zero".into()));
        }

        let description = match self.expression_evaluator.evaluate_expression(&eval_ctx, &cmd.description)? {
            DataValue::String(s) => s,
            _ => return Err(EvaluationError::InvalidType),
        };

        let dimensions = {
            let mut dims = BTreeMap::new();
            for (k, v) in cmd.dimensions.iter() {
                dims.insert(k.clone(), Arc::new(self.expression_evaluator.evaluate_expression(&eval_ctx, v)?));
            }
            dims
        };

        let periods = generate_periods(start_date, end_date, &cmd.period);
        let num_periods = periods.len();

        let amounts = if cmd.prorate {
            // Allocate by day count
            let total_days: i64 = periods.iter().map(|(s, e)| (*e - *s).whole_days() + 1).sum();
            let mut allocated = Decimal::ZERO;
            let mut period_amounts = Vec::with_capacity(num_periods);
            for (i, (ps, pe)) in periods.iter().enumerate() {
                if i == num_periods - 1 {
                    period_amounts.push(total_amount - allocated);
                } else {
                    let days = (*pe - *ps).whole_days() + 1;
                    let amt = (total_amount * Decimal::from(days) / Decimal::from(total_days)).round_dp(2);
                    allocated += amt;
                    period_amounts.push(amt);
                }
            }
            period_amounts
        } else {
            // Even split, remainder to last period
            let per_period = (total_amount / Decimal::from(num_periods as i64)).round_dp(2);
            let mut period_amounts = vec![per_period; num_periods];
            let allocated = per_period * Decimal::from((num_periods - 1) as i64);
            period_amounts[num_periods - 1] = total_amount - allocated;
            period_amounts
        };

        for (i, (_ps, pe)) in periods.iter().enumerate() {
            let period_amount = amounts[i];
            let mut period_eval_ctx = eval_ctx.clone();
            period_eval_ctx.set_effective_date(*pe);

            let journal = CreateJournalCommand {
                date: *pe,
                description: description.clone(),
                amount: period_amount,
                ledger_entries: self.build_ledger_entries(&period_eval_ctx, &cmd.operations, period_amount)?,
                dimensions: dimensions.clone(),
            };
            self.storage.create_journal(&context.entity_id, &journal)?;
            result.journals_created += 1;
        }

        Ok(result)
    }

    fn sell(&self, context: &ExecutionContext, sell: &SellCommand) -> Result<ExecutionResult, EvaluationError> {
        let eval_ctx: ExpressionEvaluationContext = context.into();

        let units = match self.expression_evaluator.evaluate_expression(&eval_ctx, &sell.units)? {
            DataValue::Money(d) => d,
            DataValue::Int(i) => Decimal::from(i),
            _ => return Err(EvaluationError::InvalidType),
        };

        let price = match self.expression_evaluator.evaluate_expression(&eval_ctx, &sell.price)? {
            DataValue::Money(d) => d,
            DataValue::Int(i) => Decimal::from(i),
            _ => return Err(EvaluationError::InvalidType),
        };

        let date = match self.expression_evaluator.evaluate_expression(&eval_ctx, &sell.date)? {
            DataValue::Date(d) => d,
            _ => return Err(EvaluationError::InvalidType),
        };

        let description = match self.expression_evaluator.evaluate_expression(&eval_ctx, &sell.description)? {
            DataValue::String(s) => s,
            _ => return Err(EvaluationError::InvalidType),
        };

        let proceeds = units * price;

        // Evaluate dimensions for the SELL
        let mut dim_map = BTreeMap::new();
        for (key, expr) in &sell.dimensions {
            let val = self.expression_evaluator.evaluate_expression(&eval_ctx, expr)?;
            dim_map.insert(key.clone(), Arc::new(val));
        }

        let cost_basis = self.storage.deplete_lots(&context.entity_id, &sell.account, units, &sell.method, &dim_map)?;
        let gain_or_loss = proceeds - cost_basis;

        let mut entries = vec![
            LedgerEntryCommand::Debit {
                account_id: sell.proceeds_account.clone(),
                amount: proceeds,
                units: None,
            },
            LedgerEntryCommand::Credit {
                account_id: sell.account.clone(),
                amount: cost_basis,
                units: None, // lots already depleted by sell
            },
        ];

        if gain_or_loss > dec!(0) {
            entries.push(LedgerEntryCommand::Credit {
                account_id: sell.gain_loss_account.clone(),
                amount: gain_or_loss,
                units: None,
            });
        } else if gain_or_loss < dec!(0) {
            entries.push(LedgerEntryCommand::Debit {
                account_id: sell.gain_loss_account.clone(),
                amount: gain_or_loss.abs(),
                units: None,
            });
        }

        let command = CreateJournalCommand {
            date,
            description,
            amount: proceeds,
            dimensions: dim_map,
            ledger_entries: entries,
        };

        self.storage.create_journal(&context.entity_id, &command)?;

        let mut result = ExecutionResult::new();
        result.journals_created += 1;
        Ok(result)
    }

    fn split(&self, context: &ExecutionContext, split: &SplitCommand) -> Result<ExecutionResult, EvaluationError> {
        let eval_ctx: ExpressionEvaluationContext = context.into();

        let new_units = match self.expression_evaluator.evaluate_expression(&eval_ctx, &split.new_units)? {
            DataValue::Money(d) => d,
            DataValue::Int(i) => Decimal::from(i),
            _ => return Err(EvaluationError::InvalidType),
        };

        let old_units = match self.expression_evaluator.evaluate_expression(&eval_ctx, &split.old_units)? {
            DataValue::Money(d) => d,
            DataValue::Int(i) => Decimal::from(i),
            _ => return Err(EvaluationError::InvalidType),
        };

        if old_units == dec!(0) {
            return Err(EvaluationError::DivideByZero);
        }

        let ratio = new_units / old_units;
        self.storage.split_lots(&context.entity_id, &split.account, ratio, None)?;

        Ok(ExecutionResult::new())
    }
}

/// Generate a list of (period_start, period_end) date tuples for the given range and frequency.
fn generate_periods(start: Date, end: Date, period: &Period) -> Vec<(Date, Date)> {
    let mut periods = Vec::new();
    let mut cursor = start;

    while cursor <= end {
        let period_end = match period {
            Period::Monthly => {
                let m = cursor.month().next();
                let (y, next_month) = if m == time::Month::January {
                    (cursor.year() + 1, m)
                } else {
                    (cursor.year(), m)
                };
                // First day of next month minus 1 day = last day of current period month
                let first_of_next = Date::from_calendar_date(y, next_month, 1).unwrap();
                first_of_next.previous_day().unwrap()
            }
            Period::Quarterly => {
                let quarter_end_month = match cursor.month() {
                    time::Month::January | time::Month::February | time::Month::March => time::Month::March,
                    time::Month::April | time::Month::May | time::Month::June => time::Month::June,
                    time::Month::July | time::Month::August | time::Month::September => time::Month::September,
                    time::Month::October | time::Month::November | time::Month::December => time::Month::December,
                };
                let (y, next_month) = match quarter_end_month {
                    time::Month::December => (cursor.year() + 1, time::Month::January),
                    _ => (cursor.year(), quarter_end_month.next()),
                };
                let first_of_next = Date::from_calendar_date(y, next_month, 1).unwrap();
                first_of_next.previous_day().unwrap()
            }
            Period::Yearly => {
                let first_of_next_year = Date::from_calendar_date(cursor.year() + 1, time::Month::January, 1).unwrap();
                first_of_next_year.previous_day().unwrap()
            }
        };

        // Clamp period_end to the overall end date
        let clamped_end = if period_end > end { end } else { period_end };
        periods.push((cursor, clamped_end));

        // Move cursor to next period
        cursor = match period_end.next_day() {
            Some(d) => d,
            None => break,
        };
    }

    periods
}

fn calc_daily_accural_amount(rate: Decimal, pv: Decimal, compounding: &Option<Compounding>) -> Decimal {
    match compounding {
        Some(Compounding::Continuous) => pv * rate,
        Some(Compounding::Daily) => pv * rate / dec!(365),
        None => pv * rate,
    }
}