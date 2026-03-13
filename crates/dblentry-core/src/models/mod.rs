use std::{collections::BTreeMap, sync::Arc};
use rust_decimal::Decimal;
use time::Date;

pub mod write;
pub mod read;

#[derive(Debug, Clone, PartialEq, Hash, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AccountType {
    Asset,
    Liability,
    Equity,
    Income,
    Expense,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AccountExpression {
    pub id: Arc<str>,
    pub account_type: AccountType,
    pub unit_rate_id: Option<Arc<str>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CostMethod {
    Fifo,
    Lifo,
    Average,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Lot {
    pub date: Date,
    pub units_remaining: Decimal,
    pub cost_per_unit: Decimal,
    pub journal_id: u128,
    pub dimensions: BTreeMap<Arc<str>, Arc<DataValue>>,
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct LotItem {
    pub date: Date,
    pub units: Decimal,
    pub cost_per_unit: Decimal,
    pub total_cost: Decimal,
    pub dimensions: BTreeMap<Arc<str>, Arc<DataValue>>,
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum DataValue {
    Null,
    Bool(bool),
    Int(i64),
    Money(Decimal),
    Percentage(Decimal),
    String(Arc<str>),
    Date(Date),
    List(Vec<DataValue>),
    Map(BTreeMap<Arc<str>, DataValue>),
    AccountId(Arc<str>),
    Dimension((Arc<str>, Arc<DataValue>)),
    Statement(Vec<StatementTxn>),
    TrialBalance(Vec<TrialBalanceItem>),
    Lots(Vec<LotItem>),
}

impl DataValue {
    pub fn is_null(&self) -> bool {
        matches!(self, DataValue::Null)
    }
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct StatementTxn {
    pub journal_id: u128,
    pub date: Date,
    pub description: Arc<str>,
    pub amount: Decimal,
    pub balance: Decimal,
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct TrialBalanceItem {
    pub account_id: Arc<str>,
    pub account_type: AccountType,    
    pub balance: Decimal,
}
