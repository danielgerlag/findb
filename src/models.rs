use std::{collections::BTreeMap, sync::Arc};

use ordered_float::OrderedFloat;
use time::Date;

pub mod write;
pub mod read;

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum DataValue {
    Null,
    Bool(bool),
    Int(i64),
    Money(OrderedFloat<f64>),
    Percentage(OrderedFloat<f64>),
    String(String),
    Date(Date),
    List(Vec<DataValue>),
    Map(BTreeMap<Arc<str>, DataValue>),
}

impl DataValue {
    pub fn is_null(&self) -> bool {
        match self {
            DataValue::Null => true,
            _ => false,
        }
    }
}

pub enum AccountType {
    Asset,
    Liability,
    Equity,
    Income,
    Expense,
}
