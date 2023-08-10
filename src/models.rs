use std::{collections::BTreeMap, sync::Arc, fmt::Display};

use ordered_float::OrderedFloat;
use prettytable::{Table, row};
use time::Date;

use crate::ast::AccountType;

pub mod write;
pub mod read;

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum DataValue {
    Null,
    Bool(bool),
    Int(i64),
    Money(OrderedFloat<f64>),
    Percentage(OrderedFloat<f64>),
    String(Arc<str>),
    Date(Date),
    List(Vec<DataValue>),
    Map(BTreeMap<Arc<str>, DataValue>),
    AccountId(Arc<str>),
    Dimension((Arc<str>, Arc<DataValue>)),
    Statement(Vec<StatementTxn>),
    TrialBalance(Vec<TrialBalanceItem>),
}

impl DataValue {
    pub fn is_null(&self) -> bool {
        match self {
            DataValue::Null => true,
            _ => false,
        }
    }
}

impl Display for DataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let result: String = match self {
            DataValue::Null => "null".to_string(),
            DataValue::Bool(b) => if *b { "true".to_string() } else { "false".to_string() },
            DataValue::Int(i) => i.to_string(),
            DataValue::Money(m) => m.to_string(),
            DataValue::Percentage(p) => p.to_string(),
            DataValue::String(s) => s.to_string(),
            DataValue::Date(d) => d.to_string(),
            DataValue::List(l) => format!("{:?}", l),
            DataValue::Map(m) => format!("{:?}", m),
            DataValue::AccountId(id) => id.to_string(),
            DataValue::Dimension((name, value)) => format!("{}={}", name, value),
            DataValue::Statement(stmt) => {
                let mut table = Table::new();
                table.add_row(row!["Date", "Description", "Amount", "Balance"]);
                table.add_empty_row();

                for item in stmt {
                    table.add_row(row![item.date, item.description, item.amount, item.balance]);
                }

                format!("\n{}\n", table.to_string())
            },
            DataValue::TrialBalance(tb) => {
                let mut table = Table::new();
                table.add_row(row!["Account", "Debit", "Credit"]);
                table.add_empty_row();

                for item in tb {
                    match item.account_type {
                        AccountType::Asset | AccountType::Expense => {
                            table.add_row(row![item.account_id, item.balance, ""]);
                        },
                        AccountType::Liability | AccountType::Equity | AccountType::Income => {
                            table.add_row(row![item.account_id, "", item.balance]);
                        },
                    }
                }

                format!("\n{}\n", table.to_string())
            },
        };
        
        f.write_str(&result)
    }
}

// pub enum AccountType {
//     Asset,
//     Liability,
//     Equity,
//     Income,
//     Expense,
// }

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct StatementTxn {
    pub journal_id: u128,
    pub date: Date,
    pub description: Arc<str>,
    pub amount: OrderedFloat<f64>,
    pub balance: OrderedFloat<f64>,

}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct TrialBalanceItem {
    pub account_id: Arc<str>,
    pub account_type: AccountType,    
    pub balance: OrderedFloat<f64>,
}