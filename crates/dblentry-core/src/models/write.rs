use std::{collections::BTreeMap, sync::Arc};

use rust_decimal::Decimal;
use time::Date;

use super::DataValue;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateJournalCommand {
    pub date: Date,
    pub description: Arc<str>,    
    pub amount: Decimal,
    pub ledger_entries: Vec<LedgerEntryCommand>,
    pub dimensions: BTreeMap<Arc<str>, Arc<DataValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LedgerEntryCommand {
    Debit {account_id: Arc<str>, amount: Decimal},
    Credit {account_id: Arc<str>, amount: Decimal},
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateRateCommand {
    pub id: Arc<str>,    
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetRateCommand {
    pub id: Arc<str>,
    pub date: Date,
    pub rate: Decimal,
}
