use std::{sync::Arc, collections::BTreeMap};

use time::Date;

use super::DataValue;


#[derive(Debug, Clone, PartialEq)]
pub struct CreateJournalCommand {
    pub date: Date,
    pub description: Arc<str>,    
    pub amount: f64,
    pub ledger_entries: Vec<LedgerEntryCommand>,
    pub dimensions: BTreeMap<Arc<str>, Arc<DataValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LedgerEntryCommand {
    Debit {account_id: Arc<str>, amount: f64},
    Credit {account_id: Arc<str>, amount: f64},
}


// #[derive(Debug, Clone, PartialEq)]
// pub struct CreateAccountCommand {
//     pub id: Arc<str>,    
    
// }
