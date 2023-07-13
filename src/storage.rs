use std::{collections::{BTreeMap, HashMap}, sync::{Arc, RwLock}, ops::Bound};

use time::Date;
use uuid::Uuid;

use crate::models::{write::{CreateJournalCommand, LedgerEntryCommand}, AccountType, DataValue, read::JournalEntry};


pub enum StorageError {
    IOError(std::io::Error),
    Other(String),
}

pub struct Storage {
    ledger_accounts: RwLock<BTreeMap<Arc<str>, LedgerStore>>,
    journals: RwLock<BTreeMap<u128, JournalEntry>>,
    
}

impl Storage {
    pub fn new() -> Self {
        Self {
            ledger_accounts: RwLock::new(BTreeMap::new()),
            journals: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn create_account(&self, account_id: Arc<str>, account_type: AccountType) -> Result<(), StorageError> {
        let mut ledger_accounts = self.ledger_accounts.write().unwrap();
        ledger_accounts.insert(account_id, LedgerStore::new(account_type));
        Ok(())
    }

    pub fn create_journal(&self, command: &CreateJournalCommand) -> Result<(), StorageError> {
        let jid = Uuid::new_v4().as_u128();

        let entry = JournalEntry {
            date: command.date,
            description: command.description.clone(),
            amount: command.amount,
            dimensions: command.dimensions.clone(),
        };

        self.journals.write().unwrap().insert(jid, entry);

        let mut ledger_accounts = self.ledger_accounts.write().unwrap();

        for ledger_entry in &command.ledger_entries {
            match ledger_entry {
                LedgerEntryCommand::Debit {account_id, amount} => {
                    let ledger_account = ledger_accounts.get_mut(account_id).unwrap();
                    ledger_account.add_entry(command.date, jid, *amount, &command.dimensions);
                },
                LedgerEntryCommand::Credit {account_id, amount} => {
                    let ledger_account = ledger_accounts.get_mut(account_id).unwrap();
                    ledger_account.add_entry(command.date, jid, -*amount, &command.dimensions);
                },
            }
        }

        
        Ok(())
    }

    //pub fn get_balance
}


#[derive(Debug, Clone)]
struct LedgerEntry {
    journal_id: u128,
    amount: f64,
}

struct LedgerStore {
    account_type: AccountType,
    days: BTreeMap<Date, LedgerDay>,
}

impl LedgerStore {
    pub fn new(account_type: AccountType) -> Self {
        Self {
            account_type,
            days: BTreeMap::new(),
        }
    }

    pub fn add_entry(&mut self, date: Date, journal_id: u128, amount: f64, dimensions: &BTreeMap<Arc<str>, DataValue>) {
        let amount = match self.account_type {
            AccountType::Asset | AccountType::Expense => amount,
            AccountType::Liability | AccountType::Equity | AccountType::Income => -amount,
        };
        //todo: get prev day balances
        let day = self.days.entry(date).or_insert(LedgerDay::new());
        day.add_entry(journal_id, amount, dimensions);

        let future_days = self.days.range_mut((Bound::Excluded(date), Bound::Unbounded));
        for (fd, fe) in future_days {
            fe.increment_balance(dimensions, amount);
        }
        
    }

    pub fn get_balance(&self, dimension: &(Arc<str>, DataValue)) -> f64 {        
        let day = self.days.last_key_value().unwrap().1;
        day.get_balance(dimension)
    }
}

#[derive(Debug, Clone)]
struct LedgerDay {
    sum_by_dimension: HashMap<(Arc<str>, DataValue), f64>,
    accumulated_sum_by_dimension: HashMap<(Arc<str>, DataValue), f64>,
    entries: Vec<LedgerEntry>,
}

impl LedgerDay {
    pub fn new() -> Self {
        Self {
            sum_by_dimension: HashMap::new(),
            accumulated_sum_by_dimension: HashMap::new(),
            entries: Vec::new(),
        }
    }

    pub fn add_entry(&mut self, journal_id: u128, amount: f64, dimensions: &BTreeMap<Arc<str>, DataValue>) {
        let entry = LedgerEntry {
            journal_id,
            amount,
        };
        
        self.entries.push(entry);
        self.increment_balance(dimensions, amount);
        
    }

    pub fn increment_balance(&mut self, dimensions: &BTreeMap<Arc<str>, DataValue>, amount: f64) {
        for (dimension, value) in dimensions {
            let sum = self.sum_by_dimension.entry((dimension.clone(), value.clone())).or_insert(0.0);
            *sum += amount;

            let accumulated_sum = self.accumulated_sum_by_dimension.entry((dimension.clone(), value.clone())).or_insert(0.0);
            *accumulated_sum += amount;
        }
    }

    pub fn get_balance(&self, dimension: &(Arc<str>, DataValue)) -> f64 {
        *self.accumulated_sum_by_dimension.get(dimension).unwrap_or(&0.0)
    }
}