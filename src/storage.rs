use std::{collections::{BTreeMap, HashMap, HashSet}, sync::{Arc, RwLock}, ops::Bound, hash::Hash};

use ordered_float::OrderedFloat;
use time::Date;
use uuid::Uuid;

use crate::{models::{write::{CreateJournalCommand, LedgerEntryCommand, CreateRateCommand, SetRateCommand}, DataValue, read::JournalEntry, StatementTxn}, evaluator::EvaluationError, ast::{AccountExpression, AccountType}};


#[derive(Debug)]
pub enum StorageError {
    IOError(std::io::Error),
    Other(String),
    NoRateFound
}

pub struct Storage {
    ledger_accounts: RwLock<BTreeMap<Arc<str>, LedgerStore>>,
    rates: RwLock<BTreeMap<Arc<str>, RateStore>>,
    journals: RwLock<BTreeMap<u128, JournalEntry>>,
    
}

impl Storage {
    pub fn new() -> Self {
        Self {
            ledger_accounts: RwLock::new(BTreeMap::new()),
            rates: RwLock::new(BTreeMap::new()),
            journals: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn create_account(&self, account: &AccountExpression) -> Result<(), StorageError> {
        let mut ledger_accounts = self.ledger_accounts.write().unwrap();
        ledger_accounts.insert(account.id.clone(), LedgerStore::new(account.account_type.clone()));
        Ok(())
    }

    pub fn create_rate(&self, rate: &CreateRateCommand) -> Result<(), StorageError> {
        let mut rates = self.rates.write().unwrap();
        rates.insert(rate.id.clone(), RateStore::new());
        Ok(())
    }

    pub fn set_rate(&self, command: &SetRateCommand) -> Result<(), StorageError> {
        let mut rates = self.rates.write().unwrap();
        let rate_store = rates.get_mut(&command.id).unwrap();
        rate_store.add_rate(command.date, command.rate);
        Ok(())
    }

    pub fn get_rate(&self, id: &str, date: Date) -> Result<f64, StorageError> {
        let rates = self.rates.read().unwrap();
        let rate_store = rates.get(id).unwrap();
        rate_store.get_rate(date)
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

    pub fn get_balance(&self, account_id: &str, date: Date, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> f64 {
        let ledger_accounts = self.ledger_accounts.read().unwrap();
        ledger_accounts.get(account_id).unwrap().get_balance(date, dimension)
    }

    pub fn get_statement(&self, account_id: &str, from: Bound<Date>, to: Bound<Date>, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> DataValue {
        let ledger_accounts = self.ledger_accounts.read().unwrap();
        let acct = ledger_accounts.get(account_id).unwrap(); //.get_balance(date, dimension)
        let entries = acct.get_statement(from, to, dimension);
        drop(ledger_accounts);
        let mut result = Vec::new();

        let journals = self.journals.read().unwrap();

        for e in entries {
            match journals.get(&e.0) {
                Some(j) => {
                    result.push(StatementTxn {
                        journal_id: e.0,
                        date: j.date,
                        description: j.description.clone(),
                        amount: OrderedFloat(e.1),
                        balance: OrderedFloat(e.2),
                    });
                },
                None => {},
            }
        }

        DataValue::Statement(result)
    }

    pub fn get_dimension_values(&self, account_id: &str, dimension_key: Arc<str>, from: Date, to: Date) -> HashSet<Arc<DataValue>> {
        let ledger_accounts = self.ledger_accounts.read().unwrap();
        let acct = ledger_accounts.get(account_id).unwrap();
        let entries = acct.get_dimension_values(dimension_key, from, to);
        drop(ledger_accounts);
        entries
    }

    pub fn list_accounts(&self) -> Vec<(Arc<str>, AccountType)> {
        let ledger_accounts = self.ledger_accounts.read().unwrap();
        let mut result = Vec::new();
        for (k, v) in ledger_accounts.iter() {
            result.push((k.clone(), v.account_type.clone()));
        }
        result
    }
}

// #[derive(Debug, Clone)]
// struct LedgerEntry {
//     journal_id: u128,
//     amount: f64,
// }

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

    pub fn add_entry(&mut self, date: Date, journal_id: u128, amount: f64, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>) {
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

    pub fn get_balance(&self, date: Date, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> f64 {        
        let mut balance = 0.0;
        let mut days = self.days.range((Bound::Unbounded, Bound::Included(date)));
        while let Some((_, day)) = days.next() {
            match &dimension {
                Some(dimension) => {
                    balance += day.get_balance(dimension);
                },
                None => {
                    balance += day.total;
                }
            }
        }
        balance
    }

    pub fn get_statement(&self, from: Bound<Date>, to: Bound<Date>, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Vec<(u128, f64, f64)> {        
        let mut result = Vec::new();
        
        let balance_date = match from {
            Bound::Included(d) => d.previous_day().unwrap(),
            Bound::Excluded(d) => d,
            Bound::Unbounded => Date::MIN,
        };

        let mut balance = self.get_balance(balance_date, dimension);

        let mut days = self.days.range((from, to));
        while let Some((_, day)) = days.next() {
            let entries = day.get_entries(dimension);
            for (jid, amt) in entries {
                balance += amt;
                result.push((jid, amt, balance));
            }
        }

        result
    }

    pub fn get_dimension_values(&self, dimension_key: Arc<str>, from: Date, to: Date) -> HashSet<Arc<DataValue>> {
        let mut result = HashSet::new();
        let mut days = self.days.range((Bound::Included(from), Bound::Included(to)));
        while let Some((_, day)) = days.next() {
            let values = day.get_dimension_values(dimension_key.clone());
            result.extend(values);
        }
        result
    }
}

#[derive(Debug, Clone)]
struct LedgerDay {
    sum_by_dimension: HashMap<Arc<str>, HashMap<Arc<DataValue>, f64>>,
    total: f64,
    entries: HashMap<u128, f64>, // journal_id -> amount
    entry_by_dimension: HashMap<(Arc<str>, Arc<DataValue>), Vec<u128>>,
}

impl LedgerDay {
    pub fn new() -> Self {
        Self {
            sum_by_dimension: HashMap::new(),
            total: 0.0,
            entries: HashMap::new(),
            entry_by_dimension: HashMap::new(),
        }
    }

    pub fn add_entry(&mut self, journal_id: u128, amount: f64, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>) {
        
        self.entries.insert(journal_id, amount);
        for (k, v) in dimensions {
            let e = self.entry_by_dimension.entry((k.clone(), v.clone())).or_insert(Vec::new());
            e.push(journal_id);
        }
        
        self.increment_balance(dimensions, amount);
        
    }

    fn increment_balance(&mut self, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>, amount: f64) {
        self.total += amount;
        for (dimension, value) in dimensions {
            //let sum = self.sum_by_dimension.entry((dimension.clone(), value.clone())).or_insert(0.0);
            let sum = self.sum_by_dimension
                .entry(dimension.clone())
                .or_insert(HashMap::new())
                .entry(value.clone())
                .or_insert(0.0);
        
            *sum += amount;
        }
    }

    pub fn get_balance(&self, dimension: &(Arc<str>, Arc<DataValue>)) -> f64 {
        *self.sum_by_dimension
            .get(&dimension.0)
            .unwrap_or(&HashMap::new())
            .get(&dimension.1)
            .unwrap_or(&0.0)
    }

    pub fn get_dimension_values(&self, dimension: Arc<str>) -> HashSet<Arc<DataValue>> {
        match self.sum_by_dimension.get(&dimension) {
            Some(d) => d.keys().cloned().collect(),
            None => HashSet::new(),
        }
    }

    pub fn get_entries(&self, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Vec<(u128, f64)> {
        let mut result = Vec::new();

        match dimension {
            Some(dimension) => {
                match self.entry_by_dimension.get(dimension) {
                    Some(jids) => {
                        for jid in jids {
                            match self.entries.get(jid) {
                                Some(amt) => result.push((*jid, *amt)),
                                None => {},
                            }
                        }
                    },
                    None => {},
                };
            },
            None => {
                for (jid, amt) in self.entries.iter() {
                    result.push((*jid, *amt));
                }
            },
        }
        
        result
    }
}


struct RateStore {
    values: BTreeMap<Date, f64>,
}

impl RateStore {
    pub fn new() -> Self {
        Self {
            values: BTreeMap::new(),
        }
    }

    pub fn add_rate(&mut self, date: Date, value: f64) {
        self.values.insert(date, value);
    }

    pub fn get_rate(&self, date: Date) -> Result<f64, StorageError> {
        let mut rates = self.values.range((Bound::Unbounded, Bound::Included(date)));
        match rates.next_back() {
            Some((_, rate)) => Ok(*rate),
            None => Err(StorageError::NoRateFound),
        }
    }
}