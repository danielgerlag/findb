use std::{collections::{BTreeMap, HashMap, HashSet}, sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}}, ops::Bound};

use rust_decimal::Decimal;
use time::Date;
use uuid::Uuid;

use crate::{models::{write::{CreateJournalCommand, LedgerEntryCommand, CreateRateCommand, SetRateCommand}, DataValue, read::JournalEntry, StatementTxn}, ast::{AccountExpression, AccountType}};


use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
    #[error("no rate found for the given date")]
    NoRateFound,
    #[error("account not found: {0}")]
    AccountNotFound(String),
    #[error("rate not found: {0}")]
    RateNotFound(String),
    #[error("no active transaction")]
    NoActiveTransaction,
}

pub type TransactionId = u64;

pub trait StorageBackend: Send + Sync {
    fn create_account(&self, account: &AccountExpression) -> Result<(), StorageError>;
    fn create_rate(&self, rate: &CreateRateCommand) -> Result<(), StorageError>;
    fn set_rate(&self, command: &SetRateCommand) -> Result<(), StorageError>;
    fn get_rate(&self, id: &str, date: Date) -> Result<Decimal, StorageError>;
    fn create_journal(&self, command: &CreateJournalCommand) -> Result<(), StorageError>;
    fn get_balance(&self, account_id: &str, date: Date, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<Decimal, StorageError>;
    fn get_statement(&self, account_id: &str, from: Bound<Date>, to: Bound<Date>, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<DataValue, StorageError>;
    fn get_dimension_values(&self, account_id: &str, dimension_key: Arc<str>, from: Date, to: Date) -> Result<HashSet<Arc<DataValue>>, StorageError>;
    fn list_accounts(&self) -> Vec<(Arc<str>, AccountType)>;

    fn begin_transaction(&self) -> Result<TransactionId, StorageError>;
    fn commit_transaction(&self, tx_id: TransactionId) -> Result<(), StorageError>;
    fn rollback_transaction(&self, tx_id: TransactionId) -> Result<(), StorageError>;
}

struct Snapshot {
    ledger_accounts: BTreeMap<Arc<str>, LedgerStore>,
    rates: BTreeMap<Arc<str>, RateStore>,
    journals: BTreeMap<u128, JournalEntry>,
    sequence_value: u64,
}

pub struct InMemoryStorage {
    ledger_accounts: RwLock<BTreeMap<Arc<str>, LedgerStore>>,
    rates: RwLock<BTreeMap<Arc<str>, RateStore>>,
    journals: RwLock<BTreeMap<u128, JournalEntry>>,
    sequence_counter: AtomicU64,
    tx_counter: AtomicU64,
    snapshots: RwLock<HashMap<TransactionId, Snapshot>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            ledger_accounts: RwLock::new(BTreeMap::new()),
            rates: RwLock::new(BTreeMap::new()),
            journals: RwLock::new(BTreeMap::new()),
            sequence_counter: AtomicU64::new(1),
            tx_counter: AtomicU64::new(1),
            snapshots: RwLock::new(HashMap::new()),
        }
    }

    fn next_sequence(&self) -> u64 {
        self.sequence_counter.fetch_add(1, Ordering::SeqCst)
    }
}

impl StorageBackend for InMemoryStorage {
    fn create_account(&self, account: &AccountExpression) -> Result<(), StorageError> {
        let mut ledger_accounts = self.ledger_accounts.write().unwrap();
        ledger_accounts.insert(account.id.clone(), LedgerStore::new(account.account_type.clone()));
        Ok(())
    }

    fn create_rate(&self, rate: &CreateRateCommand) -> Result<(), StorageError> {
        let mut rates = self.rates.write().unwrap();
        rates.insert(rate.id.clone(), RateStore::new());
        Ok(())
    }

    fn set_rate(&self, command: &SetRateCommand) -> Result<(), StorageError> {
        let mut rates = self.rates.write().unwrap();
        let rate_store = rates.get_mut(&command.id)
            .ok_or_else(|| StorageError::RateNotFound(command.id.to_string()))?;
        rate_store.add_rate(command.date, command.rate);
        Ok(())
    }

    fn get_rate(&self, id: &str, date: Date) -> Result<Decimal, StorageError> {
        let rates = self.rates.read().unwrap();
        let rate_store = rates.get(id)
            .ok_or_else(|| StorageError::RateNotFound(id.to_string()))?;
        rate_store.get_rate(date)
    }

    fn create_journal(&self, command: &CreateJournalCommand) -> Result<(), StorageError> {
        let jid = Uuid::new_v4().as_u128();
        let seq = self.next_sequence();

        let entry = JournalEntry {
            id: jid,
            sequence: seq,
            date: command.date,
            description: command.description.clone(),
            amount: command.amount,
            dimensions: command.dimensions.clone(),
            created_at: time::OffsetDateTime::now_utc(),
        };

        self.journals.write().unwrap().insert(jid, entry);

        let mut ledger_accounts = self.ledger_accounts.write().unwrap();

        for ledger_entry in &command.ledger_entries {
            match ledger_entry {
                LedgerEntryCommand::Debit {account_id, amount} => {
                    let ledger_account = ledger_accounts.get_mut(account_id)
                        .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
                    ledger_account.add_entry(command.date, jid, *amount, &command.dimensions);
                },
                LedgerEntryCommand::Credit {account_id, amount} => {
                    let ledger_account = ledger_accounts.get_mut(account_id)
                        .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
                    ledger_account.add_entry(command.date, jid, -*amount, &command.dimensions);
                },
            }
        }

        
        Ok(())
    }

    fn get_balance(&self, account_id: &str, date: Date, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<Decimal, StorageError> {
        let ledger_accounts = self.ledger_accounts.read().unwrap();
        let acct = ledger_accounts.get(account_id)
            .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
        Ok(acct.get_balance(date, dimension))
    }

    fn get_statement(&self, account_id: &str, from: Bound<Date>, to: Bound<Date>, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<DataValue, StorageError> {
        let ledger_accounts = self.ledger_accounts.read().unwrap();
        let acct = ledger_accounts.get(account_id)
            .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
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
                        amount: e.1,
                        balance: e.2,
                    });
                },
                None => {},
            }
        }

        Ok(DataValue::Statement(result))
    }

    fn get_dimension_values(&self, account_id: &str, dimension_key: Arc<str>, from: Date, to: Date) -> Result<HashSet<Arc<DataValue>>, StorageError> {
        let ledger_accounts = self.ledger_accounts.read().unwrap();
        let acct = ledger_accounts.get(account_id)
            .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
        let entries = acct.get_dimension_values(dimension_key, from, to);
        drop(ledger_accounts);
        Ok(entries)
    }

    fn list_accounts(&self) -> Vec<(Arc<str>, AccountType)> {
        let ledger_accounts = self.ledger_accounts.read().unwrap();
        let mut result = Vec::new();
        for (k, v) in ledger_accounts.iter() {
            result.push((k.clone(), v.account_type.clone()));
        }
        result
    }

    fn begin_transaction(&self) -> Result<TransactionId, StorageError> {
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst);
        let snapshot = Snapshot {
            ledger_accounts: self.ledger_accounts.read().unwrap().clone(),
            rates: self.rates.read().unwrap().clone(),
            journals: self.journals.read().unwrap().clone(),
            sequence_value: self.sequence_counter.load(Ordering::SeqCst),
        };
        self.snapshots.write().unwrap().insert(tx_id, snapshot);
        tracing::debug!(tx_id, "Transaction started");
        Ok(tx_id)
    }

    fn commit_transaction(&self, tx_id: TransactionId) -> Result<(), StorageError> {
        self.snapshots.write().unwrap().remove(&tx_id)
            .ok_or(StorageError::NoActiveTransaction)?;
        tracing::debug!(tx_id, "Transaction committed");
        Ok(())
    }

    fn rollback_transaction(&self, tx_id: TransactionId) -> Result<(), StorageError> {
        let snapshot = self.snapshots.write().unwrap().remove(&tx_id)
            .ok_or(StorageError::NoActiveTransaction)?;
        *self.ledger_accounts.write().unwrap() = snapshot.ledger_accounts;
        *self.rates.write().unwrap() = snapshot.rates;
        *self.journals.write().unwrap() = snapshot.journals;
        self.sequence_counter.store(snapshot.sequence_value, Ordering::SeqCst);
        tracing::debug!(tx_id, "Transaction rolled back");
        Ok(())
    }
}

#[derive(Clone)]
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

    pub fn add_entry(&mut self, date: Date, journal_id: u128, amount: Decimal, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>) {
        let amount = match self.account_type {
            AccountType::Asset | AccountType::Expense => amount,
            AccountType::Liability | AccountType::Equity | AccountType::Income => -amount,
        };
        //todo: get prev day balances
        let day = self.days.entry(date).or_insert(LedgerDay::new());
        day.add_entry(journal_id, amount, dimensions);

        let future_days = self.days.range_mut((Bound::Excluded(date), Bound::Unbounded));
        for (_fd, fe) in future_days {
            fe.increment_balance(dimensions, amount);
        }
        
    }

    pub fn get_balance(&self, date: Date, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Decimal {        
        let mut balance = Decimal::ZERO;
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

    pub fn get_statement(&self, from: Bound<Date>, to: Bound<Date>, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Vec<(u128, Decimal, Decimal)> {        
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
    sum_by_dimension: HashMap<Arc<str>, HashMap<Arc<DataValue>, Decimal>>,
    total: Decimal,
    entries: HashMap<u128, Decimal>,
    entry_by_dimension: HashMap<(Arc<str>, Arc<DataValue>), Vec<u128>>,
}

impl LedgerDay {
    pub fn new() -> Self {
        Self {
            sum_by_dimension: HashMap::new(),
            total: Decimal::ZERO,
            entries: HashMap::new(),
            entry_by_dimension: HashMap::new(),
        }
    }

    pub fn add_entry(&mut self, journal_id: u128, amount: Decimal, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>) {
        
        self.entries.insert(journal_id, amount);
        for (k, v) in dimensions {
            let e = self.entry_by_dimension.entry((k.clone(), v.clone())).or_insert(Vec::new());
            e.push(journal_id);
        }
        
        self.increment_balance(dimensions, amount);
        
    }

    fn increment_balance(&mut self, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>, amount: Decimal) {
        self.total += amount;
        for (dimension, value) in dimensions {
            let sum = self.sum_by_dimension
                .entry(dimension.clone())
                .or_insert(HashMap::new())
                .entry(value.clone())
                .or_insert(Decimal::ZERO);
        
            *sum += amount;
        }
    }

    pub fn get_balance(&self, dimension: &(Arc<str>, Arc<DataValue>)) -> Decimal {
        *self.sum_by_dimension
            .get(&dimension.0)
            .unwrap_or(&HashMap::new())
            .get(&dimension.1)
            .unwrap_or(&Decimal::ZERO)
    }

    pub fn get_dimension_values(&self, dimension: Arc<str>) -> HashSet<Arc<DataValue>> {
        match self.sum_by_dimension.get(&dimension) {
            Some(d) => d.keys().cloned().collect(),
            None => HashSet::new(),
        }
    }

    pub fn get_entries(&self, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Vec<(u128, Decimal)> {
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


#[derive(Clone)]
struct RateStore {
    values: BTreeMap<Date, Decimal>,
}

impl RateStore {
    pub fn new() -> Self {
        Self {
            values: BTreeMap::new(),
        }
    }

    pub fn add_rate(&mut self, date: Date, value: Decimal) {
        self.values.insert(date, value);
    }

    pub fn get_rate(&self, date: Date) -> Result<Decimal, StorageError> {
        let mut rates = self.values.range((Bound::Unbounded, Bound::Included(date)));
        match rates.next_back() {
            Some((_, rate)) => Ok(*rate),
            None => Err(StorageError::NoRateFound),
        }
    }
}