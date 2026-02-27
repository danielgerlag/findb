use std::{collections::{BTreeMap, HashMap, HashSet}, sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}}, ops::Bound};

use rust_decimal::Decimal;
use time::Date;
use uuid::Uuid;

use dblentry_core::{
    AccountExpression, AccountType,
    CreateJournalCommand, LedgerEntryCommand, CreateRateCommand, SetRateCommand,
    DataValue, JournalEntry, StatementTxn,
};

// Re-export core storage types so existing code using crate::storage::* still works
pub use dblentry_core::storage::{StorageBackend, StorageError, TransactionId};

/// Default entity used when no entity is specified
pub const DEFAULT_ENTITY: &str = "default";

#[derive(Clone)]
struct EntityData {
    ledger_accounts: BTreeMap<Arc<str>, LedgerStore>,
    rates: BTreeMap<Arc<str>, RateStore>,
    journals: BTreeMap<u128, JournalEntry>,
}

impl EntityData {
    fn new() -> Self {
        Self {
            ledger_accounts: BTreeMap::new(),
            rates: BTreeMap::new(),
            journals: BTreeMap::new(),
        }
    }
}

struct Snapshot {
    entities: BTreeMap<Arc<str>, EntityData>,
    sequence_value: u64,
}

pub struct InMemoryStorage {
    entities: RwLock<BTreeMap<Arc<str>, EntityData>>,
    sequence_counter: AtomicU64,
    tx_counter: AtomicU64,
    snapshots: RwLock<HashMap<TransactionId, Snapshot>>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        let mut entities = BTreeMap::new();
        entities.insert(Arc::from(DEFAULT_ENTITY), EntityData::new());
        Self {
            entities: RwLock::new(entities),
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
    fn create_entity(&self, entity_id: &str) -> Result<(), StorageError> {
        let mut entities = self.entities.write().unwrap();
        let key: Arc<str> = Arc::from(entity_id);
        if entities.contains_key(&key) {
            return Err(StorageError::EntityAlreadyExists(entity_id.to_string()));
        }
        entities.insert(key, EntityData::new());
        Ok(())
    }

    fn list_entities(&self) -> Vec<Arc<str>> {
        self.entities.read().unwrap().keys().cloned().collect()
    }

    fn entity_exists(&self, entity_id: &str) -> bool {
        self.entities.read().unwrap().contains_key(entity_id)
    }

    fn create_account(&self, entity_id: &str, account: &AccountExpression) -> Result<(), StorageError> {
        let mut entities = self.entities.write().unwrap();
        let entity = entities.get_mut(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;
        entity.ledger_accounts.insert(account.id.clone(), LedgerStore::new(account.account_type.clone()));
        Ok(())
    }

    fn create_rate(&self, entity_id: &str, rate: &CreateRateCommand) -> Result<(), StorageError> {
        let mut entities = self.entities.write().unwrap();
        let entity = entities.get_mut(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;
        entity.rates.insert(rate.id.clone(), RateStore::new());
        Ok(())
    }

    fn set_rate(&self, entity_id: &str, command: &SetRateCommand) -> Result<(), StorageError> {
        let mut entities = self.entities.write().unwrap();
        let entity = entities.get_mut(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;
        let rate_store = entity.rates.get_mut(&command.id)
            .ok_or_else(|| StorageError::RateNotFound(command.id.to_string()))?;
        rate_store.add_rate(command.date, command.rate);
        Ok(())
    }

    fn get_rate(&self, entity_id: &str, id: &str, date: Date) -> Result<Decimal, StorageError> {
        let entities = self.entities.read().unwrap();
        let entity = entities.get(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;
        let rate_store = entity.rates.get(id)
            .ok_or_else(|| StorageError::RateNotFound(id.to_string()))?;
        rate_store.get_rate(date)
    }

    fn create_journal(&self, entity_id: &str, command: &CreateJournalCommand) -> Result<(), StorageError> {
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

        let mut entities = self.entities.write().unwrap();
        let entity = entities.get_mut(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;

        entity.journals.insert(jid, entry);

        for ledger_entry in &command.ledger_entries {
            match ledger_entry {
                LedgerEntryCommand::Debit {account_id, amount} => {
                    let ledger_account = entity.ledger_accounts.get_mut(account_id)
                        .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
                    ledger_account.add_entry(command.date, jid, *amount, &command.dimensions);
                },
                LedgerEntryCommand::Credit {account_id, amount} => {
                    let ledger_account = entity.ledger_accounts.get_mut(account_id)
                        .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
                    ledger_account.add_entry(command.date, jid, -*amount, &command.dimensions);
                },
            }
        }

        Ok(())
    }

    fn get_balance(&self, entity_id: &str, account_id: &str, date: Date, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<Decimal, StorageError> {
        let entities = self.entities.read().unwrap();
        let entity = entities.get(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;
        let acct = entity.ledger_accounts.get(account_id)
            .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
        Ok(acct.get_balance(date, dimension))
    }

    fn get_statement(&self, entity_id: &str, account_id: &str, from: Bound<Date>, to: Bound<Date>, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<DataValue, StorageError> {
        let entities = self.entities.read().unwrap();
        let entity = entities.get(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;
        let acct = entity.ledger_accounts.get(account_id)
            .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
        let entries = acct.get_statement(from, to, dimension);

        let mut result = Vec::new();
        for e in entries {
            if let Some(j) = entity.journals.get(&e.0) {
                result.push(StatementTxn {
                    journal_id: e.0,
                    date: j.date,
                    description: j.description.clone(),
                    amount: e.1,
                    balance: e.2,
                });
            }
        }

        Ok(DataValue::Statement(result))
    }

    fn get_dimension_values(&self, entity_id: &str, account_id: &str, dimension_key: Arc<str>, from: Date, to: Date) -> Result<HashSet<Arc<DataValue>>, StorageError> {
        let entities = self.entities.read().unwrap();
        let entity = entities.get(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;
        let acct = entity.ledger_accounts.get(account_id)
            .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
        Ok(acct.get_dimension_values(dimension_key, from, to))
    }

    fn list_accounts(&self, entity_id: &str) -> Vec<(Arc<str>, AccountType)> {
        let entities = self.entities.read().unwrap();
        match entities.get(entity_id) {
            Some(entity) => entity.ledger_accounts.iter()
                .map(|(k, v)| (k.clone(), v.account_type.clone()))
                .collect(),
            None => Vec::new(),
        }
    }

    fn begin_transaction(&self) -> Result<TransactionId, StorageError> {
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst);
        let snapshot = Snapshot {
            entities: self.entities.read().unwrap().clone(),
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
        *self.entities.write().unwrap() = snapshot.entities;
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
        let day = self.days.entry(date).or_insert(LedgerDay::new());
        day.add_entry(journal_id, amount, dimensions);
    }

    pub fn get_balance(&self, date: Date, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Decimal {        
        let mut balance = Decimal::ZERO;
        let days = self.days.range((Bound::Unbounded, Bound::Included(date)));
        for (_, day) in days {
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
            Bound::Included(d) => d.previous_day().unwrap_or(d),
            Bound::Excluded(d) => d,
            Bound::Unbounded => Date::MIN,
        };

        let mut balance = self.get_balance(balance_date, dimension);

        let days = self.days.range((from, to));
        for (_, day) in days {
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
        let days = self.days.range((Bound::Included(from), Bound::Included(to)));
        for (_, day) in days {
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
            let e = self.entry_by_dimension.entry((k.clone(), v.clone())).or_default();
            e.push(journal_id);
        }
        
        self.increment_balance(dimensions, amount);
        
    }

    fn increment_balance(&mut self, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>, amount: Decimal) {
        self.total += amount;
        for (dimension, value) in dimensions {
            let sum = self.sum_by_dimension
                .entry(dimension.clone())
                .or_default()
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
                if let Some(jids) = self.entry_by_dimension.get(dimension) {
                    for jid in jids {
                        if let Some(amt) = self.entries.get(jid) { result.push((*jid, *amt)) }
                    }
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