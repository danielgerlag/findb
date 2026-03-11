use std::{collections::{BTreeMap, HashMap, HashSet}, sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}}, ops::Bound};

use rust_decimal::Decimal;
use time::Date;
use uuid::Uuid;

use dblentry_core::{
    AccountExpression, AccountType,
    CreateJournalCommand, LedgerEntryCommand, CreateRateCommand, SetRateCommand,
    DataValue, JournalEntry, StatementTxn, Lot, LotItem, CostMethod,
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
    lot_stores: BTreeMap<Arc<str>, LotStoreData>,
    unit_rate_links: BTreeMap<Arc<str>, Arc<str>>,
}

impl EntityData {
    fn new() -> Self {
        Self {
            ledger_accounts: BTreeMap::new(),
            rates: BTreeMap::new(),
            journals: BTreeMap::new(),
            lot_stores: BTreeMap::new(),
            unit_rate_links: BTreeMap::new(),
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
        if entity.ledger_accounts.contains_key(&account.id) {
            return Err(StorageError::DuplicateAccount(account.id.to_string()));
        }
        entity.ledger_accounts.insert(account.id.clone(), LedgerStore::new(account.account_type.clone()));
        if let Some(ref rate_id) = account.unit_rate_id {
            entity.lot_stores.insert(account.id.clone(), LotStoreData::new());
            entity.unit_rate_links.insert(account.id.clone(), rate_id.clone());
        }
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
                LedgerEntryCommand::Debit {account_id, amount, units} => {
                    let ledger_account = entity.ledger_accounts.get_mut(account_id)
                        .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
                    ledger_account.add_entry(command.date, jid, *amount, &command.dimensions);
                    if let Some(unit_count) = units {
                        if let Some(lot_store) = entity.lot_stores.get_mut(account_id) {
                            lot_store.add_lot(Lot {
                                date: command.date,
                                units_remaining: *unit_count,
                                cost_per_unit: if *unit_count != Decimal::ZERO { *amount / *unit_count } else { Decimal::ZERO },
                                journal_id: jid,
                                dimensions: command.dimensions.clone(),
                            });
                        }
                    }
                },
                LedgerEntryCommand::Credit {account_id, amount, units} => {
                    let ledger_account = entity.ledger_accounts.get_mut(account_id)
                        .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;
                    ledger_account.add_entry(command.date, jid, -*amount, &command.dimensions);
                    if let Some(unit_count) = units {
                        if let Some(lot_store) = entity.lot_stores.get_mut(account_id) {
                            lot_store.deplete_fifo(*unit_count)
                                .map_err(StorageError::Other)?;
                        }
                    }
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

    fn list_rates(&self, entity_id: &str) -> Vec<Arc<str>> {
        let entities = self.entities.read().unwrap();
        match entities.get(entity_id) {
            Some(entity) => entity.rates.keys().cloned().collect(),
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

    fn get_lots(&self, entity_id: &str, account_id: &str, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<Vec<LotItem>, StorageError> {
        let entities = self.entities.read().unwrap();
        let entity = entities.get(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;
        match entity.lot_stores.get(account_id) {
            Some(store) => Ok(store.open_lots_filtered(dimension)),
            None => Err(StorageError::Other(format!("Account @{} is not a unit account", account_id))),
        }
    }

    fn get_total_units(&self, entity_id: &str, account_id: &str, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<Decimal, StorageError> {
        let entities = self.entities.read().unwrap();
        let entity = entities.get(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;
        match entity.lot_stores.get(account_id) {
            Some(store) => Ok(store.total_units_filtered(dimension)),
            None => Err(StorageError::Other(format!("Account @{} is not a unit account", account_id))),
        }
    }

    fn deplete_lots(&self, entity_id: &str, account_id: &str, units: Decimal, method: &CostMethod, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>) -> Result<Decimal, StorageError> {
        let mut entities = self.entities.write().unwrap();
        let entity = entities.get_mut(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;
        let store = entity.lot_stores.get_mut(account_id)
            .ok_or_else(|| StorageError::Other(format!("Account @{} is not a unit account", account_id)))?;
        let cost = match method {
            CostMethod::Fifo => store.deplete_fifo_filtered(units, dimensions),
            CostMethod::Lifo => store.deplete_lifo_filtered(units, dimensions),
            CostMethod::Average => store.deplete_average_filtered(units, dimensions),
        }.map_err(StorageError::Other)?;
        Ok(cost)
    }

    fn split_lots(&self, entity_id: &str, account_id: &str, new_per_old: Decimal, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<(), StorageError> {
        let mut entities = self.entities.write().unwrap();
        let entity = entities.get_mut(entity_id)
            .ok_or_else(|| StorageError::EntityNotFound(entity_id.to_string()))?;
        let store = entity.lot_stores.get_mut(account_id)
            .ok_or_else(|| StorageError::Other(format!("Account @{} is not a unit account", account_id)))?;
        store.split_filtered(new_per_old, dimension);
        Ok(())
    }

    fn get_unit_rate_id(&self, entity_id: &str, account_id: &str) -> Option<Arc<str>> {
        let entities = self.entities.read().unwrap();
        entities.get(entity_id)
            .and_then(|e| e.unit_rate_links.get(account_id).cloned())
    }

    fn is_unit_account(&self, entity_id: &str, account_id: &str) -> bool {
        let entities = self.entities.read().unwrap();
        entities.get(entity_id)
            .map(|e| e.lot_stores.contains_key(account_id))
            .unwrap_or(false)
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
            // Also index at ancestor prefixes for hierarchical matching
            if let DataValue::String(s) = v.as_ref() {
                for prefix in ancestor_prefixes(s) {
                    let ancestor_val: Arc<DataValue> = Arc::new(DataValue::String(Arc::from(prefix.as_str())));
                    let e = self.entry_by_dimension.entry((k.clone(), ancestor_val)).or_default();
                    if !e.contains(&journal_id) {
                        e.push(journal_id);
                    }
                }
            }
        }
        
        self.increment_balance(dimensions, amount);
        
    }

    fn increment_balance(&mut self, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>, amount: Decimal) {
        self.total += amount;
        for (dimension, value) in dimensions {
            let dim_map = self.sum_by_dimension
                .entry(dimension.clone())
                .or_default();
            
            // Store at exact value
            *dim_map.entry(value.clone()).or_insert(Decimal::ZERO) += amount;
            
            // Also store at each ancestor prefix for hierarchical queries
            if let DataValue::String(s) = value.as_ref() {
                for prefix in ancestor_prefixes(s) {
                    let ancestor_val: Arc<DataValue> = Arc::new(DataValue::String(Arc::from(prefix.as_str())));
                    *dim_map.entry(ancestor_val).or_insert(Decimal::ZERO) += amount;
                }
            }
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

/// Returns all proper ancestor prefixes of a `/`-separated path.
/// e.g., "Americas/US/West" → ["Americas", "Americas/US"]
fn ancestor_prefixes(path: &str) -> Vec<String> {
    let mut prefixes = Vec::new();
    let mut pos = 0;
    for (i, c) in path.char_indices() {
        if c == '/' {
            prefixes.push(path[..i].to_string());
            pos = i + 1;
        }
    }
    let _ = pos; // suppress unused
    prefixes
}

/// Check if a lot matches a dimension filter with hierarchical prefix matching.
/// A dimension value "Americas/US" matches "Americas", "Americas/US", but not "Americas/EU".
fn dimension_matches(lot_dims: &BTreeMap<Arc<str>, Arc<DataValue>>, filter: &(Arc<str>, Arc<DataValue>)) -> bool {
    let (key, filter_val) = filter;
    match lot_dims.get(key.as_ref()) {
        Some(lot_val) => {
            if lot_val == filter_val {
                return true;
            }
            // Hierarchical prefix match: filter "Americas" matches lot "Americas/US/West"
            if let (DataValue::String(filter_s), DataValue::String(lot_s)) = (filter_val.as_ref(), lot_val.as_ref()) {
                let prefix = filter_s.as_ref();
                let value = lot_s.as_ref();
                value.starts_with(prefix) && value.as_bytes().get(prefix.len()) == Some(&b'/')
            } else {
                false
            }
        }
        None => false,
    }
}

/// Check if a lot matches a full set of dimensions for exact pool matching.
/// Empty dimensions map matches all lots (backward compatible).
fn dimensions_match_exact(lot_dims: &BTreeMap<Arc<str>, Arc<DataValue>>, filter_dims: &BTreeMap<Arc<str>, Arc<DataValue>>) -> bool {
    if filter_dims.is_empty() {
        return true;
    }
    for (key, val) in filter_dims {
        match lot_dims.get(key.as_ref()) {
            Some(lot_val) => {
                if lot_val != val {
                    // Also try hierarchical prefix match
                    if let (DataValue::String(filter_s), DataValue::String(lot_s)) = (val.as_ref(), lot_val.as_ref()) {
                        let prefix = filter_s.as_ref();
                        let value = lot_s.as_ref();
                        if !(value == prefix || (value.starts_with(prefix) && value.as_bytes().get(prefix.len()) == Some(&b'/'))) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
            None => return false,
        }
    }
    true
}

#[derive(Clone)]
struct LotStoreData {
    lots: Vec<Lot>,
}

impl LotStoreData {
    fn new() -> Self {
        Self { lots: Vec::new() }
    }

    fn add_lot(&mut self, lot: Lot) {
        self.lots.push(lot);
    }

    fn total_units(&self) -> Decimal {
        self.lots.iter().map(|l| l.units_remaining).sum()
    }

    fn total_units_filtered(&self, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Decimal {
        match dimension {
            Some(filter) => self.lots.iter()
                .filter(|l| dimension_matches(&l.dimensions, filter))
                .map(|l| l.units_remaining)
                .sum(),
            None => self.total_units(),
        }
    }

    fn open_lots(&self) -> Vec<LotItem> {
        self.lots.iter()
            .filter(|l| l.units_remaining > Decimal::ZERO)
            .map(|l| LotItem {
                date: l.date,
                units: l.units_remaining,
                cost_per_unit: l.cost_per_unit,
                total_cost: l.units_remaining * l.cost_per_unit,
                dimensions: l.dimensions.clone(),
            })
            .collect()
    }

    fn open_lots_filtered(&self, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Vec<LotItem> {
        match dimension {
            Some(filter) => self.lots.iter()
                .filter(|l| l.units_remaining > Decimal::ZERO && dimension_matches(&l.dimensions, filter))
                .map(|l| LotItem {
                    date: l.date,
                    units: l.units_remaining,
                    cost_per_unit: l.cost_per_unit,
                    total_cost: l.units_remaining * l.cost_per_unit,
                    dimensions: l.dimensions.clone(),
                })
                .collect(),
            None => self.open_lots(),
        }
    }

    fn deplete_fifo(&mut self, mut units: Decimal) -> Result<Decimal, String> {
        let available = self.total_units();
        if units > available {
            return Err(format!("Insufficient units: need {}, have {}", units, available));
        }
        let mut total_cost = Decimal::ZERO;
        for lot in &mut self.lots {
            if units <= Decimal::ZERO { break; }
            let take = units.min(lot.units_remaining);
            total_cost += take * lot.cost_per_unit;
            lot.units_remaining -= take;
            units -= take;
        }
        Ok(total_cost)
    }

    fn deplete_fifo_filtered(&mut self, mut units: Decimal, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>) -> Result<Decimal, String> {
        if dimensions.is_empty() {
            return self.deplete_fifo(units);
        }
        let available: Decimal = self.lots.iter()
            .filter(|l| dimensions_match_exact(&l.dimensions, dimensions))
            .map(|l| l.units_remaining)
            .sum();
        if units > available {
            return Err(format!("Insufficient units: need {}, have {}", units, available));
        }
        let mut total_cost = Decimal::ZERO;
        for lot in &mut self.lots {
            if units <= Decimal::ZERO { break; }
            if !dimensions_match_exact(&lot.dimensions, dimensions) { continue; }
            let take = units.min(lot.units_remaining);
            total_cost += take * lot.cost_per_unit;
            lot.units_remaining -= take;
            units -= take;
        }
        Ok(total_cost)
    }

    fn deplete_lifo(&mut self, mut units: Decimal) -> Result<Decimal, String> {
        let available = self.total_units();
        if units > available {
            return Err(format!("Insufficient units: need {}, have {}", units, available));
        }
        let mut total_cost = Decimal::ZERO;
        for lot in self.lots.iter_mut().rev() {
            if units <= Decimal::ZERO { break; }
            let take = units.min(lot.units_remaining);
            total_cost += take * lot.cost_per_unit;
            lot.units_remaining -= take;
            units -= take;
        }
        Ok(total_cost)
    }

    fn deplete_lifo_filtered(&mut self, mut units: Decimal, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>) -> Result<Decimal, String> {
        if dimensions.is_empty() {
            return self.deplete_lifo(units);
        }
        let available: Decimal = self.lots.iter()
            .filter(|l| dimensions_match_exact(&l.dimensions, dimensions))
            .map(|l| l.units_remaining)
            .sum();
        if units > available {
            return Err(format!("Insufficient units: need {}, have {}", units, available));
        }
        let mut total_cost = Decimal::ZERO;
        for lot in self.lots.iter_mut().rev() {
            if units <= Decimal::ZERO { break; }
            if !dimensions_match_exact(&lot.dimensions, dimensions) { continue; }
            let take = units.min(lot.units_remaining);
            total_cost += take * lot.cost_per_unit;
            lot.units_remaining -= take;
            units -= take;
        }
        Ok(total_cost)
    }

    fn deplete_average(&mut self, units: Decimal) -> Result<Decimal, String> {
        let available = self.total_units();
        if units > available {
            return Err(format!("Insufficient units: need {}, have {}", units, available));
        }
        let total_value: Decimal = self.lots.iter()
            .map(|l| l.units_remaining * l.cost_per_unit)
            .sum();
        let avg_cost = if available > Decimal::ZERO { total_value / available } else { Decimal::ZERO };
        let total_cost = (units * avg_cost).round_dp(2);

        // Deplete proportionally from all lots
        let mut remaining = units;
        for lot in &mut self.lots {
            if remaining <= Decimal::ZERO { break; }
            let take = remaining.min(lot.units_remaining);
            lot.units_remaining -= take;
            remaining -= take;
        }
        Ok(total_cost)
    }

    fn deplete_average_filtered(&mut self, units: Decimal, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>) -> Result<Decimal, String> {
        if dimensions.is_empty() {
            return self.deplete_average(units);
        }
        let available: Decimal = self.lots.iter()
            .filter(|l| dimensions_match_exact(&l.dimensions, dimensions))
            .map(|l| l.units_remaining)
            .sum();
        if units > available {
            return Err(format!("Insufficient units: need {}, have {}", units, available));
        }
        let total_value: Decimal = self.lots.iter()
            .filter(|l| dimensions_match_exact(&l.dimensions, dimensions))
            .map(|l| l.units_remaining * l.cost_per_unit)
            .sum();
        let avg_cost = if available > Decimal::ZERO { total_value / available } else { Decimal::ZERO };
        let total_cost = (units * avg_cost).round_dp(2);

        let mut remaining = units;
        for lot in &mut self.lots {
            if remaining <= Decimal::ZERO { break; }
            if !dimensions_match_exact(&lot.dimensions, dimensions) { continue; }
            let take = remaining.min(lot.units_remaining);
            lot.units_remaining -= take;
            remaining -= take;
        }
        Ok(total_cost)
    }

    fn split_filtered(&mut self, new_per_old: Decimal, dimension: Option<&(Arc<str>, Arc<DataValue>)>) {
        for lot in &mut self.lots {
            let matches = match dimension {
                Some(filter) => dimension_matches(&lot.dimensions, filter),
                None => true,
            };
            if matches {
                lot.units_remaining *= new_per_old;
                if new_per_old > Decimal::ZERO {
                    lot.cost_per_unit /= new_per_old;
                }
            }
        }
    }
}