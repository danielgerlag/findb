use std::{collections::HashSet, ops::Bound, sync::Arc};

use rust_decimal::Decimal;
use time::Date;

use crate::models::{
    write::{CreateJournalCommand, CreateRateCommand, SetRateCommand},
    AccountExpression, AccountType, DataValue,
};

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
    #[error("entity not found: {0}")]
    EntityNotFound(String),
    #[error("entity already exists: {0}")]
    EntityAlreadyExists(String),
}

pub type TransactionId = u64;

pub trait StorageBackend: Send + Sync {
    // Entity management
    fn create_entity(&self, entity_id: &str) -> Result<(), StorageError>;
    fn list_entities(&self) -> Vec<Arc<str>>;
    fn entity_exists(&self, entity_id: &str) -> bool;

    // All data operations scoped by entity_id
    fn create_account(&self, entity_id: &str, account: &AccountExpression) -> Result<(), StorageError>;
    fn create_rate(&self, entity_id: &str, rate: &CreateRateCommand) -> Result<(), StorageError>;
    fn set_rate(&self, entity_id: &str, command: &SetRateCommand) -> Result<(), StorageError>;
    fn get_rate(&self, entity_id: &str, id: &str, date: Date) -> Result<Decimal, StorageError>;
    fn create_journal(&self, entity_id: &str, command: &CreateJournalCommand) -> Result<(), StorageError>;
    fn get_balance(&self, entity_id: &str, account_id: &str, date: Date, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<Decimal, StorageError>;
    fn get_statement(&self, entity_id: &str, account_id: &str, from: Bound<Date>, to: Bound<Date>, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<DataValue, StorageError>;
    fn get_dimension_values(&self, entity_id: &str, account_id: &str, dimension_key: Arc<str>, from: Date, to: Date) -> Result<HashSet<Arc<DataValue>>, StorageError>;
    fn list_accounts(&self, entity_id: &str) -> Vec<(Arc<str>, AccountType)>;

    fn begin_transaction(&self) -> Result<TransactionId, StorageError>;
    fn commit_transaction(&self, tx_id: TransactionId) -> Result<(), StorageError>;
    fn rollback_transaction(&self, tx_id: TransactionId) -> Result<(), StorageError>;
}
