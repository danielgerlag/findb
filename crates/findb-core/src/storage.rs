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
