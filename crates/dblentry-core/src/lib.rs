//! Core types and traits for DblEntry storage backends.
//!
//! This crate provides the `StorageBackend` trait and all associated types,
//! enabling pluggable storage implementations in separate crates.

pub mod models;
pub mod storage;

// Re-export key types at crate root for convenience
pub use models::{DataValue, StatementTxn, TrialBalanceItem, AccountType, AccountExpression, Lot, LotItem, CostMethod};
pub use models::write::{CreateJournalCommand, LedgerEntryCommand, CreateRateCommand, SetRateCommand};
pub use models::read::JournalEntry;
pub use storage::{StorageBackend, StorageError, TransactionId};

