pub use dblentry_core::storage::{StorageBackend, StorageError, TransactionId};
pub use dblentry_memory::InMemoryStorage;

/// Default entity used when no entity is specified
pub const DEFAULT_ENTITY: &str = "default";
