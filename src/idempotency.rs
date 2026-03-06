use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use serde_json::Value;

/// Sentinel value used to mark an in-flight request (TOCTOU prevention).
const PENDING_STATUS: u16 = 0;

pub enum IdempotencyCheck {
    /// Key was found with a completed response — return it.
    Cached(Value, u16),
    /// Key is already being processed by another request.
    InFlight,
    /// Key was not found — caller should proceed and call `set()` when done.
    Proceed,
}

pub struct IdempotencyStore {
    entries: RwLock<HashMap<String, CachedEntry>>,
    ttl: Duration,
}

struct CachedEntry {
    response: Value,
    status_code: u16,
    created_at: Instant,
}

impl IdempotencyStore {
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    /// Atomically check for a cached result or claim the key.
    /// Returns `Cached` if a completed response exists, `InFlight` if another
    /// request is processing this key, or `Proceed` if the caller should execute.
    pub fn check_or_claim(&self, key: &str) -> IdempotencyCheck {
        let mut entries = self.entries.write().unwrap();
        if let Some(entry) = entries.get(key) {
            if entry.created_at.elapsed() < self.ttl {
                if entry.status_code == PENDING_STATUS {
                    return IdempotencyCheck::InFlight;
                }
                return IdempotencyCheck::Cached(entry.response.clone(), entry.status_code);
            }
            // Expired — fall through and reclaim
        }
        // Insert pending marker atomically
        entries.insert(
            key.to_string(),
            CachedEntry {
                response: Value::Null,
                status_code: PENDING_STATUS,
                created_at: Instant::now(),
            },
        );
        IdempotencyCheck::Proceed
    }

    pub fn get(&self, key: &str) -> Option<(Value, u16)> {
        let entries = self.entries.read().unwrap();
        entries.get(key).and_then(|entry| {
            if entry.created_at.elapsed() < self.ttl && entry.status_code != PENDING_STATUS {
                Some((entry.response.clone(), entry.status_code))
            } else {
                None
            }
        })
    }

    pub fn set(&self, key: String, response: Value, status_code: u16) {
        let mut entries = self.entries.write().unwrap();
        entries.insert(
            key,
            CachedEntry {
                response,
                status_code,
                created_at: Instant::now(),
            },
        );
    }

    pub fn cleanup_expired(&self) {
        let mut entries = self.entries.write().unwrap();
        entries.retain(|_, entry| entry.created_at.elapsed() < self.ttl);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_and_retrieve() {
        let store = IdempotencyStore::new(3600);
        let response = serde_json::json!({"success": true, "results": []});
        store.set("key1".to_string(), response.clone(), 200);

        let result = store.get("key1");
        assert!(result.is_some());
        let (cached_resp, status) = result.unwrap();
        assert_eq!(status, 200);
        assert_eq!(cached_resp, response);
    }

    #[test]
    fn test_missing_key_returns_none() {
        let store = IdempotencyStore::new(3600);
        assert!(store.get("nonexistent").is_none());
    }

    #[test]
    fn test_expired_entry_returns_none() {
        let store = IdempotencyStore::new(0);
        let response = serde_json::json!({"success": true});
        store.set("key1".to_string(), response, 200);
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(store.get("key1").is_none());
    }

    #[test]
    fn test_cleanup_removes_expired() {
        let store = IdempotencyStore::new(0);
        store.set("a".to_string(), serde_json::json!({}), 200);
        store.set("b".to_string(), serde_json::json!({}), 200);
        std::thread::sleep(std::time::Duration::from_millis(10));
        store.cleanup_expired();

        let entries = store.entries.read().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_overwrite_key() {
        let store = IdempotencyStore::new(3600);
        store.set("key1".to_string(), serde_json::json!({"v": 1}), 200);
        store.set("key1".to_string(), serde_json::json!({"v": 2}), 201);

        let (resp, status) = store.get("key1").unwrap();
        assert_eq!(status, 201);
        assert_eq!(resp["v"], 2);
    }

    #[test]
    fn test_non_expired_entry_survives_cleanup() {
        let store = IdempotencyStore::new(3600);
        store.set("keep".to_string(), serde_json::json!({"alive": true}), 200);
        store.cleanup_expired();

        assert!(store.get("keep").is_some());
    }
}
