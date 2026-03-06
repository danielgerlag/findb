use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use serde_json::Value;

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

    pub fn get(&self, key: &str) -> Option<(Value, u16)> {
        let entries = self.entries.read().unwrap();
        entries.get(key).and_then(|entry| {
            if entry.created_at.elapsed() < self.ttl {
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
