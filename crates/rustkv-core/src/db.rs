use std::collections::HashMap;
use std::mem::size_of;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::Instant;

use crate::entry::Entry;
use crate::error::KvError;
use crate::storage::StorageEngine;

#[derive(Debug, Default, Clone)]
pub struct Database {
    data: HashMap<String, Entry>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn memory_estimate_bytes(&self) -> usize {
        self.data
            .iter()
            .map(|(key, entry)| {
                size_of::<String>() + key.len() + size_of::<Entry>() + entry.value.len()
            })
            .sum()
    }

    fn remove_if_expired(&mut self, key: &str) -> bool {
        if self.data.get(key).is_some_and(|entry| entry.is_expired()) {
            self.data.remove(key);
            true
        } else {
            false
        }
    }

    pub fn set_at_ms(
        &mut self,
        key: String,
        value: Vec<u8>,
        timestamp_ms: u64,
    ) -> Result<(), KvError> {
        let Some(duration) = duration_until_unix_ms(timestamp_ms) else {
            self.data.remove(&key);
            return Ok(());
        };

        self.data.insert(
            key,
            Entry::with_expire_at(value, Some(Instant::now() + duration)),
        );
        Ok(())
    }

    pub fn expire_at_ms(&mut self, key: String, timestamp_ms: u64) -> Result<bool, KvError> {
        if self.remove_if_expired(&key) {
            return Ok(false);
        }

        let Some(duration) = duration_until_unix_ms(timestamp_ms) else {
            return Ok(self.data.remove(&key).is_some());
        };

        let Some(entry) = self.data.get_mut(&key) else {
            return Ok(false);
        };

        entry.expire_at = Some(Instant::now() + duration);
        Ok(true)
    }
}

impl StorageEngine for Database {
    fn set(
        &mut self,
        key: String,
        value: Vec<u8>,
        expire: Option<Duration>,
    ) -> Result<(), KvError> {
        self.data.insert(key, Entry::new(value, expire));
        Ok(())
    }

    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }

        Ok(self.data.get(key).map(|entry| entry.value.clone()))
    }

    fn del(&mut self, keys: &[String]) -> Result<usize, KvError> {
        let mut removed = 0;

        for key in keys {
            if self.remove_if_expired(key) {
                continue;
            }

            if self.data.remove(key).is_some() {
                removed += 1;
            }
        }

        Ok(removed)
    }

    fn exists(&mut self, key: &str) -> Result<bool, KvError> {
        if self.remove_if_expired(key) {
            return Ok(false);
        }

        Ok(self.data.contains_key(key))
    }

    fn keys(&self) -> Result<Vec<String>, KvError> {
        let mut keys = self
            .data
            .iter()
            .filter_map(|(key, entry)| {
                if entry.is_expired() {
                    None
                } else {
                    Some(key.clone())
                }
            })
            .collect::<Vec<_>>();

        keys.sort();
        Ok(keys)
    }

    fn expire(&mut self, key: String, secs: u64) -> Result<bool, KvError> {
        if self.remove_if_expired(&key) {
            return Ok(false);
        }

        let Some(entry) = self.data.get_mut(&key) else {
            return Ok(false);
        };

        entry.expire_at = Some(Instant::now() + Duration::from_secs(secs));
        Ok(true)
    }

    fn ttl(&mut self, key: &str) -> Result<i64, KvError> {
        if self.remove_if_expired(key) {
            return Ok(-2);
        }

        let Some(entry) = self.data.get(key) else {
            return Ok(-2);
        };

        let Some(expire_at) = entry.expire_at else {
            return Ok(-1);
        };

        let now = Instant::now();
        if now >= expire_at {
            self.data.remove(key);
            return Ok(-2);
        }

        let remaining = expire_at.duration_since(now);
        let ttl = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
        Ok(ttl as i64)
    }

    fn flushdb(&mut self) -> Result<(), KvError> {
        self.data.clear();
        Ok(())
    }

    fn remove_expired(&mut self) -> usize {
        let before = self.data.len();
        self.data.retain(|_, entry| !entry.is_expired());
        before.saturating_sub(self.data.len())
    }
}

pub fn to_json_string<T: serde::Serialize>(val: &T) -> Result<String, KvError> {
    serde_json::to_string(val).map_err(KvError::from)
}

fn duration_until_unix_ms(timestamp_ms: u64) -> Option<Duration> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let now_ms = now.as_millis();
    let target_ms = u128::from(timestamp_ms);

    if target_ms <= now_ms {
        return None;
    }

    let remaining_ms = target_ms - now_ms;
    let bounded_ms = remaining_ms.min(u128::from(u64::MAX));
    Some(Duration::from_millis(bounded_ms as u64))
}
