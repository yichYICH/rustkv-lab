use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::entry::Entry;
use crate::error::KvError;
use crate::storage::StorageEngine;

pub const DEFAULT_SHARD_COUNT: usize = 16;

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

#[derive(Debug)]
pub struct DbOutcome<T> {
    pub result: Result<T, KvError>,
    pub key_count: usize,
    pub expired_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DbInfoSnapshot {
    pub key_count: usize,
    pub memory_estimate_bytes: usize,
    pub expired_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbSnapshotEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub expire_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbSnapshot {
    pub entries: Vec<DbSnapshotEntry>,
    pub key_count: usize,
    pub expired_count: usize,
}

#[derive(Debug)]
pub struct ShardedDatabase {
    global: RwLock<()>,
    shards: Vec<RwLock<Database>>,
    key_count: AtomicUsize,
}

impl ShardedDatabase {
    pub fn new(shard_count: usize) -> Self {
        let shard_count = shard_count.max(1);
        let shards = (0..shard_count)
            .map(|_| RwLock::new(Database::new()))
            .collect();

        Self {
            global: RwLock::new(()),
            shards,
            key_count: AtomicUsize::new(0),
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn len(&self) -> usize {
        self.key_count.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub async fn set(
        &self,
        key: String,
        value: Vec<u8>,
        expire: Option<Duration>,
    ) -> DbOutcome<()> {
        let _global = self.global.read().await;
        let shard = self.shard_for_key(&key);
        let mut db = self.shards[shard].write().await;
        let before = db.len();
        let expired_count = db.remove_expired();
        let result = db.set(key, value, expire);
        let after = db.len();
        self.apply_len_change(before, after);

        DbOutcome {
            result,
            key_count: self.len(),
            expired_count,
        }
    }

    pub async fn set_at_ms(&self, key: String, value: Vec<u8>, timestamp_ms: u64) -> DbOutcome<()> {
        let _global = self.global.read().await;
        let shard = self.shard_for_key(&key);
        let mut db = self.shards[shard].write().await;
        let before = db.len();
        let expired_count = db.remove_expired();
        let result = db.set_at_ms(key, value, timestamp_ms);
        let after = db.len();
        self.apply_len_change(before, after);

        DbOutcome {
            result,
            key_count: self.len(),
            expired_count,
        }
    }

    pub async fn get(&self, key: &str) -> DbOutcome<Option<Vec<u8>>> {
        let _global = self.global.read().await;
        let shard = self.shard_for_key(key);
        let mut db = self.shards[shard].write().await;
        let before = db.len();
        let result = db.get(key);
        let after = db.len();
        self.apply_len_change(before, after);

        DbOutcome {
            result,
            key_count: self.len(),
            expired_count: before.saturating_sub(after),
        }
    }

    pub async fn del(&self, keys: &[String]) -> DbOutcome<usize> {
        let _global = self.global.read().await;
        let mut removed = 0;
        let mut expired_count = 0;

        for key in keys {
            let shard = self.shard_for_key(key);
            let mut db = self.shards[shard].write().await;
            let before = db.len();
            let result = db.del(std::slice::from_ref(key));
            let after = db.len();
            self.apply_len_change(before, after);

            match result {
                Ok(count) => {
                    removed += count;
                    expired_count += before.saturating_sub(after).saturating_sub(count);
                }
                Err(error) => {
                    return DbOutcome {
                        result: Err(error),
                        key_count: self.len(),
                        expired_count,
                    };
                }
            }
        }

        DbOutcome {
            result: Ok(removed),
            key_count: self.len(),
            expired_count,
        }
    }

    pub async fn exists(&self, key: &str) -> DbOutcome<bool> {
        let _global = self.global.read().await;
        let shard = self.shard_for_key(key);
        let mut db = self.shards[shard].write().await;
        let before = db.len();
        let result = db.exists(key);
        let after = db.len();
        self.apply_len_change(before, after);

        DbOutcome {
            result,
            key_count: self.len(),
            expired_count: before.saturating_sub(after),
        }
    }

    pub async fn keys(&self) -> Result<Vec<String>, KvError> {
        let _global = self.global.write().await;
        let mut keys = Vec::new();

        for shard in &self.shards {
            let db = shard.read().await;
            keys.extend(db.keys()?);
        }

        keys.sort();
        Ok(keys)
    }

    pub async fn expire(&self, key: String, seconds: u64) -> DbOutcome<bool> {
        let _global = self.global.read().await;
        let shard = self.shard_for_key(&key);
        let mut db = self.shards[shard].write().await;
        let before = db.len();
        let result = db.expire(key, seconds);
        let after = db.len();
        self.apply_len_change(before, after);

        DbOutcome {
            result,
            key_count: self.len(),
            expired_count: before.saturating_sub(after),
        }
    }

    pub async fn expire_at_ms(&self, key: String, timestamp_ms: u64) -> DbOutcome<bool> {
        let _global = self.global.read().await;
        let shard = self.shard_for_key(&key);
        let mut db = self.shards[shard].write().await;
        let before = db.len();
        let result = db.expire_at_ms(key, timestamp_ms);
        let after = db.len();
        self.apply_len_change(before, after);

        DbOutcome {
            result,
            key_count: self.len(),
            expired_count: before.saturating_sub(after),
        }
    }

    pub async fn ttl(&self, key: &str) -> DbOutcome<i64> {
        let _global = self.global.read().await;
        let shard = self.shard_for_key(key);
        let mut db = self.shards[shard].write().await;
        let before = db.len();
        let result = db.ttl(key);
        let after = db.len();
        self.apply_len_change(before, after);

        DbOutcome {
            result,
            key_count: self.len(),
            expired_count: before.saturating_sub(after),
        }
    }

    pub async fn flushdb(&self) -> DbOutcome<()> {
        let _global = self.global.write().await;

        for shard in &self.shards {
            let mut db = shard.write().await;
            if let Err(error) = db.flushdb() {
                return DbOutcome {
                    result: Err(error),
                    key_count: self.len(),
                    expired_count: 0,
                };
            }
        }

        self.key_count.store(0, Ordering::Relaxed);

        DbOutcome {
            result: Ok(()),
            key_count: 0,
            expired_count: 0,
        }
    }

    pub async fn info_snapshot(&self) -> DbInfoSnapshot {
        let _global = self.global.write().await;
        let mut expired_count = 0;
        let mut memory_estimate_bytes = 0;

        for shard in &self.shards {
            let mut db = shard.write().await;
            let before = db.len();
            let removed = db.remove_expired();
            let after = db.len();
            self.apply_len_change(before, after);
            expired_count += removed;
            memory_estimate_bytes += db.memory_estimate_bytes();
        }

        DbInfoSnapshot {
            key_count: self.len(),
            memory_estimate_bytes,
            expired_count,
        }
    }

    pub async fn remove_expired(&self) -> (usize, usize) {
        let _global = self.global.write().await;
        let mut expired_count = 0;

        for shard in &self.shards {
            let mut db = shard.write().await;
            let before = db.len();
            let removed = db.remove_expired();
            let after = db.len();
            self.apply_len_change(before, after);
            expired_count += removed;
        }

        (expired_count, self.len())
    }

    pub async fn snapshot_entries(&self) -> DbSnapshot {
        let _global = self.global.write().await;
        let mut entries = Vec::new();
        let mut expired_count = 0;
        let instant_now = Instant::now();
        let unix_now_ms = current_unix_ms();

        for shard in &self.shards {
            let mut db = shard.write().await;
            let before = db.len();
            db.data.retain(|_, entry| {
                entry
                    .expire_at
                    .is_none_or(|expire_at| instant_now < expire_at)
            });
            let after = db.len();
            self.apply_len_change(before, after);
            expired_count += before.saturating_sub(after);

            entries.extend(db.data.iter().map(|(key, entry)| DbSnapshotEntry {
                key: key.clone(),
                value: entry.value.clone(),
                expire_at_ms: entry.expire_at.map(|expire_at| {
                    let remaining = expire_at.duration_since(instant_now);
                    unix_ms_after(unix_now_ms, remaining)
                }),
            }));
        }

        entries.sort_by(|left, right| left.key.cmp(&right.key));

        DbSnapshot {
            entries,
            key_count: self.len(),
            expired_count,
        }
    }

    fn shard_for_key(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn apply_len_change(&self, before: usize, after: usize) {
        if after >= before {
            self.key_count.fetch_add(after - before, Ordering::Relaxed);
        } else {
            let delta = before - after;
            let _ = self
                .key_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(delta))
                });
        }
    }
}

impl Default for ShardedDatabase {
    fn default() -> Self {
        Self::new(DEFAULT_SHARD_COUNT)
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

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
        .min(u128::from(u64::MAX)) as u64
}

fn unix_ms_after(base_ms: u64, duration: Duration) -> u64 {
    let target = u128::from(base_ms).saturating_add(duration.as_millis());
    target.min(u128::from(u64::MAX)) as u64
}
