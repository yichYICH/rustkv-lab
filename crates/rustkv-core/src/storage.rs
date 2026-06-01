use std::time::Duration;

use crate::error::KvError;

pub trait StorageEngine {
    fn set(&mut self, key: String, value: Vec<u8>, expire: Option<Duration>)
        -> Result<(), KvError>;

    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, KvError>;

    fn del(&mut self, keys: &[String]) -> Result<usize, KvError>;

    fn exists(&mut self, key: &str) -> Result<bool, KvError>;

    fn keys(&self) -> Result<Vec<String>, KvError>;

    fn expire(&mut self, key: String, secs: u64) -> Result<bool, KvError>;

    fn ttl(&mut self, key: &str) -> Result<i64, KvError>;

    fn flushdb(&mut self) -> Result<(), KvError>;

    fn remove_expired(&mut self) -> usize;
}
