use std::time::Duration;

use tokio::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub value: Vec<u8>,
    pub expire_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: Vec<u8>, expire: Option<Duration>) -> Self {
        Self {
            value,
            expire_at: expire.map(|duration| Instant::now() + duration),
        }
    }

    pub fn with_expire_at(value: Vec<u8>, expire_at: Option<Instant>) -> Self {
        Self { value, expire_at }
    }

    pub fn is_expired(&self) -> bool {
        self.expire_at
            .is_some_and(|expire_at| Instant::now() >= expire_at)
    }
}
