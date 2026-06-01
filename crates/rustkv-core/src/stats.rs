use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};

const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const SERVER_ROLE: &str = "standalone";

#[derive(Debug)]
pub struct ServerStats {
    server_version: String,
    role: String,
    addr: String,
    pub memory_estimate_bytes: AtomicU64,
    pub aof_enabled: AtomicBool,
    pub max_frame_size: AtomicU64,
    pub total_commands: AtomicU64,
    pub connected_clients: AtomicU64,
    pub key_count: AtomicU64,
    pub expired_keys: AtomicU64,
    pub get_count: AtomicU64,
    pub set_count: AtomicU64,
    pub del_count: AtomicU64,
    pub uptime_seconds: AtomicU64,
    started_at: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerStatsSnapshot {
    pub server_version: String,
    pub role: String,
    pub uptime_seconds: u64,
    pub memory_estimate_bytes: u64,
    pub aof_enabled: bool,
    pub addr: String,
    pub max_frame_size: u64,
    pub total_commands: u64,
    pub connected_clients: u64,
    pub key_count: u64,
    pub expired_keys: u64,
    pub get_count: u64,
    pub set_count: u64,
    pub del_count: u64,
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            server_version: String::from(SERVER_VERSION),
            role: String::from(SERVER_ROLE),
            addr: String::from("unknown"),
            memory_estimate_bytes: AtomicU64::new(0),
            aof_enabled: AtomicBool::new(false),
            max_frame_size: AtomicU64::new(0),
            total_commands: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
            key_count: AtomicU64::new(0),
            expired_keys: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
            set_count: AtomicU64::new(0),
            del_count: AtomicU64::new(0),
            uptime_seconds: AtomicU64::new(0),
            started_at: Instant::now(),
        }
    }

    pub fn configure_runtime(
        &mut self,
        addr: impl Into<String>,
        aof_enabled: bool,
        max_frame_size: usize,
    ) {
        self.addr = addr.into();
        self.aof_enabled.store(aof_enabled, Ordering::Relaxed);
        self.max_frame_size
            .store(max_frame_size as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ServerStatsSnapshot {
        let uptime_seconds = self
            .uptime_seconds
            .load(Ordering::Relaxed)
            .max(self.started_at.elapsed().as_secs());

        self.uptime_seconds.store(uptime_seconds, Ordering::Relaxed);

        ServerStatsSnapshot {
            server_version: self.server_version.clone(),
            role: self.role.clone(),
            uptime_seconds,
            memory_estimate_bytes: self.memory_estimate_bytes.load(Ordering::Relaxed),
            aof_enabled: self.aof_enabled.load(Ordering::Relaxed),
            addr: self.addr.clone(),
            max_frame_size: self.max_frame_size.load(Ordering::Relaxed),
            total_commands: self.total_commands.load(Ordering::Relaxed),
            connected_clients: self.connected_clients.load(Ordering::Relaxed),
            key_count: self.key_count.load(Ordering::Relaxed),
            expired_keys: self.expired_keys.load(Ordering::Relaxed),
            get_count: self.get_count.load(Ordering::Relaxed),
            set_count: self.set_count.load(Ordering::Relaxed),
            del_count: self.del_count.load(Ordering::Relaxed),
        }
    }

    pub fn incr_total_commands(&self) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_connected_clients(&self) {
        self.connected_clients.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decr_connected_clients(&self) {
        let _ =
            self.connected_clients
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current.checked_sub(1)
                });
    }

    pub fn set_key_count(&self, count: usize) {
        self.key_count.store(count as u64, Ordering::Relaxed);
    }

    pub fn set_memory_estimate_bytes(&self, bytes: usize) {
        self.memory_estimate_bytes
            .store(bytes as u64, Ordering::Relaxed);
    }

    pub fn incr_expired_keys_by(&self, count: usize) {
        self.expired_keys.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn incr_get_count(&self) {
        self.get_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_set_count(&self) {
        self.set_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_del_count(&self) {
        self.del_count.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for ServerStats {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ServerStats {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.snapshot().serialize(serializer)
    }
}
