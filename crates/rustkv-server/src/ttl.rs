use std::sync::Arc;
use std::time::Duration;

use rustkv_core::db::Database;
use rustkv_core::stats::ServerStats;
use rustkv_core::storage::StorageEngine;
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info};

pub fn start_ttl_worker(
    db: Arc<RwLock<Database>>,
    stats: Arc<RwLock<ServerStats>>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        info!("TTL worker received shutdown signal");
                        break;
                    }
                }
                _ = interval.tick() => {
                    let (removed, key_count) = {
                        let mut db_guard = db.write().await;
                        let removed = db_guard.remove_expired();
                        let key_count = db_guard.len();
                        (removed, key_count)
                    };

                    {
                        let stats_guard = stats.read().await;
                        stats_guard.incr_expired_keys_by(removed);
                        stats_guard.set_key_count(key_count);
                    }

                    if removed > 0 {
                        debug!(removed, "removed expired keys");
                    }
                }
            }
        }

        info!("TTL worker stopped");
    })
}
