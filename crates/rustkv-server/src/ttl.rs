use std::sync::Arc;
use std::time::Duration;

use rustkv_core::db::ShardedDatabase;
use rustkv_core::stats::ServerStats;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info};

pub fn start_ttl_worker(
    db: Arc<ShardedDatabase>,
    stats: Arc<ServerStats>,
    ttl_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(ttl_interval);

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        info!("TTL worker received shutdown signal");
                        break;
                    }
                }
                _ = interval.tick() => {
                    let (removed, key_count) = db.remove_expired().await;
                    stats.incr_expired_keys_by(removed);
                    stats.set_key_count(key_count);

                    if removed > 0 {
                        debug!(removed, "removed expired keys");
                    }
                }
            }
        }

        info!("TTL worker stopped");
    })
}
