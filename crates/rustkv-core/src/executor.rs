use rustkv_protocol::resp::RespValue;
use tokio::sync::RwLock;

use crate::command::Command;
use crate::db::{to_json_string, Database};
use crate::error::KvError;
use crate::stats::ServerStats;
use crate::storage::StorageEngine;

pub async fn execute_command(
    cmd: Command,
    db: &RwLock<Database>,
    stats: &RwLock<ServerStats>,
) -> RespValue {
    {
        let stats_guard = stats.read().await;
        stats_guard.incr_total_commands();
    }

    match cmd {
        Command::Ping => RespValue::SimpleString(String::from("PONG")),
        Command::Set { key, value, expire } => {
            let (result, key_count, expired_count) = {
                let mut db_guard = db.write().await;
                let expired_count = db_guard.remove_expired();
                let result = db_guard.set(key, value, expire);
                (result, db_guard.len(), expired_count)
            };

            {
                let stats_guard = stats.read().await;
                stats_guard.incr_set_count();
                stats_guard.set_key_count(key_count);
                stats_guard.incr_expired_keys_by(expired_count);
            }

            result_to_ok_response(result)
        }
        Command::SetPxAt {
            key,
            value,
            timestamp_ms,
        } => {
            let (result, key_count, expired_count) = {
                let mut db_guard = db.write().await;
                let expired_count = db_guard.remove_expired();
                let result = db_guard.set_at_ms(key, value, timestamp_ms);
                (result, db_guard.len(), expired_count)
            };

            {
                let stats_guard = stats.read().await;
                stats_guard.incr_set_count();
                stats_guard.set_key_count(key_count);
                stats_guard.incr_expired_keys_by(expired_count);
            }

            result_to_ok_response(result)
        }
        Command::Get { key } => {
            let (result, key_count, expired_count) = {
                let mut db_guard = db.write().await;
                let before = db_guard.len();
                let result = db_guard.get(&key);
                let key_count = db_guard.len();
                let expired_count = before.saturating_sub(key_count);
                (result, key_count, expired_count)
            };

            {
                let stats_guard = stats.read().await;
                stats_guard.incr_get_count();
                stats_guard.set_key_count(key_count);
                stats_guard.incr_expired_keys_by(expired_count);
            }

            match result {
                Ok(Some(value)) => bulk_response(value),
                Ok(None) => RespValue::Null,
                Err(error) => error_response(error),
            }
        }
        Command::Del { keys } => {
            let (result, key_count, expired_count) = {
                let mut db_guard = db.write().await;
                let before = db_guard.len();
                let result = db_guard.del(&keys);
                let key_count = db_guard.len();
                let removed_by_del = result.as_ref().copied().unwrap_or(0);
                let expired_count = before
                    .saturating_sub(key_count)
                    .saturating_sub(removed_by_del);
                (result, key_count, expired_count)
            };

            {
                let stats_guard = stats.read().await;
                stats_guard.incr_del_count();
                stats_guard.set_key_count(key_count);
                stats_guard.incr_expired_keys_by(expired_count);
            }

            match result {
                Ok(count) => RespValue::Integer(count as i64),
                Err(error) => error_response(error),
            }
        }
        Command::Exists { key } => {
            let (result, key_count, expired_count) = {
                let mut db_guard = db.write().await;
                let before = db_guard.len();
                let result = db_guard.exists(&key);
                let key_count = db_guard.len();
                let expired_count = before.saturating_sub(key_count);
                (result, key_count, expired_count)
            };

            {
                let stats_guard = stats.read().await;
                stats_guard.set_key_count(key_count);
                stats_guard.incr_expired_keys_by(expired_count);
            }

            match result {
                Ok(true) => RespValue::Integer(1),
                Ok(false) => RespValue::Integer(0),
                Err(error) => error_response(error),
            }
        }
        Command::Keys => {
            let result = {
                let db_guard = db.read().await;
                db_guard.keys()
            };

            match result {
                Ok(keys) => RespValue::Array(
                    keys.into_iter()
                        .map(|key| bulk_response(key.into_bytes()))
                        .collect(),
                ),
                Err(error) => error_response(error),
            }
        }
        Command::Expire { key, seconds } => {
            let (result, key_count, expired_count) = {
                let mut db_guard = db.write().await;
                let before = db_guard.len();
                let result = db_guard.expire(key, seconds);
                let key_count = db_guard.len();
                let expired_count = before.saturating_sub(key_count);
                (result, key_count, expired_count)
            };

            {
                let stats_guard = stats.read().await;
                stats_guard.set_key_count(key_count);
                stats_guard.incr_expired_keys_by(expired_count);
            }

            match result {
                Ok(true) => RespValue::Integer(1),
                Ok(false) => RespValue::Integer(0),
                Err(error) => error_response(error),
            }
        }
        Command::ExpireAt { key, timestamp_ms } => {
            let (result, key_count, expired_count) = {
                let mut db_guard = db.write().await;
                let before = db_guard.len();
                let result = db_guard.expire_at_ms(key, timestamp_ms);
                let key_count = db_guard.len();
                let expired_count = before.saturating_sub(key_count);
                (result, key_count, expired_count)
            };

            {
                let stats_guard = stats.read().await;
                stats_guard.set_key_count(key_count);
                stats_guard.incr_expired_keys_by(expired_count);
            }

            match result {
                Ok(true) => RespValue::Integer(1),
                Ok(false) => RespValue::Integer(0),
                Err(error) => error_response(error),
            }
        }
        Command::Ttl { key } => {
            let (result, key_count, expired_count) = {
                let mut db_guard = db.write().await;
                let before = db_guard.len();
                let result = db_guard.ttl(&key);
                let key_count = db_guard.len();
                let expired_count = before.saturating_sub(key_count);
                (result, key_count, expired_count)
            };

            {
                let stats_guard = stats.read().await;
                stats_guard.set_key_count(key_count);
                stats_guard.incr_expired_keys_by(expired_count);
            }

            match result {
                Ok(ttl) => RespValue::Integer(ttl),
                Err(error) => error_response(error),
            }
        }
        Command::FlushDb => {
            let result = {
                let mut db_guard = db.write().await;
                db_guard.flushdb()
            };

            {
                let stats_guard = stats.read().await;
                stats_guard.set_key_count(0);
            }

            result_to_ok_response(result)
        }
        Command::Info => {
            let result = {
                let (key_count, memory_estimate_bytes, expired_count) = {
                    let mut db_guard = db.write().await;
                    let expired_count = db_guard.remove_expired();
                    (
                        db_guard.len(),
                        db_guard.memory_estimate_bytes(),
                        expired_count,
                    )
                };

                let stats_guard = stats.read().await;
                stats_guard.set_key_count(key_count);
                stats_guard.set_memory_estimate_bytes(memory_estimate_bytes);
                stats_guard.incr_expired_keys_by(expired_count);
                to_json_string(&*stats_guard)
            };

            match result {
                Ok(json) => bulk_response(json.into_bytes()),
                Err(error) => error_response(error),
            }
        }
        Command::Unknown(command) => error_response(format!("ERR unknown command '{command}'")),
    }
}

fn result_to_ok_response(result: Result<(), KvError>) -> RespValue {
    match result {
        Ok(()) => RespValue::SimpleString(String::from("OK")),
        Err(error) => error_response(error),
    }
}

fn bulk_response(bytes: Vec<u8>) -> RespValue {
    RespValue::BulkString(bytes)
}

fn error_response(error: impl ToString) -> RespValue {
    RespValue::Error(error.to_string())
}
