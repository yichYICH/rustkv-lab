use rustkv_protocol::resp::RespValue;

use crate::command::Command;
use crate::db::{to_json_string, ShardedDatabase};
use crate::error::KvError;
use crate::stats::ServerStats;

pub async fn execute_command(cmd: Command, db: &ShardedDatabase, stats: &ServerStats) -> RespValue {
    if !matches!(cmd, Command::Info) {
        stats.incr_total_commands();
    }

    match cmd {
        Command::Ping => RespValue::SimpleString(String::from("PONG")),
        Command::Set { key, value, expire } => {
            let outcome = db.set(key, value, expire).await;
            stats.incr_set_count();
            stats.set_key_count(outcome.key_count);
            stats.incr_expired_keys_by(outcome.expired_count);

            result_to_ok_response(outcome.result)
        }
        Command::SetPxAt {
            key,
            value,
            timestamp_ms,
        } => {
            let outcome = db.set_at_ms(key, value, timestamp_ms).await;
            stats.incr_set_count();
            stats.set_key_count(outcome.key_count);
            stats.incr_expired_keys_by(outcome.expired_count);

            result_to_ok_response(outcome.result)
        }
        Command::Get { key } => {
            let outcome = db.get(&key).await;
            stats.incr_get_count();
            stats.set_key_count(outcome.key_count);
            stats.incr_expired_keys_by(outcome.expired_count);

            match outcome.result {
                Ok(Some(value)) => bulk_response(value),
                Ok(None) => RespValue::Null,
                Err(error) => error_response(error),
            }
        }
        Command::Del { keys } => {
            let outcome = db.del(&keys).await;
            stats.incr_del_count();
            stats.set_key_count(outcome.key_count);
            stats.incr_expired_keys_by(outcome.expired_count);

            match outcome.result {
                Ok(count) => RespValue::Integer(count as i64),
                Err(error) => error_response(error),
            }
        }
        Command::Exists { key } => {
            let outcome = db.exists(&key).await;
            stats.set_key_count(outcome.key_count);
            stats.incr_expired_keys_by(outcome.expired_count);

            match outcome.result {
                Ok(true) => RespValue::Integer(1),
                Ok(false) => RespValue::Integer(0),
                Err(error) => error_response(error),
            }
        }
        Command::Keys => match db.keys().await {
            Ok(keys) => RespValue::Array(
                keys.into_iter()
                    .map(|key| bulk_response(key.into_bytes()))
                    .collect(),
            ),
            Err(error) => error_response(error),
        },
        Command::Expire { key, seconds } => {
            let outcome = db.expire(key, seconds).await;
            stats.set_key_count(outcome.key_count);
            stats.incr_expired_keys_by(outcome.expired_count);

            match outcome.result {
                Ok(true) => RespValue::Integer(1),
                Ok(false) => RespValue::Integer(0),
                Err(error) => error_response(error),
            }
        }
        Command::ExpireAt { key, timestamp_ms } => {
            let outcome = db.expire_at_ms(key, timestamp_ms).await;
            stats.set_key_count(outcome.key_count);
            stats.incr_expired_keys_by(outcome.expired_count);

            match outcome.result {
                Ok(true) => RespValue::Integer(1),
                Ok(false) => RespValue::Integer(0),
                Err(error) => error_response(error),
            }
        }
        Command::Ttl { key } => {
            let outcome = db.ttl(&key).await;
            stats.set_key_count(outcome.key_count);
            stats.incr_expired_keys_by(outcome.expired_count);

            match outcome.result {
                Ok(ttl) => RespValue::Integer(ttl),
                Err(error) => error_response(error),
            }
        }
        Command::FlushDb => {
            let outcome = db.flushdb().await;
            stats.set_key_count(outcome.key_count);

            result_to_ok_response(outcome.result)
        }
        Command::Info => {
            let result = {
                let snapshot = db.info_snapshot().await;
                stats.set_key_count(snapshot.key_count);
                stats.set_memory_estimate_bytes(snapshot.memory_estimate_bytes);
                stats.incr_expired_keys_by(snapshot.expired_count);
                to_json_string(stats)
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
