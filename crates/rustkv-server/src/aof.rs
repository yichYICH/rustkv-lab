use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustkv_core::command::Command;
use rustkv_core::db::Database;
use rustkv_core::storage::StorageEngine;
use rustkv_protocol::encoder::encode_resp;
use rustkv_protocol::parser::parse_resp;
use rustkv_protocol::resp::RespValue;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

pub struct AofEngine {
    file: File,
}

impl AofEngine {
    pub async fn new(path: &str) -> Result<Self, io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;

        Ok(Self { file })
    }

    pub async fn append(&mut self, cmd: &Command) -> Result<(), io::Error> {
        let Some(bytes) = command_to_resp_bytes(cmd) else {
            return Ok(());
        };

        self.file.write_all(&bytes).await?;
        self.file.sync_all().await
    }

    pub async fn flush(&mut self) -> Result<(), io::Error> {
        self.file.flush().await?;
        self.file.sync_all().await
    }

    pub async fn load_and_replay(path: &str, db: &RwLock<Database>) -> Result<(), io::Error> {
        let mut file = match File::open(path).await {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;

        let mut offset = 0;
        while offset < bytes.len() {
            let (resp, consumed) = parse_resp(&bytes[offset..]).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to parse AOF RESP command: {error}"),
                )
            })?;
            let cmd = Command::from_resp(resp).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to decode AOF command: {error}"),
                )
            })?;

            replay_command(cmd, db).await?;
            offset += consumed;
        }

        Ok(())
    }
}

fn command_to_resp_bytes(cmd: &Command) -> Option<Vec<u8>> {
    let args = match cmd {
        Command::Set { key, value, expire } => {
            let mut args = vec![b"SET".to_vec(), key.as_bytes().to_vec(), value.clone()];

            if let Some(duration) = expire {
                args.push(b"PXAT".to_vec());
                args.push(expire_at_ms_from_now(duration).to_string().into_bytes());
            }

            args
        }
        Command::SetPxAt {
            key,
            value,
            timestamp_ms,
        } => vec![
            b"SET".to_vec(),
            key.as_bytes().to_vec(),
            value.clone(),
            b"PXAT".to_vec(),
            timestamp_ms.to_string().into_bytes(),
        ],
        Command::Del { keys } => {
            let mut args = Vec::with_capacity(keys.len() + 1);
            args.push(b"DEL".to_vec());
            args.extend(keys.iter().map(|key| key.as_bytes().to_vec()));
            args
        }
        Command::Expire { key, seconds } => vec![
            b"EXPIREAT".to_vec(),
            key.as_bytes().to_vec(),
            expire_at_ms_from_now(&Duration::from_secs(*seconds))
                .to_string()
                .into_bytes(),
        ],
        Command::ExpireAt { key, timestamp_ms } => vec![
            b"EXPIREAT".to_vec(),
            key.as_bytes().to_vec(),
            timestamp_ms.to_string().into_bytes(),
        ],
        Command::FlushDb => vec![b"FLUSHDB".to_vec()],
        _ => return None,
    };

    Some(encode_args(&args))
}

fn encode_args(args: &[Vec<u8>]) -> Vec<u8> {
    let values = args
        .iter()
        .map(|arg| RespValue::BulkString(arg.clone()))
        .collect::<Vec<_>>();
    encode_resp(&RespValue::Array(values))
}

fn expire_at_ms_from_now(duration: &Duration) -> u64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    let target_ms = now_ms.saturating_add(duration.as_millis());
    target_ms.min(u128::from(u64::MAX)) as u64
}

async fn replay_command(cmd: Command, db: &RwLock<Database>) -> Result<(), io::Error> {
    match cmd {
        Command::Set { key, value, expire } => {
            db.write()
                .await
                .set(key, value, expire)
                .map_err(command_replay_error)?;
        }
        Command::SetPxAt {
            key,
            value,
            timestamp_ms,
        } => {
            db.write()
                .await
                .set_at_ms(key, value, timestamp_ms)
                .map_err(command_replay_error)?;
        }
        Command::Del { keys } => {
            db.write().await.del(&keys).map_err(command_replay_error)?;
        }
        Command::Expire { key, seconds } => {
            db.write()
                .await
                .expire(key, seconds)
                .map_err(command_replay_error)?;
        }
        Command::ExpireAt { key, timestamp_ms } => {
            db.write()
                .await
                .expire_at_ms(key, timestamp_ms)
                .map_err(command_replay_error)?;
        }
        Command::FlushDb => {
            db.write().await.flushdb().map_err(command_replay_error)?;
        }
        _ => {}
    }

    Ok(())
}

fn command_replay_error(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use rustkv_core::storage::StorageEngine;
    use tokio::sync::RwLock;

    use super::*;

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn aof_replay_keeps_remaining_ttl_instead_of_resetting_it() {
        let path = temp_aof_path("remaining-ttl");
        let mut aof = AofEngine::new(path.to_str().expect("test path is valid UTF-8"))
            .await
            .expect("create AOF");

        aof.append(&Command::Set {
            key: String::from("ttl-key"),
            value: b"value".to_vec(),
            expire: Some(Duration::from_secs(3)),
        })
        .await
        .expect("append SET PXAT");
        drop(aof);

        tokio::time::sleep(Duration::from_millis(1_100)).await;

        let db = RwLock::new(Database::new());
        AofEngine::load_and_replay(path.to_str().expect("test path is valid UTF-8"), &db)
            .await
            .expect("replay AOF");

        let ttl = db.write().await.ttl("ttl-key").expect("read ttl");
        assert!(
            (1..3).contains(&ttl),
            "TTL should keep elapsed time after replay, got {ttl}"
        );

        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn aof_replay_does_not_restore_already_expired_key() {
        let path = temp_aof_path("expired-key");
        let mut aof = AofEngine::new(path.to_str().expect("test path is valid UTF-8"))
            .await
            .expect("create AOF");

        aof.append(&Command::Set {
            key: String::from("expired"),
            value: b"value".to_vec(),
            expire: Some(Duration::from_secs(1)),
        })
        .await
        .expect("append SET PXAT");
        drop(aof);

        tokio::time::sleep(Duration::from_millis(1_500)).await;

        let db = RwLock::new(Database::new());
        AofEngine::load_and_replay(path.to_str().expect("test path is valid UTF-8"), &db)
            .await
            .expect("replay AOF");

        let value = db.write().await.get("expired").expect("read key");
        assert_eq!(value, None);

        let _ = tokio::fs::remove_file(path).await;
    }

    fn temp_aof_path(name: &str) -> PathBuf {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("rustkv-lab-{name}-{id}.aof"))
    }
}
