use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustkv_core::command::Command;
use rustkv_core::db::{DbSnapshot, DbSnapshotEntry, ShardedDatabase};
use rustkv_protocol::encoder::encode_resp;
use rustkv_protocol::parser::parse_resp;
use rustkv_protocol::resp::RespValue;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct AofEngine {
    path: PathBuf,
    file: Option<File>,
}

impl AofEngine {
    pub async fn new(path: &str) -> Result<Self, io::Error> {
        let path = PathBuf::from(path);
        let file = open_append_file(&path).await?;

        Ok(Self {
            path,
            file: Some(file),
        })
    }

    pub async fn append(&mut self, cmd: &Command) -> Result<(), io::Error> {
        let Some(bytes) = command_to_resp_bytes(cmd) else {
            return Ok(());
        };

        let file = self.file_mut()?;
        file.write_all(&bytes).await?;
        file.sync_all().await
    }

    pub async fn flush(&mut self) -> Result<(), io::Error> {
        let file = self.file_mut()?;
        file.flush().await?;
        file.sync_all().await
    }

    pub async fn file_size(&self) -> Result<u64, io::Error> {
        match fs::metadata(&self.path).await {
            Ok(metadata) => Ok(metadata.len()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error),
        }
    }

    pub async fn rewrite(&mut self, snapshot: &DbSnapshot) -> Result<(), io::Error> {
        let temp_path = rewrite_temp_path(&self.path);
        let backup_path = rewrite_backup_path(&self.path);

        remove_file_if_exists(&temp_path).await?;
        write_rewrite_file(&temp_path, snapshot).await?;
        self.flush().await?;

        self.file.take();
        let replace_result = replace_aof_file(&self.path, &temp_path, &backup_path).await;
        let reopen_result = open_append_file(&self.path).await;

        match (replace_result, reopen_result) {
            (Ok(()), Ok(file)) => {
                self.file = Some(file);
                Ok(())
            }
            (Err(error), Ok(file)) => {
                self.file = Some(file);
                Err(error)
            }
            (Ok(()), Err(error)) | (Err(error), Err(_)) => Err(error),
        }
    }

    pub async fn load_and_replay(path: &str, db: &ShardedDatabase) -> Result<(), io::Error> {
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

    fn file_mut(&mut self) -> Result<&mut File, io::Error> {
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::other("AOF file is temporarily closed"))
    }
}

async fn open_append_file(path: &Path) -> Result<File, io::Error> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
}

async fn write_rewrite_file(path: &Path, snapshot: &DbSnapshot) -> Result<(), io::Error> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await?;

    for entry in &snapshot.entries {
        let bytes = snapshot_entry_to_resp_bytes(entry);
        file.write_all(&bytes).await?;
    }

    file.flush().await?;
    file.sync_all().await
}

async fn remove_file_if_exists(path: &Path) -> Result<(), io::Error> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

async fn rename_file_if_exists(from: &Path, to: &Path) -> Result<bool, io::Error> {
    match fs::rename(from, to).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

async fn replace_aof_file(
    path: &Path,
    temp_path: &Path,
    backup_path: &Path,
) -> Result<(), io::Error> {
    remove_file_if_exists(backup_path).await?;
    let original_backed_up = rename_file_if_exists(path, backup_path).await?;

    if let Err(error) = fs::rename(temp_path, path).await {
        if original_backed_up {
            let _ = fs::rename(backup_path, path).await;
        }
        return Err(error);
    }

    if original_backed_up {
        remove_file_if_exists(backup_path).await?;
    }

    Ok(())
}

fn rewrite_temp_path(path: &Path) -> PathBuf {
    path.with_file_name(format!(
        "{}.rewrite",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("rustkv.aof")
    ))
}

fn rewrite_backup_path(path: &Path) -> PathBuf {
    path.with_file_name(format!(
        "{}.bak",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("rustkv.aof")
    ))
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

fn snapshot_entry_to_resp_bytes(entry: &DbSnapshotEntry) -> Vec<u8> {
    let command = if let Some(timestamp_ms) = entry.expire_at_ms {
        Command::SetPxAt {
            key: entry.key.clone(),
            value: entry.value.clone(),
            timestamp_ms,
        }
    } else {
        Command::Set {
            key: entry.key.clone(),
            value: entry.value.clone(),
            expire: None,
        }
    };

    command_to_resp_bytes(&command).expect("snapshot entries must encode to AOF commands")
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

async fn replay_command(cmd: Command, db: &ShardedDatabase) -> Result<(), io::Error> {
    match cmd {
        Command::Set { key, value, expire } => {
            db.set(key, value, expire)
                .await
                .result
                .map_err(command_replay_error)?;
        }
        Command::SetPxAt {
            key,
            value,
            timestamp_ms,
        } => {
            db.set_at_ms(key, value, timestamp_ms)
                .await
                .result
                .map_err(command_replay_error)?;
        }
        Command::Del { keys } => {
            db.del(&keys).await.result.map_err(command_replay_error)?;
        }
        Command::Expire { key, seconds } => {
            db.expire(key, seconds)
                .await
                .result
                .map_err(command_replay_error)?;
        }
        Command::ExpireAt { key, timestamp_ms } => {
            db.expire_at_ms(key, timestamp_ms)
                .await
                .result
                .map_err(command_replay_error)?;
        }
        Command::FlushDb => {
            db.flushdb().await.result.map_err(command_replay_error)?;
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

    use rustkv_core::db::{DbSnapshot, DbSnapshotEntry, ShardedDatabase};

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

        let db = ShardedDatabase::default();
        AofEngine::load_and_replay(path.to_str().expect("test path is valid UTF-8"), &db)
            .await
            .expect("replay AOF");

        let ttl = db.ttl("ttl-key").await.result.expect("read ttl");
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

        let db = ShardedDatabase::default();
        AofEngine::load_and_replay(path.to_str().expect("test path is valid UTF-8"), &db)
            .await
            .expect("replay AOF");

        let value = db.get("expired").await.result.expect("read key");
        assert_eq!(value, None);

        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn aof_rewrite_replaces_history_with_snapshot_entries() {
        let path = temp_aof_path("rewrite-current-state");
        let mut aof = AofEngine::new(path.to_str().expect("test path is valid UTF-8"))
            .await
            .expect("create AOF");

        aof.append(&Command::Set {
            key: String::from("name"),
            value: b"old".to_vec(),
            expire: None,
        })
        .await
        .expect("append old SET");
        aof.append(&Command::Set {
            key: String::from("name"),
            value: b"new".to_vec(),
            expire: None,
        })
        .await
        .expect("append new SET");

        let snapshot = DbSnapshot {
            entries: vec![DbSnapshotEntry {
                key: String::from("name"),
                value: b"new".to_vec(),
                expire_at_ms: None,
            }],
            key_count: 1,
            expired_count: 0,
        };

        aof.rewrite(&snapshot).await.expect("rewrite AOF");
        drop(aof);

        let bytes = tokio::fs::read(&path).await.expect("read rewritten AOF");
        assert_eq!(resp_frame_count(&bytes), 1);

        let db = ShardedDatabase::default();
        AofEngine::load_and_replay(path.to_str().expect("test path is valid UTF-8"), &db)
            .await
            .expect("replay rewritten AOF");

        assert_eq!(
            db.get("name").await.result.expect("read name"),
            Some(b"new".to_vec())
        );

        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn aof_rewrite_preserves_absolute_ttl() {
        let path = temp_aof_path("rewrite-ttl");
        let mut aof = AofEngine::new(path.to_str().expect("test path is valid UTF-8"))
            .await
            .expect("create AOF");
        let expire_at_ms = expire_at_ms_from_now(&Duration::from_secs(5));
        let snapshot = DbSnapshot {
            entries: vec![DbSnapshotEntry {
                key: String::from("ttl-key"),
                value: b"value".to_vec(),
                expire_at_ms: Some(expire_at_ms),
            }],
            key_count: 1,
            expired_count: 0,
        };

        aof.rewrite(&snapshot).await.expect("rewrite AOF");
        drop(aof);

        let db = ShardedDatabase::default();
        AofEngine::load_and_replay(path.to_str().expect("test path is valid UTF-8"), &db)
            .await
            .expect("replay rewritten AOF");

        let ttl = db.ttl("ttl-key").await.result.expect("read ttl");
        assert!(
            (1..=5).contains(&ttl),
            "TTL should survive rewrite as an absolute deadline, got {ttl}"
        );

        let _ = tokio::fs::remove_file(path).await;
    }

    fn resp_frame_count(bytes: &[u8]) -> usize {
        let mut offset = 0;
        let mut count = 0;

        while offset < bytes.len() {
            let (_frame, consumed) = parse_resp(&bytes[offset..]).expect("valid RESP frame");
            offset += consumed;
            count += 1;
        }

        count
    }

    fn temp_aof_path(name: &str) -> PathBuf {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("rustkv-lab-{name}-{id}.aof"))
    }
}
