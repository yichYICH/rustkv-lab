use std::io;
use std::sync::Arc;
use std::time::Duration;

use rustkv_core::command::Command;
use rustkv_core::db::{ShardedDatabase, DEFAULT_SHARD_COUNT};
use rustkv_core::executor::execute_command;
use rustkv_core::stats::ServerStats;
use rustkv_protocol::parser::parse_resp;
use rustkv_protocol::resp::RespValue;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::aof::AofEngine;
use crate::connection::Connection;
use crate::ttl::start_ttl_worker;

pub const DEFAULT_ADDR: &str = "127.0.0.1:6379";
pub const DEFAULT_MAX_FRAME_SIZE: usize = 1024 * 1024;
pub const DEFAULT_TTL_INTERVAL_MS: u64 = 1_000;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub addr: String,
    pub aof_path: Option<String>,
    pub max_frame_size: usize,
    pub ttl_interval: Duration,
    pub shard_count: usize,
}

impl ServerConfig {
    fn validate(&self) -> Result<(), io::Error> {
        if self.addr.trim().is_empty() {
            return Err(invalid_config("server address must not be empty"));
        }

        if self.max_frame_size == 0 {
            return Err(invalid_config("max frame size must be greater than 0"));
        }

        if self.ttl_interval.is_zero() {
            return Err(invalid_config("TTL interval must be greater than 0"));
        }

        if self.shard_count == 0 {
            return Err(invalid_config("shard count must be greater than 0"));
        }

        Ok(())
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: String::from(DEFAULT_ADDR),
            aof_path: None,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            ttl_interval: Duration::from_millis(DEFAULT_TTL_INTERVAL_MS),
            shard_count: DEFAULT_SHARD_COUNT,
        }
    }
}

type SharedDatabase = Arc<ShardedDatabase>;
type SharedStats = Arc<ServerStats>;
type SharedAof = Option<Arc<Mutex<AofEngine>>>;

pub async fn run(config: ServerConfig) -> Result<(), io::Error> {
    config.validate()?;
    let listener = TcpListener::bind(&config.addr).await?;
    let shutdown = shutdown_signal();
    run_with_listener_and_shutdown(listener, config, shutdown).await
}

pub async fn run_with_shutdown(
    config: ServerConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<(), io::Error> {
    config.validate()?;
    let listener = TcpListener::bind(&config.addr).await?;
    run_with_listener_and_shutdown(listener, config, shutdown).await
}

pub async fn run_with_listener(
    listener: TcpListener,
    config: ServerConfig,
) -> Result<(), io::Error> {
    let (shutdown_tx, shutdown) = watch::channel(false);
    let result = run_with_listener_and_shutdown(listener, config, shutdown).await;
    drop(shutdown_tx);
    result
}

pub async fn run_with_listener_and_shutdown(
    listener: TcpListener,
    config: ServerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), io::Error> {
    config.validate()?;

    let db = Arc::new(ShardedDatabase::new(config.shard_count));
    let addr = listener.local_addr()?;

    let aof = if let Some(path) = &config.aof_path {
        AofEngine::load_and_replay(path, db.as_ref()).await?;
        info!(path = %path, "loaded AOF file");
        Some(Arc::new(Mutex::new(AofEngine::new(path).await?)))
    } else {
        None
    };

    let mut stats_value = ServerStats::new();
    stats_value.configure_runtime(addr.to_string(), aof.is_some(), config.max_frame_size);
    let stats = Arc::new(stats_value);

    let ttl_worker = start_ttl_worker(
        db.clone(),
        stats.clone(),
        config.ttl_interval,
        shutdown.clone(),
    );
    let mut client_tasks = JoinSet::new();
    let max_frame_size = config.max_frame_size;

    info!(
        addr = %addr,
        max_frame_size,
        shard_count = db.shard_count(),
        ttl_interval_ms = config.ttl_interval.as_millis(),
        "rustkv server is listening"
    );

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    info!("shutdown signal received; stopping accept loop");
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer_addr) = accepted?;
                info!(peer = %peer_addr, "client connected");

                let db = db.clone();
                let stats = stats.clone();
                let aof = aof.clone();
                let client_shutdown = shutdown.clone();

                client_tasks.spawn(async move {
                    stats.incr_connected_clients();

                    if let Err(error) =
                        handle_client(stream, db, stats.clone(), aof, client_shutdown, max_frame_size).await
                    {
                        warn!(peer = %peer_addr, error = %error, "client task ended with error");
                    }

                    stats.decr_connected_clients();
                    info!(peer = %peer_addr, "client disconnected");
                });
            }
        }
    }

    info!("waiting for active client tasks to finish");
    while let Some(result) = client_tasks.join_next().await {
        if let Err(error) = result {
            warn!(error = %error, "client task failed during shutdown");
        }
    }

    if let Err(error) = ttl_worker.await {
        warn!(error = %error, "TTL worker failed during shutdown");
    }

    if let Some(aof) = &aof {
        aof.lock().await.flush().await?;
        info!("AOF file flushed");
    }

    info!("rustkv server shutdown complete");
    Ok(())
}

async fn handle_client(
    stream: TcpStream,
    db: SharedDatabase,
    stats: SharedStats,
    aof: SharedAof,
    mut shutdown: watch::Receiver<bool>,
    max_frame_size: usize,
) -> Result<(), io::Error> {
    let mut connection = Connection::new(stream, max_frame_size);

    loop {
        let frame = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }

                continue;
            }
            frame = connection.read_value() => frame?,
        };

        let Some(frame) = frame else {
            return Ok(());
        };

        let command = match parse_command(&frame) {
            Ok(command) => command,
            Err(error) => {
                connection.write_value(&error_response(error)).await?;
                continue;
            }
        };

        if is_write_command(&command) {
            if let Some(aof) = &aof {
                if let Err(error) = aof.lock().await.append(&command).await {
                    error!(error = %error, "failed to append command to AOF");
                    connection.write_value(&error_response(error)).await?;
                    continue;
                }
            }
        }

        let response = execute_command(command, db.as_ref(), stats.as_ref()).await;
        connection.write_value(&response).await?;
    }
}

fn invalid_config(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn parse_command(frame: &[u8]) -> Result<Command, String> {
    let (resp, _consumed) =
        parse_resp(frame).map_err(|error| format!("ERR protocol error: {error}"))?;

    Command::from_resp(resp).map_err(|error| format!("ERR {error}"))
}

fn is_write_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Set { .. }
            | Command::SetPxAt { .. }
            | Command::Del { .. }
            | Command::Expire { .. }
            | Command::ExpireAt { .. }
            | Command::FlushDb
    )
}

fn error_response(error: impl ToString) -> RespValue {
    RespValue::Error(error.to_string())
}

fn shutdown_signal() -> watch::Receiver<bool> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("Ctrl+C received; starting graceful shutdown");
                if shutdown_tx.send(true).is_err() {
                    warn!("shutdown signal receiver was dropped before notification");
                }
            }
            Err(error) => {
                error!(error = %error, "failed to listen for Ctrl+C");
            }
        }
    });

    shutdown_rx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_keeps_existing_server_defaults() {
        let config = ServerConfig::default();

        assert_eq!(config.addr, DEFAULT_ADDR);
        assert_eq!(config.aof_path, None);
        assert_eq!(config.max_frame_size, DEFAULT_MAX_FRAME_SIZE);
        assert_eq!(
            config.ttl_interval,
            Duration::from_millis(DEFAULT_TTL_INTERVAL_MS)
        );
        assert_eq!(config.shard_count, DEFAULT_SHARD_COUNT);
    }

    #[test]
    fn config_rejects_zero_sized_runtime_limits() {
        let mut config = ServerConfig {
            max_frame_size: 0,
            ..ServerConfig::default()
        };
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        config = ServerConfig {
            ttl_interval: Duration::from_millis(0),
            ..ServerConfig::default()
        };
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        config = ServerConfig {
            shard_count: 0,
            ..ServerConfig::default()
        };
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
