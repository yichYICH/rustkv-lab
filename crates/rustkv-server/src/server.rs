use std::io;
use std::sync::Arc;

use rustkv_core::command::Command;
use rustkv_core::db::Database;
use rustkv_core::executor::execute_command;
use rustkv_core::stats::ServerStats;
use rustkv_protocol::parser::parse_resp;
use rustkv_protocol::resp::RespValue;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Mutex, RwLock};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::aof::AofEngine;
use crate::connection::{Connection, MAX_FRAME_SIZE};
use crate::ttl::start_ttl_worker;

type SharedDatabase = Arc<RwLock<Database>>;
type SharedStats = Arc<RwLock<ServerStats>>;
type SharedAof = Option<Arc<Mutex<AofEngine>>>;

pub async fn run(addr: &str, aof_path: Option<String>) -> Result<(), io::Error> {
    let listener = TcpListener::bind(addr).await?;
    let shutdown = shutdown_signal();
    run_with_listener_and_shutdown(listener, aof_path, shutdown).await
}

pub async fn run_with_shutdown(
    addr: &str,
    aof_path: Option<String>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), io::Error> {
    let listener = TcpListener::bind(addr).await?;
    run_with_listener_and_shutdown(listener, aof_path, shutdown).await
}

pub async fn run_with_listener(
    listener: TcpListener,
    aof_path: Option<String>,
) -> Result<(), io::Error> {
    let (shutdown_tx, shutdown) = watch::channel(false);
    let result = run_with_listener_and_shutdown(listener, aof_path, shutdown).await;
    drop(shutdown_tx);
    result
}

pub async fn run_with_listener_and_shutdown(
    listener: TcpListener,
    aof_path: Option<String>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), io::Error> {
    let db = Arc::new(RwLock::new(Database::new()));
    let stats = Arc::new(RwLock::new(ServerStats::new()));
    let addr = listener.local_addr()?;

    let aof = if let Some(path) = aof_path {
        AofEngine::load_and_replay(&path, db.as_ref()).await?;
        info!(path = %path, "loaded AOF file");
        Some(Arc::new(Mutex::new(AofEngine::new(&path).await?)))
    } else {
        None
    };

    {
        let mut stats_guard = stats.write().await;
        stats_guard.configure_runtime(addr.to_string(), aof.is_some(), MAX_FRAME_SIZE);
    }

    let ttl_worker = start_ttl_worker(db.clone(), stats.clone(), shutdown.clone());
    let mut client_tasks = JoinSet::new();

    info!(addr = %addr, "rustkv server is listening");

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
                    stats.read().await.incr_connected_clients();

                    if let Err(error) =
                        handle_client(stream, db, stats.clone(), aof, client_shutdown).await
                    {
                        warn!(peer = %peer_addr, error = %error, "client task ended with error");
                    }

                    stats.read().await.decr_connected_clients();
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
) -> Result<(), io::Error> {
    let mut connection = Connection::new(stream);

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
