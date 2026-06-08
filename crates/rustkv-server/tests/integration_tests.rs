use std::error::Error;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustkv_protocol::encoder::encode_resp;
use rustkv_protocol::parser::parse_resp;
use rustkv_protocol::resp::{RespFrame, RespValue};
use rustkv_protocol::ProtocolError;
use rustkv_server::server::{ServerConfig, DEFAULT_MAX_FRAME_SIZE};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const TEST_MAX_FRAME_SIZE: usize = DEFAULT_MAX_FRAME_SIZE;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_returns_pong() -> TestResult<()> {
    let (addr, server_task) = spawn_test_server().await?;

    let pong = send_command(addr, &[b"PING".as_slice()]).await?;
    assert_eq!(pong, RespValue::SimpleString(String::from("PONG")));

    server_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_then_get_returns_value() -> TestResult<()> {
    let (addr, server_task) = spawn_test_server().await?;

    let set = send_command(
        addr,
        &[b"SET".as_slice(), b"name".as_slice(), b"rust".as_slice()],
    )
    .await?;
    assert_eq!(set, RespValue::SimpleString(String::from("OK")));

    let get = send_command(addr, &[b"GET".as_slice(), b"name".as_slice()]).await?;
    assert_eq!(get, RespValue::BulkString(b"rust".to_vec()));

    server_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expire_then_get_after_sleep_returns_null() -> TestResult<()> {
    let (addr, server_task) = spawn_test_server().await?;

    let set = send_command(
        addr,
        &[
            b"SET".as_slice(),
            b"short_lived".as_slice(),
            b"value".as_slice(),
            b"EX".as_slice(),
            b"1".as_slice(),
        ],
    )
    .await?;
    assert_eq!(set, RespValue::SimpleString(String::from("OK")));

    let ttl = send_command(addr, &[b"TTL".as_slice(), b"short_lived".as_slice()]).await?;
    match ttl {
        RespValue::Integer(value) => assert!(value > 0, "expected TTL > 0, got {value}"),
        other => panic!("expected integer TTL response, got {other:?}"),
    }

    tokio::time::sleep(Duration::from_millis(1500)).await;

    let get = send_command(addr, &[b"GET".as_slice(), b"short_lived".as_slice()]).await?;
    assert_eq!(get, RespValue::Null);

    server_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ttl_worker_updates_info_stats_after_cleanup() -> TestResult<()> {
    let (addr, server_task) = spawn_test_server().await?;

    let set = send_command(
        addr,
        &[
            b"SET".as_slice(),
            b"stats_ttl".as_slice(),
            b"value".as_slice(),
            b"EX".as_slice(),
            b"1".as_slice(),
        ],
    )
    .await?;
    assert_eq!(set, RespValue::SimpleString(String::from("OK")));

    tokio::time::sleep(Duration::from_millis(2_200)).await;

    let info = send_command(addr, &[b"INFO".as_slice()]).await?;
    let RespValue::BulkString(bytes) = info else {
        panic!("expected INFO bulk string response");
    };
    let text = std::str::from_utf8(&bytes)?;

    assert!(
        text.contains("\"expired_keys\":1"),
        "expected expired_keys to be updated after TTL worker cleanup, got {text}"
    );
    assert!(
        text.contains("\"key_count\":0"),
        "expected key_count to be synchronized after TTL worker cleanup, got {text}"
    );

    server_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn info_returns_json_with_runtime_fields() -> TestResult<()> {
    let aof_path = temp_aof_path()?;
    let (addr, server_task) =
        spawn_test_server_with_aof(Some(aof_path.to_string_lossy().to_string())).await?;

    let set = send_command(
        addr,
        &[
            b"SET".as_slice(),
            b"info_key".as_slice(),
            b"value".as_slice(),
        ],
    )
    .await?;
    assert_eq!(set, RespValue::SimpleString(String::from("OK")));

    let info = send_command(addr, &[b"INFO".as_slice()]).await?;
    let RespValue::BulkString(bytes) = info else {
        panic!("expected INFO bulk string response");
    };

    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    let addr_text = addr.to_string();

    assert!(value["server_version"]
        .as_str()
        .is_some_and(|text| !text.is_empty()));
    assert_eq!(value["role"], "standalone");
    assert_eq!(value["aof_enabled"], true);
    assert_eq!(value["addr"].as_str(), Some(addr_text.as_str()));
    assert_eq!(value["max_frame_size"], TEST_MAX_FRAME_SIZE as u64);
    assert_eq!(value["key_count"], 1);
    assert!(value["memory_estimate_bytes"]
        .as_u64()
        .is_some_and(|bytes| bytes > 0));
    assert!(value["uptime_seconds"].as_u64().is_some());
    assert_eq!(value["total_commands"], 1);
    assert!(value["connected_clients"]
        .as_u64()
        .is_some_and(|count| count >= 1));
    assert!(value["expired_keys"].as_u64().is_some());
    assert_eq!(value["set_count"], 1);
    assert_eq!(value["del_count"], 0);

    server_task.abort();
    let _ = tokio::fs::remove_file(aof_path).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aof_reload_restores_written_key() -> TestResult<()> {
    let aof_path = temp_aof_path()?;
    let aof_path_text = aof_path.to_string_lossy().to_string();

    let (addr, shutdown, server_task) =
        spawn_stoppable_test_server_with_aof(aof_path_text.clone()).await?;

    let set = send_command(
        addr,
        &[
            b"SET".as_slice(),
            b"persistent".as_slice(),
            b"hello".as_slice(),
        ],
    )
    .await?;
    assert_eq!(set, RespValue::SimpleString(String::from("OK")));

    shutdown.send(true)?;
    tokio::time::timeout(Duration::from_secs(2), server_task).await???;

    let (addr, shutdown, server_task) = spawn_stoppable_test_server_with_aof(aof_path_text).await?;

    let get = send_command(addr, &[b"GET".as_slice(), b"persistent".as_slice()]).await?;
    assert_eq!(get, RespValue::BulkString(b"hello".to_vec()));

    shutdown.send(true)?;
    tokio::time::timeout(Duration::from_secs(2), server_task).await???;

    let _ = tokio::fs::remove_file(aof_path).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aof_background_rewrite_compacts_and_restores_final_state() -> TestResult<()> {
    let aof_path = temp_aof_path()?;
    let aof_path_text = aof_path.to_string_lossy().to_string();

    let rewrite_config = ServerConfig {
        aof_path: Some(aof_path_text.clone()),
        aof_rewrite_interval: Duration::from_millis(50),
        aof_rewrite_min_size: 1,
        ..ServerConfig::default()
    };
    let (addr, shutdown, server_task) =
        spawn_stoppable_test_server_with_config(rewrite_config).await?;

    assert_eq!(
        send_command(
            addr,
            &[b"SET".as_slice(), b"name".as_slice(), b"old".as_slice()]
        )
        .await?,
        RespValue::SimpleString(String::from("OK"))
    );
    assert_eq!(
        send_command(
            addr,
            &[b"SET".as_slice(), b"name".as_slice(), b"new".as_slice()]
        )
        .await?,
        RespValue::SimpleString(String::from("OK"))
    );
    assert_eq!(
        send_command(
            addr,
            &[
                b"SET".as_slice(),
                b"deleted".as_slice(),
                b"value".as_slice()
            ]
        )
        .await?,
        RespValue::SimpleString(String::from("OK"))
    );
    assert_eq!(
        send_command(addr, &[b"DEL".as_slice(), b"deleted".as_slice()]).await?,
        RespValue::Integer(1)
    );

    wait_for_aof_frame_count(&aof_path, 1).await?;

    shutdown.send(true)?;
    tokio::time::timeout(Duration::from_secs(2), server_task).await???;

    let (addr, shutdown, server_task) = spawn_stoppable_test_server_with_aof(aof_path_text).await?;

    assert_eq!(
        send_command(addr, &[b"GET".as_slice(), b"name".as_slice()]).await?,
        RespValue::BulkString(b"new".to_vec())
    );
    assert_eq!(
        send_command(addr, &[b"GET".as_slice(), b"deleted".as_slice()]).await?,
        RespValue::Null
    );

    shutdown.send(true)?;
    tokio::time::timeout(Duration::from_secs(2), server_task).await???;

    let _ = tokio::fs::remove_file(aof_path).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aof_background_rewrite_keeps_later_successful_writes() -> TestResult<()> {
    let aof_path = temp_aof_path()?;
    let aof_path_text = aof_path.to_string_lossy().to_string();

    let rewrite_config = ServerConfig {
        aof_path: Some(aof_path_text.clone()),
        aof_rewrite_interval: Duration::from_millis(20),
        aof_rewrite_min_size: 1,
        ..ServerConfig::default()
    };
    let (addr, shutdown, server_task) =
        spawn_stoppable_test_server_with_config(rewrite_config).await?;

    for value in 0..20 {
        let text = value.to_string();
        assert_eq!(
            send_command(
                addr,
                &[b"SET".as_slice(), b"counter".as_slice(), text.as_bytes()]
            )
            .await?,
            RespValue::SimpleString(String::from("OK"))
        );
    }

    assert_eq!(
        send_command(
            addr,
            &[
                b"SET".as_slice(),
                b"counter".as_slice(),
                b"final".as_slice()
            ]
        )
        .await?,
        RespValue::SimpleString(String::from("OK"))
    );

    wait_for_aof_frame_count(&aof_path, 1).await?;

    shutdown.send(true)?;
    tokio::time::timeout(Duration::from_secs(2), server_task).await???;

    let (addr, shutdown, server_task) = spawn_stoppable_test_server_with_aof(aof_path_text).await?;

    assert_eq!(
        send_command(addr, &[b"GET".as_slice(), b"counter".as_slice()]).await?,
        RespValue::BulkString(b"final".to_vec())
    );

    shutdown.send(true)?;
    tokio::time::timeout(Duration::from_secs(2), server_task).await???;

    let _ = tokio::fs::remove_file(aof_path).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_incomplete_frame_is_disconnected() -> TestResult<()> {
    let (addr, server_task) = spawn_test_server().await?;
    let mut stream = TcpStream::connect(addr).await?;

    let mut request = Vec::with_capacity(TEST_MAX_FRAME_SIZE + 2);
    request.push(b'+');
    request.extend(std::iter::repeat_n(b'a', TEST_MAX_FRAME_SIZE + 1));
    let write_result = stream.write_all(&request).await;
    if let Err(error) = write_result {
        if matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe
        ) {
            server_task.abort();
            return Ok(());
        }

        return Err(error.into());
    }
    let flush_result = stream.flush().await;
    if let Err(error) = flush_result {
        if matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe
        ) {
            server_task.abort();
            return Ok(());
        }

        return Err(error.into());
    }

    let mut buffer = [0_u8; 1];
    let read_result = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buffer)).await;

    match read_result {
        Ok(Ok(0)) => {}
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::UnexpectedEof
            ) => {}
        Ok(Ok(n)) => panic!("expected oversized frame connection to close, read {n} bytes"),
        Ok(Err(error)) => return Err(error.into()),
        Err(_) => return Err("server did not close oversized incomplete frame in time".into()),
    }

    server_task.abort();
    Ok(())
}

async fn spawn_test_server() -> TestResult<(SocketAddr, JoinHandle<()>)> {
    spawn_test_server_with_aof(None).await
}

async fn spawn_test_server_with_aof(
    aof_path: Option<String>,
) -> TestResult<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server_task = tokio::spawn(async move {
        let config = ServerConfig {
            aof_path,
            ..ServerConfig::default()
        };

        if let Err(error) = rustkv_server::server::run_with_listener(listener, config).await {
            eprintln!("test server stopped with error: {error}");
        }
    });

    Ok((addr, server_task))
}

async fn spawn_stoppable_test_server_with_aof(
    aof_path: String,
) -> TestResult<(
    SocketAddr,
    watch::Sender<bool>,
    JoinHandle<Result<(), std::io::Error>>,
)> {
    let config = ServerConfig {
        aof_path: Some(aof_path),
        ..ServerConfig::default()
    };

    spawn_stoppable_test_server_with_config(config).await
}

async fn spawn_stoppable_test_server_with_config(
    config: ServerConfig,
) -> TestResult<(
    SocketAddr,
    watch::Sender<bool>,
    JoinHandle<Result<(), std::io::Error>>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let server_task = tokio::spawn(async move {
        rustkv_server::server::run_with_listener_and_shutdown(listener, config, shutdown_rx).await
    });

    Ok((addr, shutdown_tx, server_task))
}

fn temp_aof_path() -> TestResult<std::path::PathBuf> {
    let id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(std::env::temp_dir().join(format!("rustkv-lab-info-{id}.aof")))
}

async fn wait_for_aof_frame_count(path: &std::path::Path, expected: usize) -> TestResult<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);

    loop {
        if let Ok(bytes) = tokio::fs::read(path).await {
            if aof_frame_count(&bytes)? == expected {
                return Ok(());
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(format!("AOF did not reach {expected} frame(s) in time").into());
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn aof_frame_count(bytes: &[u8]) -> TestResult<usize> {
    let mut offset = 0;
    let mut count = 0;

    while offset < bytes.len() {
        let (_frame, consumed) = parse_resp(&bytes[offset..])?;
        offset += consumed;
        count += 1;
    }

    Ok(count)
}

async fn send_command(addr: SocketAddr, args: &[&[u8]]) -> TestResult<RespValue> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = encode_command(args);

    stream.write_all(&request).await?;
    stream.flush().await?;

    let response = read_response(&mut stream).await?;
    let (frame, _consumed) = parse_resp(&response)?;

    Ok(frame_to_value(frame))
}

fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let values = args
        .iter()
        .map(|arg| RespValue::BulkString(arg.to_vec()))
        .collect::<Vec<_>>();

    encode_resp(&RespValue::Array(values))
}

async fn read_response(stream: &mut TcpStream) -> TestResult<Vec<u8>> {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 4096];

    loop {
        if is_complete_response(&buffer)? {
            return Ok(buffer);
        }

        let bytes_read = stream.read(&mut chunk).await?;
        if bytes_read == 0 {
            return Err("server closed before sending a complete RESP response".into());
        }

        buffer.extend_from_slice(&chunk[..bytes_read]);
    }
}

fn is_complete_response(buffer: &[u8]) -> TestResult<bool> {
    match parse_resp(buffer) {
        Ok((_value, _consumed)) => Ok(true),
        Err(ProtocolError::Incomplete) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn frame_to_value(frame: RespFrame<'_>) -> RespValue {
    match frame {
        RespFrame::SimpleString(text) => RespValue::SimpleString(text.to_owned()),
        RespFrame::Error(text) => RespValue::Error(text.to_owned()),
        RespFrame::Integer(number) => RespValue::Integer(number),
        RespFrame::BulkString(bytes) => RespValue::BulkString(bytes.to_vec()),
        RespFrame::Array(values) => {
            RespValue::Array(values.into_iter().map(frame_to_value).collect())
        }
        RespFrame::Null => RespValue::Null,
    }
}
