use std::time::{Duration, Instant};

use rustkv_protocol::encoder::encode_resp;
use rustkv_protocol::parser::parse_resp;
use rustkv_protocol::resp::{RespFrame, RespValue};
use rustkv_protocol::ProtocolError;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::timeout;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connecting,
    Online,
    Offline,
}

impl ConnectionStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Connecting => "CONNECTING",
            Self::Online => "ONLINE",
            Self::Offline => "OFFLINE",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Metrics {
    pub server_version: String,
    pub role: String,
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
    pub uptime_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct MonitorState {
    pub addr: String,
    pub metrics: Metrics,
    pub status: ConnectionStatus,
    pub qps: f64,
    pub last_error: Option<String>,
    pub last_updated: Option<Instant>,
}

impl MonitorState {
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            metrics: Metrics::default(),
            status: ConnectionStatus::Connecting,
            qps: 0.0,
            last_error: None,
            last_updated: None,
        }
    }

    pub fn uptime_text(&self) -> String {
        format_duration(self.metrics.uptime_seconds)
    }

    pub fn last_updated_text(&self) -> String {
        match self.last_updated {
            Some(updated_at) => {
                let elapsed = updated_at.elapsed().as_secs();
                if elapsed == 0 {
                    String::from("just now")
                } else {
                    format!("{elapsed}s ago")
                }
            }
            None => String::from("never"),
        }
    }

    pub fn health_percent(&self) -> u16 {
        match self.status {
            ConnectionStatus::Online => 100,
            ConnectionStatus::Connecting => 50,
            ConnectionStatus::Offline => 0,
        }
    }
}

pub struct EventLoop {
    addr: String,
    state: MonitorState,
    tx: watch::Sender<MonitorState>,
    previous_total_commands: Option<u64>,
    previous_sample_at: Option<Instant>,
}

impl EventLoop {
    pub fn new(addr: String) -> (Self, watch::Receiver<MonitorState>) {
        let state = MonitorState::new(addr.clone());
        let (tx, rx) = watch::channel(state.clone());

        (
            Self {
                addr,
                state,
                tx,
                previous_total_commands: None,
                previous_sample_at: None,
            },
            rx,
        )
    }

    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;
            self.refresh_once().await;
            let _ = self.tx.send(self.state.clone());
        }
    }

    async fn refresh_once(&mut self) {
        match fetch_metrics(&self.addr).await {
            Ok(metrics) => {
                let now = Instant::now();
                self.state.qps = self.calculate_qps(metrics.total_commands, now);
                self.state.metrics = metrics;
                self.state.status = ConnectionStatus::Online;
                self.state.last_error = None;
                self.state.last_updated = Some(now);
            }
            Err(error) => {
                self.state.status = ConnectionStatus::Offline;
                self.state.last_error = Some(error);
            }
        }
    }

    fn calculate_qps(&mut self, total_commands: u64, now: Instant) -> f64 {
        let qps = match (self.previous_total_commands, self.previous_sample_at) {
            (Some(previous_total), Some(previous_sample_at)) => {
                let elapsed = now.duration_since(previous_sample_at).as_secs_f64();
                if elapsed > 0.0 {
                    total_commands.saturating_sub(previous_total) as f64 / elapsed
                } else {
                    0.0
                }
            }
            _ => 0.0,
        };

        self.previous_total_commands = Some(total_commands);
        self.previous_sample_at = Some(now);
        qps
    }
}

async fn fetch_metrics(addr: &str) -> Result<Metrics, String> {
    let mut stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .map_err(|_| String::from("connection timed out"))?
        .map_err(|error| format!("connect failed: {error}"))?;

    let request = encode_info_command();
    stream
        .write_all(&request)
        .await
        .map_err(|error| format!("write failed: {error}"))?;
    stream
        .flush()
        .await
        .map_err(|error| format!("flush failed: {error}"))?;

    let response = read_response(&mut stream).await?;
    let (value, _consumed) =
        parse_resp(&response).map_err(|error| format!("parse failed: {error}"))?;

    match value {
        RespFrame::BulkString(bytes) => parse_metrics(bytes),
        RespFrame::SimpleString(text) => parse_metrics(text.as_bytes()),
        RespFrame::Error(text) => Err(format!("server returned error: {text}")),
        other => Err(format!("unexpected INFO response: {other:?}")),
    }
}

fn encode_info_command() -> Vec<u8> {
    let args = [b"INFO".to_vec()];
    let values = args
        .iter()
        .map(|arg| RespValue::BulkString(arg.clone()))
        .collect::<Vec<_>>();

    encode_resp(&RespValue::Array(values))
}

async fn read_response(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 4096];

    loop {
        if is_complete_response(&buffer)? {
            return Ok(buffer);
        }

        let bytes_read = timeout(Duration::from_secs(2), stream.read(&mut chunk))
            .await
            .map_err(|_| String::from("read timed out"))?
            .map_err(|error| format!("read failed: {error}"))?;

        if bytes_read == 0 {
            return Err(String::from(
                "server closed connection before a complete response arrived",
            ));
        }

        buffer.extend_from_slice(&chunk[..bytes_read]);
    }
}

fn is_complete_response(buffer: &[u8]) -> Result<bool, String> {
    match parse_resp(buffer) {
        Ok((_value, _consumed)) => Ok(true),
        Err(ProtocolError::Incomplete) => Ok(false),
        Err(error) => Err(format!("protocol error: {error}")),
    }
}

fn parse_metrics(bytes: &[u8]) -> Result<Metrics, String> {
    let value = serde_json::from_slice::<Value>(bytes)
        .map_err(|error| format!("invalid INFO json: {error}"))?;

    Ok(Metrics {
        server_version: field_string(&value, "server_version"),
        role: field_string(&value, "role"),
        memory_estimate_bytes: field_u64(&value, "memory_estimate_bytes"),
        aof_enabled: field_bool(&value, "aof_enabled"),
        addr: field_string(&value, "addr"),
        max_frame_size: field_u64(&value, "max_frame_size"),
        total_commands: field_u64(&value, "total_commands"),
        connected_clients: field_u64(&value, "connected_clients"),
        key_count: field_u64(&value, "key_count"),
        expired_keys: field_u64(&value, "expired_keys"),
        get_count: field_u64(&value, "get_count"),
        set_count: field_u64(&value, "set_count"),
        del_count: field_u64(&value, "del_count"),
        uptime_seconds: field_u64(&value, "uptime_seconds"),
    })
}

fn field_u64(value: &Value, name: &str) -> u64 {
    value.get(name).and_then(Value::as_u64).unwrap_or_default()
}

fn field_bool(value: &Value, name: &str) -> bool {
    value.get(name).and_then(Value::as_bool).unwrap_or_default()
}

fn field_string(value: &Value, name: &str) -> String {
    value
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn format_duration(seconds: u64) -> String {
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;

    if days > 0 {
        format!("{days}d {hours:02}h {minutes:02}m {seconds:02}s")
    } else {
        format!("{hours:02}h {minutes:02}m {seconds:02}s")
    }
}
