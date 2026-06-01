use std::error::Error;
use std::io;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use rustkv_protocol::encoder::encode_resp;
use rustkv_protocol::parser::parse_resp;
use rustkv_protocol::resp::{RespFrame, RespValue};
use rustkv_protocol::ProtocolError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinSet;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Parser)]
#[command(name = "rustkv-bench")]
#[command(about = "Simple throughput benchmark for rustkv-server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:6379")]
    addr: String,

    #[arg(long, default_value_t = 1000)]
    requests: usize,

    #[arg(long, value_enum, default_value = "set")]
    command: BenchCommand,

    #[arg(long, default_value_t = 1)]
    clients: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BenchCommand {
    Set,
    Get,
    Mixed,
}

impl BenchCommand {
    fn as_str(self) -> &'static str {
        match self {
            Self::Set => "set",
            Self::Get => "get",
            Self::Mixed => "mixed",
        }
    }
}

#[derive(Debug, Default)]
struct WorkerStats {
    completed: usize,
    total_latency_nanos: u128,
}

#[tokio::main]
async fn main() -> BenchResult<()> {
    let args = Args::parse();
    validate_args(&args)?;

    let client_count = args.clients.min(args.requests);

    if matches!(args.command, BenchCommand::Get) {
        prepare_get_keys(&args.addr, args.requests, client_count).await?;
    }

    let started_at = Instant::now();
    let mut workers = JoinSet::new();

    for client_id in 0..client_count {
        let addr = args.addr.clone();
        let command = args.command;
        let request_count = requests_for_client(args.requests, client_count, client_id);

        if request_count == 0 {
            continue;
        }

        workers.spawn(async move { run_worker(addr, command, client_id, request_count).await });
    }

    let mut aggregate = WorkerStats::default();

    while let Some(result) = workers.join_next().await {
        let worker_stats =
            result.map_err(|error| format!("benchmark worker failed: {error}"))??;
        aggregate.completed += worker_stats.completed;
        aggregate.total_latency_nanos += worker_stats.total_latency_nanos;
    }

    let elapsed = started_at.elapsed();
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let qps = if elapsed.as_secs_f64() > 0.0 {
        aggregate.completed as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    let avg_latency_ms = if aggregate.completed > 0 {
        aggregate.total_latency_nanos as f64 / aggregate.completed as f64 / 1_000_000.0
    } else {
        0.0
    };

    println!("rustkv-bench result");
    println!("addr: {}", args.addr);
    println!("command: {}", args.command.as_str());
    println!("clients: {client_count}");
    println!("total_requests: {}", aggregate.completed);
    println!("total_elapsed_ms: {elapsed_ms:.2}");
    println!("avg_latency_ms: {avg_latency_ms:.4}");
    println!("qps: {qps:.2}");

    Ok(())
}

fn validate_args(args: &Args) -> BenchResult<()> {
    if args.requests == 0 {
        return Err(String::from("--requests must be greater than 0").into());
    }

    if args.clients == 0 {
        return Err(String::from("--clients must be greater than 0").into());
    }

    Ok(())
}

async fn prepare_get_keys(
    addr: &str,
    total_requests: usize,
    client_count: usize,
) -> BenchResult<()> {
    let mut connection = BenchConnection::connect(addr).await?;

    for client_id in 0..client_count {
        let request_count = requests_for_client(total_requests, client_count, client_id);

        for sequence in 0..request_count {
            let args = set_args("bench:get", client_id, sequence);
            connection.send_command(args).await?;
        }
    }

    Ok(())
}

async fn run_worker(
    addr: String,
    command: BenchCommand,
    client_id: usize,
    request_count: usize,
) -> BenchResult<WorkerStats> {
    let mut connection = BenchConnection::connect(&addr).await?;
    let mut stats = WorkerStats::default();

    for sequence in 0..request_count {
        let args = command_args(command, client_id, sequence);
        let started_at = Instant::now();

        connection.send_command(args).await?;

        stats.completed += 1;
        stats.total_latency_nanos += started_at.elapsed().as_nanos();
    }

    Ok(stats)
}

fn requests_for_client(total_requests: usize, client_count: usize, client_id: usize) -> usize {
    let base = total_requests / client_count;
    let remainder = total_requests % client_count;

    if client_id < remainder {
        base + 1
    } else {
        base
    }
}

fn command_args(command: BenchCommand, client_id: usize, sequence: usize) -> Vec<Vec<u8>> {
    match command {
        BenchCommand::Set => set_args("bench:set", client_id, sequence),
        BenchCommand::Get => get_args("bench:get", client_id, sequence),
        BenchCommand::Mixed => {
            if sequence.is_multiple_of(2) {
                set_args("bench:mixed", client_id, sequence)
            } else {
                get_args("bench:mixed", client_id, sequence - 1)
            }
        }
    }
}

fn set_args(prefix: &str, client_id: usize, sequence: usize) -> Vec<Vec<u8>> {
    vec![
        b"SET".to_vec(),
        key(prefix, client_id, sequence).into_bytes(),
        value(client_id, sequence).into_bytes(),
    ]
}

fn get_args(prefix: &str, client_id: usize, sequence: usize) -> Vec<Vec<u8>> {
    vec![
        b"GET".to_vec(),
        key(prefix, client_id, sequence).into_bytes(),
    ]
}

fn key(prefix: &str, client_id: usize, sequence: usize) -> String {
    format!("{prefix}:{client_id}:{sequence}")
}

fn value(client_id: usize, sequence: usize) -> String {
    format!("value-{client_id}-{sequence}")
}

fn encode_command(args: Vec<Vec<u8>>) -> Vec<u8> {
    let values = args.into_iter().map(RespValue::BulkString).collect();
    encode_resp(&RespValue::Array(values))
}

struct BenchConnection {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl BenchConnection {
    async fn connect(addr: &str) -> BenchResult<Self> {
        let stream = TcpStream::connect(addr).await?;

        Ok(Self {
            stream,
            buffer: Vec::with_capacity(4096),
        })
    }

    async fn send_command(&mut self, args: Vec<Vec<u8>>) -> BenchResult<()> {
        let bytes = encode_command(args);
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;
        self.read_response().await
    }

    async fn read_response(&mut self) -> BenchResult<()> {
        let mut chunk = [0_u8; 4096];

        loop {
            let parsed = match parse_resp(&self.buffer) {
                Ok((frame, consumed)) => {
                    let server_error = match frame {
                        RespFrame::Error(message) => Some(message.to_owned()),
                        _ => None,
                    };
                    Some((consumed, server_error))
                }
                Err(ProtocolError::Incomplete) => None,
                Err(error) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid RESP response: {error}"),
                    )
                    .into());
                }
            };

            if let Some((consumed, server_error)) = parsed {
                self.buffer.drain(..consumed);

                if let Some(message) = server_error {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, message).into());
                }

                return Ok(());
            }

            let bytes_read = self.stream.read(&mut chunk).await?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "server closed before sending a complete response",
                )
                .into());
            }

            self.buffer.extend_from_slice(&chunk[..bytes_read]);
        }
    }
}
