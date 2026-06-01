use std::error::Error;
use std::str;

mod tui;

use clap::{Parser, Subcommand};
use rustkv_protocol::encoder::encode_resp;
use rustkv_protocol::parser::parse_resp;
use rustkv_protocol::resp::{RespFrame, RespValue};
use rustkv_protocol::ProtocolError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Parser)]
#[command(name = "rustkv-cli")]
#[command(about = "Command line client for rustkv")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:6379")]
    addr: String,
    #[command(subcommand)]
    command: ClientCommand,
}

#[derive(Debug, Subcommand)]
enum ClientCommand {
    Ping,
    Set {
        key: String,
        value: String,
        #[arg(long, conflicts_with = "px")]
        ex: Option<u64>,
        #[arg(long, conflicts_with = "ex")]
        px: Option<u64>,
    },
    Get {
        key: String,
    },
    Del {
        #[arg(required = true)]
        keys: Vec<String>,
    },
    Exists {
        key: String,
    },
    Keys,
    Expire {
        key: String,
        seconds: u64,
    },
    Ttl {
        key: String,
    },
    Flushdb,
    Info,
    Shell,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    match cli.command {
        ClientCommand::Shell => tui::run_shell(cli.addr).await,
        command => run_one_shot(cli.addr, command).await,
    }
}

async fn run_one_shot(addr: String, command: ClientCommand) -> Result<(), Box<dyn Error>> {
    let args = command.into_args()?;
    let request = encode_command(&args);

    let mut stream = TcpStream::connect(&addr).await?;
    stream.write_all(&request).await?;
    stream.flush().await?;

    let response_bytes = read_response(&mut stream).await?;
    let (response, _consumed) = parse_resp(&response_bytes)?;

    print_resp(&response, 0);
    Ok(())
}

impl ClientCommand {
    fn into_args(self) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
        let args = match self {
            Self::Ping => vec![b"PING".to_vec()],
            Self::Set { key, value, ex, px } => {
                let mut args = vec![b"SET".to_vec(), key.into_bytes(), value.into_bytes()];

                if let Some(seconds) = ex {
                    args.push(b"EX".to_vec());
                    args.push(seconds.to_string().into_bytes());
                }

                if let Some(milliseconds) = px {
                    args.push(b"PX".to_vec());
                    args.push(milliseconds.to_string().into_bytes());
                }

                args
            }
            Self::Get { key } => vec![b"GET".to_vec(), key.into_bytes()],
            Self::Del { keys } => {
                let mut args = Vec::with_capacity(keys.len() + 1);
                args.push(b"DEL".to_vec());
                args.extend(keys.into_iter().map(String::into_bytes));
                args
            }
            Self::Exists { key } => vec![b"EXISTS".to_vec(), key.into_bytes()],
            Self::Keys => vec![b"KEYS".to_vec()],
            Self::Expire { key, seconds } => {
                vec![
                    b"EXPIRE".to_vec(),
                    key.into_bytes(),
                    seconds.to_string().into_bytes(),
                ]
            }
            Self::Ttl { key } => vec![b"TTL".to_vec(), key.into_bytes()],
            Self::Flushdb => vec![b"FLUSHDB".to_vec()],
            Self::Info => vec![b"INFO".to_vec()],
            Self::Shell => {
                return Err(
                    "shell mode is interactive and cannot be encoded as one request".into(),
                );
            }
        };

        Ok(args)
    }
}

pub(crate) fn encode_command(args: &[Vec<u8>]) -> Vec<u8> {
    let values = args
        .iter()
        .map(|arg| RespValue::BulkString(arg.clone()))
        .collect::<Vec<_>>();

    encode_resp(&RespValue::Array(values))
}

async fn read_response(stream: &mut TcpStream) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 4096];

    loop {
        if is_complete_response(&buffer)? {
            return Ok(buffer);
        }

        let bytes_read = stream.read(&mut chunk).await?;
        if bytes_read == 0 {
            return Err("server closed connection before sending a complete RESP response".into());
        }

        buffer.extend_from_slice(&chunk[..bytes_read]);
    }
}

fn is_complete_response(buffer: &[u8]) -> Result<bool, Box<dyn Error>> {
    match parse_resp(buffer) {
        Ok((_value, _consumed)) => Ok(true),
        Err(ProtocolError::Incomplete) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn print_resp(value: &RespFrame<'_>, indent: usize) {
    for line in format_resp_lines(value, indent) {
        println!("{line}");
    }
}

pub(crate) fn format_resp(value: &RespFrame<'_>) -> String {
    format_resp_lines(value, 0).join("\n")
}

fn format_resp_lines(value: &RespFrame<'_>, indent: usize) -> Vec<String> {
    let prefix = "  ".repeat(indent);

    match value {
        RespFrame::SimpleString(text) => vec![format!("{prefix}{text}")],
        RespFrame::Error(text) => vec![format!("{prefix}(error) {text}")],
        RespFrame::Integer(number) => vec![format!("{prefix}(integer) {number}")],
        RespFrame::BulkString(bytes) => vec![format!("{prefix}{}", format_bytes(bytes))],
        RespFrame::Array(values) => {
            let mut lines = Vec::new();
            for (index, value) in values.iter().enumerate() {
                lines.push(format!("{prefix}{})", index + 1));
                lines.extend(format_resp_lines(value, indent + 1));
            }
            lines
        }
        RespFrame::Null => vec![format!("{prefix}(nil)")],
    }
}

fn format_bytes(bytes: &[u8]) -> String {
    match str::from_utf8(bytes) {
        Ok(text) if is_terminal_text(text) => text.to_owned(),
        _ => {
            let hex = bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            format!("0x{hex}")
        }
    }
}

fn is_terminal_text(text: &str) -> bool {
    text.chars()
        .all(|ch| !ch.is_control() || matches!(ch, '\n' | '\r' | '\t'))
}
