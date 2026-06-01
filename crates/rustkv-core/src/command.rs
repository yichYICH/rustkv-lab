use std::str;
use std::time::Duration;

use rustkv_protocol::resp::RespFrame;

use crate::error::KvError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping,
    Set {
        key: String,
        value: Vec<u8>,
        expire: Option<Duration>,
    },
    SetPxAt {
        key: String,
        value: Vec<u8>,
        timestamp_ms: u64,
    },
    Get {
        key: String,
    },
    Del {
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
    ExpireAt {
        key: String,
        timestamp_ms: u64,
    },
    Ttl {
        key: String,
    },
    FlushDb,
    Info,
    Unknown(String),
}

impl Command {
    pub fn from_resp(resp: RespFrame<'_>) -> Result<Self, KvError> {
        let args = resp_to_args(resp)?;

        if args.is_empty() {
            return Err(KvError::InvalidCommand(String::from(
                "command array must not be empty",
            )));
        }

        let command_name = arg_to_str(args[0], "command")?;
        let normalized = command_name.to_ascii_uppercase();

        match normalized.as_str() {
            "PING" => parse_ping(&args),
            "SET" => parse_set(&args),
            "GET" => parse_get(&args),
            "DEL" => parse_del(&args),
            "EXISTS" => parse_exists(&args),
            "KEYS" => parse_keys(&args),
            "EXPIRE" => parse_expire(&args),
            "EXPIREAT" => parse_expire_at(&args),
            "TTL" => parse_ttl(&args),
            "FLUSHDB" => parse_flushdb(&args),
            "INFO" => parse_info(&args),
            _ => Ok(Command::Unknown(command_name.to_owned())),
        }
    }
}

fn parse_ping(args: &[&[u8]]) -> Result<Command, KvError> {
    expect_arity(args, 1, "PING")?;
    Ok(Command::Ping)
}

fn parse_set(args: &[&[u8]]) -> Result<Command, KvError> {
    if args.len() != 3 && args.len() != 5 {
        return Err(KvError::InvalidCommand(String::from(
            "SET expects key value or key value EX|PX duration",
        )));
    }

    let key = arg_to_owned_string(args[1], "SET key")?;
    let value = args[2].to_vec();
    let expire = if args.len() == 5 {
        let option = arg_to_str(args[3], "SET expire option")?.to_ascii_uppercase();
        let amount = parse_u64_arg(args[4], "SET expire duration")?;

        match option.as_str() {
            "EX" => Some(Duration::from_secs(amount)),
            "PX" => Some(Duration::from_millis(amount)),
            "PXAT" => {
                return Ok(Command::SetPxAt {
                    key,
                    value,
                    timestamp_ms: amount,
                });
            }
            _ => {
                return Err(KvError::InvalidCommand(format!(
                    "unsupported SET expire option: {option}"
                )));
            }
        }
    } else {
        None
    };

    Ok(Command::Set { key, value, expire })
}

fn parse_get(args: &[&[u8]]) -> Result<Command, KvError> {
    expect_arity(args, 2, "GET")?;
    Ok(Command::Get {
        key: arg_to_owned_string(args[1], "GET key")?,
    })
}

fn parse_del(args: &[&[u8]]) -> Result<Command, KvError> {
    if args.len() < 2 {
        return Err(KvError::InvalidCommand(String::from(
            "DEL expects at least one key",
        )));
    }

    let keys = args[1..]
        .iter()
        .map(|arg| arg_to_owned_string(arg, "DEL key"))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Command::Del { keys })
}

fn parse_exists(args: &[&[u8]]) -> Result<Command, KvError> {
    expect_arity(args, 2, "EXISTS")?;
    Ok(Command::Exists {
        key: arg_to_owned_string(args[1], "EXISTS key")?,
    })
}

fn parse_keys(args: &[&[u8]]) -> Result<Command, KvError> {
    if args.len() == 1 {
        return Ok(Command::Keys);
    }

    if args.len() == 2 && args[1] == b"*" {
        return Ok(Command::Keys);
    }

    Err(KvError::InvalidCommand(String::from(
        "KEYS supports only zero arguments or '*'",
    )))
}

fn parse_expire(args: &[&[u8]]) -> Result<Command, KvError> {
    expect_arity(args, 3, "EXPIRE")?;
    Ok(Command::Expire {
        key: arg_to_owned_string(args[1], "EXPIRE key")?,
        seconds: parse_u64_arg(args[2], "EXPIRE seconds")?,
    })
}

fn parse_expire_at(args: &[&[u8]]) -> Result<Command, KvError> {
    expect_arity(args, 3, "EXPIREAT")?;
    Ok(Command::ExpireAt {
        key: arg_to_owned_string(args[1], "EXPIREAT key")?,
        timestamp_ms: parse_u64_arg(args[2], "EXPIREAT timestamp_ms")?,
    })
}

fn parse_ttl(args: &[&[u8]]) -> Result<Command, KvError> {
    expect_arity(args, 2, "TTL")?;
    Ok(Command::Ttl {
        key: arg_to_owned_string(args[1], "TTL key")?,
    })
}

fn parse_flushdb(args: &[&[u8]]) -> Result<Command, KvError> {
    expect_arity(args, 1, "FLUSHDB")?;
    Ok(Command::FlushDb)
}

fn parse_info(args: &[&[u8]]) -> Result<Command, KvError> {
    expect_arity(args, 1, "INFO")?;
    Ok(Command::Info)
}

fn resp_to_args(resp: RespFrame<'_>) -> Result<Vec<&[u8]>, KvError> {
    match resp {
        RespFrame::Array(values) => values
            .iter()
            .map(resp_value_to_arg)
            .collect::<Result<Vec<_>, _>>(),
        RespFrame::SimpleString(line) => inline_line_to_args(line),
        RespFrame::BulkString(bytes) => {
            let line = str::from_utf8(bytes).map_err(|_| {
                KvError::InvalidCommand(String::from("inline command must be UTF-8"))
            })?;
            inline_line_to_args(line)
        }
        _ => Err(KvError::InvalidCommand(String::from(
            "command must be a RESP array or inline string",
        ))),
    }
}

fn resp_value_to_arg<'a>(value: &RespFrame<'a>) -> Result<&'a [u8], KvError> {
    match value {
        RespFrame::SimpleString(text) => Ok(text.as_bytes()),
        RespFrame::BulkString(bytes) => Ok(bytes),
        _ => Err(KvError::InvalidCommand(String::from(
            "command arguments must be simple strings or bulk strings",
        ))),
    }
}

fn inline_line_to_args(line: &str) -> Result<Vec<&[u8]>, KvError> {
    let args = line
        .split_ascii_whitespace()
        .map(str::as_bytes)
        .collect::<Vec<_>>();

    if args.is_empty() {
        return Err(KvError::InvalidCommand(String::from(
            "inline command must not be empty",
        )));
    }

    Ok(args)
}

fn expect_arity(args: &[&[u8]], expected: usize, command: &str) -> Result<(), KvError> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(KvError::InvalidCommand(format!(
            "{command} expects {} argument(s), got {}",
            expected.saturating_sub(1),
            args.len().saturating_sub(1)
        )))
    }
}

fn arg_to_str<'a>(arg: &'a [u8], label: &str) -> Result<&'a str, KvError> {
    str::from_utf8(arg).map_err(|_| KvError::InvalidCommand(format!("{label} must be UTF-8")))
}

fn arg_to_owned_string(arg: &[u8], label: &str) -> Result<String, KvError> {
    arg_to_str(arg, label).map(str::to_owned)
}

fn parse_u64_arg(arg: &[u8], label: &str) -> Result<u64, KvError> {
    let text = arg_to_str(arg, label)?;
    text.parse::<u64>()
        .map_err(|_| KvError::InvalidCommand(format!("{label} must be a non-negative integer")))
}
