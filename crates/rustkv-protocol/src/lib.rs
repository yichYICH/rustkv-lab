pub mod encoder;
pub mod parser;
pub mod resp;

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("incomplete RESP frame")]
    Incomplete,
    #[error("invalid RESP type byte: {0}")]
    InvalidTypeByte(u8),
    #[error("invalid integer")]
    InvalidInteger,
    #[error("invalid utf-8")]
    InvalidUtf8,
    #[error("invalid RESP format: {0}")]
    InvalidFormat(String),
}
