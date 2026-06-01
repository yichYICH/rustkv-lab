use rustkv_protocol::ProtocolError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KvError {
    #[error("protocol error: {0}")]
    Protocol(ProtocolError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid command: {0}")]
    InvalidCommand(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

impl From<ProtocolError> for KvError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}
