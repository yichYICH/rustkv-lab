use std::io;

use bytes::BytesMut;
use rustkv_protocol::encoder::encode_resp;
use rustkv_protocol::parser::parse_resp;
use rustkv_protocol::resp::RespValue;
use rustkv_protocol::ProtocolError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
    max_frame_size: usize,
}

impl Connection {
    pub fn new(stream: TcpStream, max_frame_size: usize) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(4096),
            max_frame_size,
        }
    }

    pub async fn read_value(&mut self) -> Result<Option<Vec<u8>>, io::Error> {
        loop {
            match parse_resp(&self.buffer) {
                Ok((_frame, consumed)) => {
                    if consumed > self.max_frame_size {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "request exceeds maximum frame size: frame too large",
                        ));
                    }

                    let frame = self.buffer.split_to(consumed);
                    return Ok(Some(frame.to_vec()));
                }
                Err(ProtocolError::Incomplete) => {}
                Err(error) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid RESP frame: {error}"),
                    ));
                }
            }

            if self.buffer.len() > self.max_frame_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "request exceeds maximum frame size: frame too large",
                ));
            }

            let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
            if bytes_read == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                }

                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed with an incomplete RESP frame",
                ));
            }

            if self.buffer.len() > self.max_frame_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "request exceeds maximum frame size: frame too large",
                ));
            }
        }
    }

    pub async fn write_value(&mut self, value: &RespValue) -> Result<(), io::Error> {
        let bytes = encode_resp(value);
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await
    }
}
