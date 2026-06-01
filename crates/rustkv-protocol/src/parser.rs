use crate::resp::RespFrame;
use crate::ProtocolError;

pub fn parse_resp(input: &[u8]) -> Result<(RespFrame<'_>, usize), ProtocolError> {
    let mut parser = RespParser { input, pos: 0 };
    let value = parser.parse_value()?;
    Ok((value, parser.pos))
}

struct RespParser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> RespParser<'a> {
    fn parse_value(&mut self) -> Result<RespFrame<'a>, ProtocolError> {
        let type_byte = self.read_byte()?;

        match type_byte {
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b':' => self.parse_integer(),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(),
            other => Err(ProtocolError::InvalidTypeByte(other)),
        }
    }

    fn parse_simple_string(&mut self) -> Result<RespFrame<'a>, ProtocolError> {
        let line = self.read_line()?;
        let value = std::str::from_utf8(line).map_err(|_| ProtocolError::InvalidUtf8)?;
        Ok(RespFrame::SimpleString(value))
    }

    fn parse_error(&mut self) -> Result<RespFrame<'a>, ProtocolError> {
        let line = self.read_line()?;
        let value = std::str::from_utf8(line).map_err(|_| ProtocolError::InvalidUtf8)?;
        Ok(RespFrame::Error(value))
    }

    fn parse_integer(&mut self) -> Result<RespFrame<'a>, ProtocolError> {
        let line = self.read_line()?;
        let value = Self::parse_i64(line)?;
        Ok(RespFrame::Integer(value))
    }

    fn parse_bulk_string(&mut self) -> Result<RespFrame<'a>, ProtocolError> {
        let line = self.read_line()?;
        let len = Self::parse_i64(line)?;

        if len == -1 {
            return Ok(RespFrame::Null);
        }

        if len < -1 {
            return Err(ProtocolError::InvalidFormat(String::from(
                "bulk string length must be -1 or non-negative",
            )));
        }

        let bytes = self.read_bulk_bytes(len as usize)?;
        Ok(RespFrame::BulkString(bytes))
    }

    fn parse_array(&mut self) -> Result<RespFrame<'a>, ProtocolError> {
        let line = self.read_line()?;
        let len = Self::parse_i64(line)?;

        if len == -1 {
            return Ok(RespFrame::Null);
        }

        if len < -1 {
            return Err(ProtocolError::InvalidFormat(String::from(
                "array length must be -1 or non-negative",
            )));
        }

        let mut values = Vec::with_capacity(len as usize);
        for _ in 0..len {
            values.push(self.parse_value()?);
        }

        Ok(RespFrame::Array(values))
    }

    fn read_byte(&mut self) -> Result<u8, ProtocolError> {
        let byte = self
            .input
            .get(self.pos)
            .copied()
            .ok_or(ProtocolError::Incomplete)?;
        self.pos += 1;
        Ok(byte)
    }

    fn read_line(&mut self) -> Result<&'a [u8], ProtocolError> {
        let start = self.pos;
        let mut end = self.pos;

        while end + 1 < self.input.len() {
            if self.input[end] == b'\r' && self.input[end + 1] == b'\n' {
                self.pos = end + 2;
                return Ok(&self.input[start..end]);
            }

            end += 1;
        }

        Err(ProtocolError::Incomplete)
    }

    fn read_bulk_bytes(&mut self, len: usize) -> Result<&'a [u8], ProtocolError> {
        let start = self.pos;
        let end = start
            .checked_add(len)
            .ok_or_else(|| ProtocolError::InvalidFormat(String::from("bulk string overflow")))?;
        let crlf_end = end
            .checked_add(2)
            .ok_or_else(|| ProtocolError::InvalidFormat(String::from("bulk string overflow")))?;

        if crlf_end > self.input.len() {
            return Err(ProtocolError::Incomplete);
        }

        if self.input[end] != b'\r' || self.input[end + 1] != b'\n' {
            return Err(ProtocolError::InvalidFormat(String::from(
                "bulk string is not terminated by CRLF",
            )));
        }

        self.pos = crlf_end;
        Ok(&self.input[start..end])
    }

    fn parse_i64(bytes: &'a [u8]) -> Result<i64, ProtocolError> {
        let text = std::str::from_utf8(bytes).map_err(|_| ProtocolError::InvalidInteger)?;

        if text.is_empty() {
            return Err(ProtocolError::InvalidInteger);
        }

        text.parse::<i64>()
            .map_err(|_| ProtocolError::InvalidInteger)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoder::encode_resp;
    use crate::resp::RespValue;

    #[test]
    fn parse_simple_string() {
        let input = b"+OK\r\n";
        let (value, consumed) = parse_resp(input).unwrap();

        assert_eq!(value, RespFrame::SimpleString("OK"));
        assert_eq!(consumed, input.len());
    }

    #[test]
    fn parse_integer() {
        let input = b":-42\r\n";
        let (value, consumed) = parse_resp(input).unwrap();

        assert_eq!(value, RespFrame::Integer(-42));
        assert_eq!(consumed, input.len());
    }

    #[test]
    fn parse_bulk_string() {
        let input = b"$5\r\nhello\r\n";
        let (value, consumed) = parse_resp(input).unwrap();

        assert_eq!(value, RespFrame::BulkString(b"hello"));
        assert_eq!(consumed, input.len());
    }

    #[test]
    fn parse_null() {
        let input = b"$-1\r\n";
        let (value, consumed) = parse_resp(input).unwrap();

        assert_eq!(value, RespFrame::Null);
        assert_eq!(consumed, input.len());
    }

    #[test]
    fn parse_array() {
        let input = b"*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n";
        let (value, consumed) = parse_resp(input).unwrap();

        assert_eq!(
            value,
            RespFrame::Array(vec![
                RespFrame::BulkString(b"PING"),
                RespFrame::BulkString(b"PONG")
            ])
        );
        assert_eq!(consumed, input.len());
    }

    #[test]
    fn parse_nested_array() {
        let input = b"*3\r\n+OK\r\n:7\r\n*2\r\n$5\r\nhello\r\n$-1\r\n";
        let (value, consumed) = parse_resp(input).unwrap();

        assert_eq!(
            value,
            RespFrame::Array(vec![
                RespFrame::SimpleString("OK"),
                RespFrame::Integer(7),
                RespFrame::Array(vec![RespFrame::BulkString(b"hello"), RespFrame::Null])
            ])
        );
        assert_eq!(consumed, input.len());
    }

    #[test]
    fn parse_incomplete_input() {
        let error = parse_resp(b"$5\r\nhel").unwrap_err();

        assert_eq!(error, ProtocolError::Incomplete);
    }

    #[test]
    fn rejects_invalid_type_byte() {
        let error = parse_resp(b"?OK\r\n").unwrap_err();

        assert_eq!(error, ProtocolError::InvalidTypeByte(b'?'));
    }

    #[test]
    fn parse_invalid_input() {
        let error = parse_resp(b"*-2\r\n").unwrap_err();

        assert!(
            matches!(error, ProtocolError::InvalidFormat(message) if message.contains("array length"))
        );
    }

    #[test]
    fn rejects_invalid_integer() {
        let error = parse_resp(b":abc\r\n").unwrap_err();

        assert_eq!(error, ProtocolError::InvalidInteger);
    }

    #[test]
    fn rejects_invalid_utf8_simple_string() {
        let error = parse_resp(b"+\xff\r\n").unwrap_err();

        assert_eq!(error, ProtocolError::InvalidUtf8);
    }

    #[test]
    fn rejects_bulk_string_without_crlf_after_body() {
        let error = parse_resp(b"$3\r\nabcxx").unwrap_err();

        assert!(matches!(error, ProtocolError::InvalidFormat(_)));
    }

    #[test]
    fn returns_consumed_bytes_for_pipelined_input() {
        let input = b"+OK\r\n+NEXT\r\n";
        let (value, consumed) = parse_resp(input).unwrap();

        assert_eq!(value, RespFrame::SimpleString("OK"));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn encode_simple_string() {
        let value = RespValue::SimpleString(String::from("OK"));

        assert_eq!(encode_resp(&value), b"+OK\r\n");
    }

    #[test]
    fn encode_bulk_string() {
        let value = RespValue::BulkString(b"hello".to_vec());

        assert_eq!(encode_resp(&value), b"$5\r\nhello\r\n");
    }

    #[test]
    fn encode_array() {
        let value = RespValue::Array(vec![
            RespValue::BulkString(b"PING".to_vec()),
            RespValue::Integer(1),
        ]);

        assert_eq!(encode_resp(&value), b"*2\r\n$4\r\nPING\r\n:1\r\n");
    }
}
