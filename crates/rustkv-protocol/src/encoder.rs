use crate::resp::RespValue;

pub fn encode_resp(value: &RespValue) -> Vec<u8> {
    let mut output = Vec::new();
    encode_into(value, &mut output);
    output
}

fn encode_into(value: &RespValue, output: &mut Vec<u8>) {
    match value {
        RespValue::SimpleString(text) => {
            output.push(b'+');
            output.extend_from_slice(text.as_bytes());
            output.extend_from_slice(b"\r\n");
        }
        RespValue::Error(text) => {
            output.push(b'-');
            output.extend_from_slice(text.as_bytes());
            output.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(number) => {
            output.push(b':');
            output.extend_from_slice(number.to_string().as_bytes());
            output.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(bytes) => {
            output.push(b'$');
            output.extend_from_slice(bytes.len().to_string().as_bytes());
            output.extend_from_slice(b"\r\n");
            output.extend_from_slice(bytes);
            output.extend_from_slice(b"\r\n");
        }
        RespValue::Array(values) => {
            output.push(b'*');
            output.extend_from_slice(values.len().to_string().as_bytes());
            output.extend_from_slice(b"\r\n");
            for value in values {
                encode_into(value, output);
            }
        }
        RespValue::Null => {
            output.extend_from_slice(b"$-1\r\n");
        }
    }
}
