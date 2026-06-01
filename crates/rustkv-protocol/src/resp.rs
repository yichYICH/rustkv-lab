#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespFrame<'a> {
    SimpleString(&'a str),
    Error(&'a str),
    Integer(i64),
    BulkString(&'a [u8]),
    Array(Vec<RespFrame<'a>>),
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    Null,
}
