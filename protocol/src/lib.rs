use serde::{Deserialize, Serialize};

pub const VERSION_1: u8 = 1;

pub const CMD_HANDSHAKE_REQUEST: u16 = 1;
pub const CMD_PING_REQUEST: u16 = 2;
pub const CMD_CREATE_TOPIC_REQUEST: u16 = 3;
pub const CMD_PRODUCE_REQUEST: u16 = 4;
pub const CMD_FETCH_REQUEST: u16 = 5;
pub const CMD_COMMIT_OFFSET_REQUEST: u16 = 6;
pub const CMD_LIST_TOPICS_REQUEST: u16 = 7;

pub const CMD_HANDSHAKE_RESPONSE: u16 = 101;
pub const CMD_PING_RESPONSE: u16 = 102;
pub const CMD_CREATE_TOPIC_RESPONSE: u16 = 103;
pub const CMD_PRODUCE_RESPONSE: u16 = 104;
pub const CMD_FETCH_RESPONSE: u16 = 105;
pub const CMD_COMMIT_OFFSET_RESPONSE: u16 = 106;
pub const CMD_LIST_TOPICS_RESPONSE: u16 = 107;
pub const CMD_ERROR_RESPONSE: u16 = 500;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandshakeRequest {
  pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandshakeResponse {
  pub server_id: String,
  pub protocol_version: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PingRequest {
  pub nonce: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PingResponse {
  pub nonce: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateTopicRequest {
  pub topic: String,
  pub partitions: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateTopicResponse {
  pub topic: String,
  pub partitions: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProduceRequest {
  pub topic: String,
  pub partition: u32,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProduceResponse {
  pub offset: u64,
  pub next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchRequest {
  pub consumer: String,
  pub topic: String,
  pub partition: u32,
  pub offset: Option<u64>,
  pub max_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchRecord {
  pub offset: u64,
  pub timestamp_ms: u64,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchResponse {
  pub topic: String,
  pub partition: u32,
  pub records: Vec<FetchRecord>,
  pub next_offset: u64,
  pub high_watermark: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitOffsetRequest {
  pub consumer: String,
  pub topic: String,
  pub partition: u32,
  pub next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitOffsetResponse {
  pub consumer: String,
  pub topic: String,
  pub partition: u32,
  pub next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicMetadata {
  pub topic: String,
  pub partitions: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListTopicsResponse {
  pub topics: Vec<TopicMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorResponse {
  pub code: String,
  pub message: String,
}

pub fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
  serde_json::to_vec(value)
}

pub fn decode_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, serde_json::Error> {
  serde_json::from_slice(bytes)
}
