use serde::{Deserialize, Serialize};

pub const VERSION_1: u8 = 1;

pub const CMD_HANDSHAKE_REQUEST: u16 = 1;
pub const CMD_PING_REQUEST: u16 = 2;
pub const CMD_CREATE_TOPIC_REQUEST: u16 = 3;
pub const CMD_PRODUCE_REQUEST: u16 = 4;
pub const CMD_FETCH_REQUEST: u16 = 5;
pub const CMD_COMMIT_OFFSET_REQUEST: u16 = 6;
pub const CMD_LIST_TOPICS_REQUEST: u16 = 7;
pub const CMD_SEND_DIRECT_REQUEST: u16 = 8;
pub const CMD_FETCH_INBOX_REQUEST: u16 = 9;
pub const CMD_ACK_DIRECT_REQUEST: u16 = 10;
pub const CMD_JOIN_CONSUMER_GROUP_REQUEST: u16 = 11;
pub const CMD_HEARTBEAT_CONSUMER_GROUP_REQUEST: u16 = 12;
pub const CMD_REBALANCE_CONSUMER_GROUP_REQUEST: u16 = 13;
pub const CMD_GET_CONSUMER_GROUP_ASSIGNMENT_REQUEST: u16 = 14;
pub const CMD_PRODUCE_BATCH_REQUEST: u16 = 15;
pub const CMD_FETCH_BATCH_REQUEST: u16 = 16;
pub const CMD_NACK_REQUEST: u16 = 17;
pub const CMD_PROCESS_RETRY_REQUEST: u16 = 18;
pub const CMD_SCHEDULE_DELAY_REQUEST: u16 = 19;

pub const CMD_HANDSHAKE_RESPONSE: u16 = 101;
pub const CMD_PING_RESPONSE: u16 = 102;
pub const CMD_CREATE_TOPIC_RESPONSE: u16 = 103;
pub const CMD_PRODUCE_RESPONSE: u16 = 104;
pub const CMD_FETCH_RESPONSE: u16 = 105;
pub const CMD_COMMIT_OFFSET_RESPONSE: u16 = 106;
pub const CMD_LIST_TOPICS_RESPONSE: u16 = 107;
pub const CMD_SEND_DIRECT_RESPONSE: u16 = 108;
pub const CMD_FETCH_INBOX_RESPONSE: u16 = 109;
pub const CMD_ACK_DIRECT_RESPONSE: u16 = 110;
pub const CMD_JOIN_CONSUMER_GROUP_RESPONSE: u16 = 111;
pub const CMD_HEARTBEAT_CONSUMER_GROUP_RESPONSE: u16 = 112;
pub const CMD_REBALANCE_CONSUMER_GROUP_RESPONSE: u16 = 113;
pub const CMD_GET_CONSUMER_GROUP_ASSIGNMENT_RESPONSE: u16 = 114;
pub const CMD_PRODUCE_BATCH_RESPONSE: u16 = 115;
pub const CMD_FETCH_BATCH_RESPONSE: u16 = 116;
pub const CMD_NACK_RESPONSE: u16 = 117;
pub const CMD_PROCESS_RETRY_RESPONSE: u16 = 118;
pub const CMD_SCHEDULE_DELAY_RESPONSE: u16 = 119;
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
  #[serde(default)]
  pub retention_max_bytes: Option<u64>,
  #[serde(default)]
  pub cleanup_policy: Option<TopicCleanupPolicy>,
  #[serde(default)]
  pub max_message_bytes: Option<u32>,
  #[serde(default)]
  pub max_batch_bytes: Option<u32>,
  #[serde(default)]
  pub retention_ms: Option<u64>,
  #[serde(default)]
  pub retry_policy: Option<TopicRetryPolicy>,
  #[serde(default)]
  pub dead_letter_topic: Option<String>,
  #[serde(default)]
  pub delay_enabled: Option<bool>,
  #[serde(default)]
  pub compaction_enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateTopicResponse {
  pub topic: String,
  pub partitions: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TopicCleanupPolicy {
  Delete,
  Compact,
  CompactAndDelete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicRetryPolicy {
  pub max_attempts: u32,
  pub backoff_initial_ms: u64,
  pub backoff_max_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProduceRequest {
  pub topic: String,
  pub partition: u32,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProduceResponse {
  /// Offset of the appended record (record ID, inclusive).
  pub offset: u64,
  /// Next offset cursor after this append (`offset + 1`).
  pub next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProduceBatchRequest {
  pub topic: String,
  pub partition: u32,
  pub payloads: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProduceBatchResponse {
  /// Offset of the first appended record (inclusive).
  pub base_offset: u64,
  /// Offset of the last appended record (inclusive).
  pub last_offset: u64,
  /// Next offset cursor after the last appended record (`last_offset + 1`).
  pub next_offset: u64,
  pub appended: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchRequest {
  pub consumer: String,
  pub topic: String,
  pub partition: u32,
  /// Inclusive fetch start offset. When omitted, server uses committed `next_offset`.
  pub offset: Option<u64>,
  pub max_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchRecord {
  /// Record offset (record ID, inclusive).
  pub offset: u64,
  pub timestamp_ms: u64,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchResponse {
  pub topic: String,
  pub partition: u32,
  pub records: Vec<FetchRecord>,
  /// Cursor to use for the next fetch.
  /// If records are returned: `last_record.offset + 1`; otherwise request offset.
  pub next_offset: u64,
  /// Largest committed and visible offset (inclusive). `None` means no visible records.
  pub high_watermark: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchBatchItemRequest {
  pub topic: String,
  pub partition: u32,
  #[serde(default)]
  /// Inclusive fetch start offset for this topic-partition item.
  pub offset: Option<u64>,
  pub max_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchBatchRequest {
  pub consumer: String,
  pub items: Vec<FetchBatchItemRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchBatchItemResponse {
  pub topic: String,
  pub partition: u32,
  pub records: Vec<FetchRecord>,
  /// Cursor to use for the next fetch of this item.
  pub next_offset: u64,
  /// Largest committed and visible offset for this item (inclusive).
  pub high_watermark: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchBatchResponse {
  pub items: Vec<FetchBatchItemResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitOffsetRequest {
  pub consumer: String,
  pub topic: String,
  pub partition: u32,
  /// Consumer cursor to persist. This is "next to consume", not "last consumed".
  pub next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitOffsetResponse {
  pub consumer: String,
  pub topic: String,
  pub partition: u32,
  /// Echoed committed cursor ("next to consume").
  pub next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NackRequest {
  pub consumer: String,
  pub topic: String,
  pub partition: u32,
  pub offset: u64,
  #[serde(default)]
  pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NackResponse {
  pub retry_topic: String,
  pub retry_partition: u32,
  pub retry_offset: u64,
  pub retry_next_offset: u64,
  pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessRetryRequest {
  pub consumer: String,
  pub source_topic: String,
  pub partition: u32,
  pub max_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessRetryResponse {
  pub retry_topic: String,
  pub partition: u32,
  pub moved_to_origin: usize,
  pub moved_to_dead_letter: usize,
  pub committed_next_offset: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleDelayRequest {
  pub topic: String,
  pub partition: u32,
  pub payload: Vec<u8>,
  pub deliver_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleDelayResponse {
  pub delay_topic: String,
  pub partition: u32,
  pub offset: u64,
  pub next_offset: u64,
  pub deliver_at_ms: u64,
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
pub struct SendDirectRequest {
  pub sender: String,
  pub recipient: String,
  pub conversation_id: Option<String>,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendDirectResponse {
  pub message_id: String,
  pub conversation_id: String,
  /// Offset of the appended inbox record (inclusive).
  pub offset: u64,
  /// Next inbox cursor after this append (`offset + 1`).
  pub next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchInboxRequest {
  pub recipient: String,
  pub max_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectMessageRecord {
  /// Offset of this inbox record (inclusive).
  pub offset: u64,
  pub message_id: String,
  pub conversation_id: String,
  pub sender: String,
  pub recipient: String,
  pub timestamp_ms: u64,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchInboxResponse {
  pub recipient: String,
  pub records: Vec<DirectMessageRecord>,
  /// Cursor to use for the next inbox fetch.
  pub next_offset: u64,
  /// Largest committed and visible inbox offset (inclusive).
  pub high_watermark: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckDirectRequest {
  pub recipient: String,
  /// Inbox cursor to persist ("next to consume").
  pub next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckDirectResponse {
  pub recipient: String,
  /// Echoed committed inbox cursor ("next to consume").
  pub next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JoinConsumerGroupRequest {
  pub group: String,
  pub member_id: String,
  pub topics: Vec<String>,
  pub session_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsumerGroupMemberLease {
  pub group: String,
  pub member_id: String,
  pub topics: Vec<String>,
  pub session_timeout_ms: u64,
  pub joined_at_ms: u64,
  pub last_heartbeat_ms: u64,
  pub expires_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JoinConsumerGroupResponse {
  pub lease: ConsumerGroupMemberLease,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatConsumerGroupRequest {
  pub group: String,
  pub member_id: String,
  #[serde(default)]
  pub session_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatConsumerGroupResponse {
  pub lease: ConsumerGroupMemberLease,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupPartitionAssignment {
  pub member_id: String,
  pub topic: String,
  pub partition: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsumerGroupAssignment {
  pub group: String,
  pub generation: u64,
  pub assignments: Vec<GroupPartitionAssignment>,
  pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RebalanceConsumerGroupRequest {
  pub group: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RebalanceConsumerGroupResponse {
  pub assignment: ConsumerGroupAssignment,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetConsumerGroupAssignmentRequest {
  pub group: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetConsumerGroupAssignmentResponse {
  pub assignment: Option<ConsumerGroupAssignment>,
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

#[cfg(test)]
mod tests {
  use std::collections::HashSet;

  use super::*;

  #[test]
  fn handshake_roundtrip_serialization_works() {
    let request = HandshakeRequest {
      client_id: "client-1".to_owned(),
    };
    let bytes = encode_json(&request).unwrap();
    let decoded: HandshakeRequest = decode_json(&bytes).unwrap();
    assert_eq!(decoded, request);
  }

  #[test]
  fn list_topics_response_roundtrip_serialization_works() {
    let response = ListTopicsResponse {
      topics: vec![TopicMetadata {
        topic: "orders".to_owned(),
        partitions: 3,
      }],
    };
    let bytes = encode_json(&response).unwrap();
    let decoded: ListTopicsResponse = decode_json(&bytes).unwrap();
    assert_eq!(decoded, response);
  }

  #[test]
  fn command_ids_are_unique() {
    let commands = [
      CMD_HANDSHAKE_REQUEST,
      CMD_PING_REQUEST,
      CMD_CREATE_TOPIC_REQUEST,
      CMD_PRODUCE_REQUEST,
      CMD_FETCH_REQUEST,
      CMD_COMMIT_OFFSET_REQUEST,
      CMD_LIST_TOPICS_REQUEST,
      CMD_SEND_DIRECT_REQUEST,
      CMD_FETCH_INBOX_REQUEST,
      CMD_ACK_DIRECT_REQUEST,
      CMD_JOIN_CONSUMER_GROUP_REQUEST,
      CMD_HEARTBEAT_CONSUMER_GROUP_REQUEST,
      CMD_REBALANCE_CONSUMER_GROUP_REQUEST,
      CMD_GET_CONSUMER_GROUP_ASSIGNMENT_REQUEST,
      CMD_PRODUCE_BATCH_REQUEST,
      CMD_FETCH_BATCH_REQUEST,
      CMD_NACK_REQUEST,
      CMD_PROCESS_RETRY_REQUEST,
      CMD_SCHEDULE_DELAY_REQUEST,
      CMD_HANDSHAKE_RESPONSE,
      CMD_PING_RESPONSE,
      CMD_CREATE_TOPIC_RESPONSE,
      CMD_PRODUCE_RESPONSE,
      CMD_FETCH_RESPONSE,
      CMD_COMMIT_OFFSET_RESPONSE,
      CMD_LIST_TOPICS_RESPONSE,
      CMD_SEND_DIRECT_RESPONSE,
      CMD_FETCH_INBOX_RESPONSE,
      CMD_ACK_DIRECT_RESPONSE,
      CMD_JOIN_CONSUMER_GROUP_RESPONSE,
      CMD_HEARTBEAT_CONSUMER_GROUP_RESPONSE,
      CMD_REBALANCE_CONSUMER_GROUP_RESPONSE,
      CMD_GET_CONSUMER_GROUP_ASSIGNMENT_RESPONSE,
      CMD_PRODUCE_BATCH_RESPONSE,
      CMD_FETCH_BATCH_RESPONSE,
      CMD_NACK_RESPONSE,
      CMD_PROCESS_RETRY_RESPONSE,
      CMD_SCHEDULE_DELAY_RESPONSE,
      CMD_ERROR_RESPONSE,
    ];

    let unique: HashSet<u16> = commands.into_iter().collect();
    assert_eq!(unique.len(), commands.len());
  }
}
