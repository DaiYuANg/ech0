use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicPartition {
  pub topic: String,
  pub partition: u32,
}

impl TopicPartition {
  pub fn new(topic: impl Into<String>, partition: u32) -> Self {
    Self {
      topic: topic.into(),
      partition,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfig {
  pub name: String,
  pub partitions: u32,
  pub segment_max_bytes: u64,
  pub index_interval_bytes: u64,
  #[serde(default = "default_topic_retention_max_bytes")]
  pub retention_max_bytes: u64,
  #[serde(default)]
  pub cleanup_policy: TopicCleanupPolicy,
  #[serde(default = "default_topic_max_message_bytes")]
  pub max_message_bytes: u32,
  #[serde(default = "default_topic_max_batch_bytes")]
  pub max_batch_bytes: u32,
  #[serde(default)]
  pub retention_ms: Option<u64>,
  #[serde(default)]
  pub retry_policy: TopicRetryPolicy,
  #[serde(default)]
  pub dead_letter_topic: Option<String>,
  #[serde(default)]
  pub delay_enabled: bool,
  #[serde(default)]
  pub compaction_enabled: bool,
}

impl TopicConfig {
  pub fn new(name: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      partitions: 1,
      segment_max_bytes: 16 * 1024 * 1024,
      index_interval_bytes: 4 * 1024,
      retention_max_bytes: default_topic_retention_max_bytes(),
      cleanup_policy: TopicCleanupPolicy::default(),
      max_message_bytes: default_topic_max_message_bytes(),
      max_batch_bytes: default_topic_max_batch_bytes(),
      retention_ms: None,
      retry_policy: TopicRetryPolicy::default(),
      dead_letter_topic: None,
      delay_enabled: false,
      compaction_enabled: false,
    }
  }

  pub fn partition(&self, partition: u32) -> TopicPartition {
    TopicPartition::new(self.name.clone(), partition)
  }
}

fn default_topic_retention_max_bytes() -> u64 {
  256 * 1024 * 1024
}

fn default_topic_max_message_bytes() -> u32 {
  1024 * 1024
}

fn default_topic_max_batch_bytes() -> u32 {
  8 * 1024 * 1024
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TopicCleanupPolicy {
  #[default]
  Delete,
  Compact,
  CompactAndDelete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRetryPolicy {
  #[serde(default = "default_retry_max_attempts")]
  pub max_attempts: u32,
  #[serde(default = "default_retry_backoff_initial_ms")]
  pub backoff_initial_ms: u64,
  #[serde(default = "default_retry_backoff_max_ms")]
  pub backoff_max_ms: u64,
}

impl Default for TopicRetryPolicy {
  fn default() -> Self {
    Self {
      max_attempts: default_retry_max_attempts(),
      backoff_initial_ms: default_retry_backoff_initial_ms(),
      backoff_max_ms: default_retry_backoff_max_ms(),
    }
  }
}

fn default_retry_max_attempts() -> u32 {
  16
}

fn default_retry_backoff_initial_ms() -> u64 {
  100
}

fn default_retry_backoff_max_ms() -> u64 {
  30_000
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
  pub offset: u64,
  pub timestamp_ms: u64,
  pub key: Option<Bytes>,
  pub headers: Vec<RecordHeader>,
  pub attributes: u16,
  pub payload: Bytes,
}

impl Record {
  pub fn new(offset: u64, payload: impl Into<Bytes>) -> Self {
    Self {
      offset,
      timestamp_ms: now_ms(),
      key: None,
      headers: Vec::new(),
      attributes: 0,
      payload: payload.into(),
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordHeader {
  pub key: String,
  pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordAppend {
  pub timestamp_ms: Option<u64>,
  pub key: Option<Bytes>,
  pub headers: Vec<RecordHeader>,
  pub attributes: u16,
  pub payload: Bytes,
}

impl RecordAppend {
  pub fn new(payload: impl Into<Bytes>) -> Self {
    Self {
      timestamp_ms: None,
      key: None,
      headers: Vec::new(),
      attributes: 0,
      payload: payload.into(),
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerOffset {
  pub consumer: String,
  pub topic_partition: TopicPartition,
  pub next_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckedOffset {
  pub consumer: String,
  pub topic_partition: TopicPartition,
  pub acked_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroupMember {
  pub group: String,
  pub member_id: String,
  pub topics: Vec<String>,
  pub session_timeout_ms: u64,
  pub joined_at_ms: u64,
  pub last_heartbeat_ms: u64,
}

impl ConsumerGroupMember {
  pub fn expires_at_ms(&self) -> u64 {
    self
      .last_heartbeat_ms
      .saturating_add(self.session_timeout_ms.max(1))
  }

  pub fn is_expired_at_ms(&self, now_ms: u64) -> bool {
    now_ms >= self.expires_at_ms()
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupPartitionAssignment {
  pub member_id: String,
  pub topic: String,
  pub partition: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroupAssignment {
  pub group: String,
  pub generation: u64,
  pub assignments: Vec<GroupPartitionAssignment>,
  pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerState {
  pub node_id: String,
  pub epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionAvailability {
  Online,
  Recovering,
  Unavailable,
  Fenced,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionState {
  pub leader_epoch: u64,
  pub high_watermark: Option<u64>,
  pub last_appended_offset: Option<u64>,
}

impl Default for PartitionState {
  fn default() -> Self {
    Self {
      leader_epoch: 0,
      high_watermark: None,
      last_appended_offset: None,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalPartitionState {
  pub topic_partition: TopicPartition,
  pub availability: PartitionAvailability,
  pub state: PartitionState,
}

impl LocalPartitionState {
  pub fn online(topic_partition: TopicPartition, last_appended_offset: Option<u64>) -> Self {
    Self {
      topic_partition,
      availability: PartitionAvailability::Online,
      state: PartitionState {
        leader_epoch: 0,
        high_watermark: last_appended_offset,
        last_appended_offset,
      },
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicValidationIssue {
  pub topic: String,
  pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollResult {
  /// Records returned from the requested inclusive start offset.
  pub records: Vec<Record>,
  /// Cursor for the next poll/fetch request.
  pub next_offset: u64,
  /// Largest committed and visible offset (inclusive).
  pub high_watermark: Option<u64>,
}

pub fn now_ms() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64
}
