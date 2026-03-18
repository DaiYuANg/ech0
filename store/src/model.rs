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
}

impl TopicConfig {
  pub fn new(name: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      partitions: 1,
      segment_max_bytes: 16 * 1024 * 1024,
      index_interval_bytes: 4 * 1024,
    }
  }

  pub fn partition(&self, partition: u32) -> TopicPartition {
    TopicPartition::new(self.name.clone(), partition)
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
  pub offset: u64,
  pub timestamp_ms: u64,
  pub payload: Bytes,
}

impl Record {
  pub fn new(offset: u64, payload: impl Into<Bytes>) -> Self {
    Self {
      offset,
      timestamp_ms: now_ms(),
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
  pub records: Vec<Record>,
  pub next_offset: u64,
  pub high_watermark: Option<u64>,
}

pub fn now_ms() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64
}
