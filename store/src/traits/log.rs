use crate::{
  Result,
  model::{LocalPartitionState, Record, TopicConfig, TopicPartition},
};

pub trait MessageLogStore: Send + Sync {
  fn create_topic(&self, topic: TopicConfig) -> Result<()>;
  fn topic_exists(&self, topic: &str) -> Result<bool>;
  fn append(&self, topic_partition: &TopicPartition, payload: &[u8]) -> Result<Record>;
  fn append_batch(
    &self,
    topic_partition: &TopicPartition,
    payloads: &[Vec<u8>],
  ) -> Result<Vec<Record>> {
    payloads
      .iter()
      .map(|p| self.append(topic_partition, p.as_slice()))
      .collect()
  }
  fn read_from(
    &self,
    topic_partition: &TopicPartition,
    offset: u64,
    max_records: usize,
  ) -> Result<Vec<Record>>;
  fn last_offset(&self, topic_partition: &TopicPartition) -> Result<Option<u64>>;
}

pub trait MutablePartitionLogStore: Send + Sync {
  fn truncate_from(&self, topic_partition: &TopicPartition, offset: u64) -> Result<()>;
  fn local_partition_state(&self, topic_partition: &TopicPartition) -> Result<LocalPartitionState>;
}
