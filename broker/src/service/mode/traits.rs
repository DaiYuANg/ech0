use store::{Record, Result, TopicConfig, TopicPartition};

pub(in crate::service) trait PartitionAppender: Send + Sync {
  fn create_topic(&self, topic: TopicConfig) -> Result<()>;
  fn append(&self, topic_partition: &TopicPartition, payload: &[u8]) -> Result<Record>;
}

pub(in crate::service) trait MetadataWriter: Send + Sync {
  fn save_topic_config(&self, topic: &TopicConfig) -> Result<()>;
  fn save_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> Result<()>;
}

pub(in crate::service) trait OffsetCommitter: Send + Sync {
  fn save_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> Result<()>;
}
