use crate::{
  Result,
  model::{BrokerState, LocalPartitionState, TopicConfig, TopicPartition},
};

pub trait OffsetStore: Send + Sync {
  fn load_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
  ) -> Result<Option<u64>>;
  fn save_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> Result<()>;
}

pub trait TopicCatalogStore: Send + Sync {
  fn save_topic_config(&self, topic: &TopicConfig) -> Result<()>;
  fn load_topic_config(&self, topic: &str) -> Result<Option<TopicConfig>>;
  fn list_topics(&self) -> Result<Vec<TopicConfig>>;
}

pub trait BrokerStateStore: Send + Sync {
  fn save_broker_state(&self, state: &BrokerState) -> Result<()>;
  fn load_broker_state(&self) -> Result<Option<BrokerState>>;
}

pub trait LocalPartitionStateStore: Send + Sync {
  fn save_local_partition_state(&self, state: &LocalPartitionState) -> Result<()>;
  fn load_local_partition_state(
    &self,
    topic_partition: &TopicPartition,
  ) -> Result<Option<LocalPartitionState>>;
  fn list_local_partition_states(&self) -> Result<Vec<LocalPartitionState>>;
}
