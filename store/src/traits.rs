use crate::{
  Result,
  model::{BrokerState, LocalPartitionState, Record, TopicConfig, TopicPartition},
};

pub trait MessageLogStore: Send + Sync {
  fn create_topic(&self, topic: TopicConfig) -> Result<()>;
  fn topic_exists(&self, topic: &str) -> Result<bool>;
  fn append(&self, topic_partition: &TopicPartition, payload: &[u8]) -> Result<Record>;
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

pub trait ConsensusLogStore: Send + Sync {
  fn append_consensus_entry(&self, _group: &str, _index: u64, _payload: &[u8]) -> Result<()> {
    Err(crate::StoreError::Unsupported(
      "consensus log store is not implemented yet",
    ))
  }

  fn truncate_consensus_from(&self, _group: &str, _index: u64) -> Result<()> {
    Err(crate::StoreError::Unsupported(
      "consensus log store is not implemented yet",
    ))
  }
}

impl<T> MessageLogStore for &T
where
  T: MessageLogStore + ?Sized,
{
  fn create_topic(&self, topic: TopicConfig) -> Result<()> {
    (**self).create_topic(topic)
  }
  fn topic_exists(&self, topic: &str) -> Result<bool> {
    (**self).topic_exists(topic)
  }
  fn append(&self, topic_partition: &TopicPartition, payload: &[u8]) -> Result<Record> {
    (**self).append(topic_partition, payload)
  }
  fn read_from(
    &self,
    topic_partition: &TopicPartition,
    offset: u64,
    max_records: usize,
  ) -> Result<Vec<Record>> {
    (**self).read_from(topic_partition, offset, max_records)
  }
  fn last_offset(&self, topic_partition: &TopicPartition) -> Result<Option<u64>> {
    (**self).last_offset(topic_partition)
  }
}

impl<T> MutablePartitionLogStore for &T
where
  T: MutablePartitionLogStore + ?Sized,
{
  fn truncate_from(&self, topic_partition: &TopicPartition, offset: u64) -> Result<()> {
    (**self).truncate_from(topic_partition, offset)
  }
  fn local_partition_state(&self, topic_partition: &TopicPartition) -> Result<LocalPartitionState> {
    (**self).local_partition_state(topic_partition)
  }
}

impl<T> OffsetStore for &T
where
  T: OffsetStore + ?Sized,
{
  fn load_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
  ) -> Result<Option<u64>> {
    (**self).load_consumer_offset(consumer, topic_partition)
  }
  fn save_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> Result<()> {
    (**self).save_consumer_offset(consumer, topic_partition, next_offset)
  }
}

impl<T> TopicCatalogStore for &T
where
  T: TopicCatalogStore + ?Sized,
{
  fn save_topic_config(&self, topic: &TopicConfig) -> Result<()> {
    (**self).save_topic_config(topic)
  }
  fn load_topic_config(&self, topic: &str) -> Result<Option<TopicConfig>> {
    (**self).load_topic_config(topic)
  }
  fn list_topics(&self) -> Result<Vec<TopicConfig>> {
    (**self).list_topics()
  }
}

impl<T> BrokerStateStore for &T
where
  T: BrokerStateStore + ?Sized,
{
  fn save_broker_state(&self, state: &BrokerState) -> Result<()> {
    (**self).save_broker_state(state)
  }
  fn load_broker_state(&self) -> Result<Option<BrokerState>> {
    (**self).load_broker_state()
  }
}

impl<T> LocalPartitionStateStore for &T
where
  T: LocalPartitionStateStore + ?Sized,
{
  fn save_local_partition_state(&self, state: &LocalPartitionState) -> Result<()> {
    (**self).save_local_partition_state(state)
  }
  fn load_local_partition_state(
    &self,
    topic_partition: &TopicPartition,
  ) -> Result<Option<LocalPartitionState>> {
    (**self).load_local_partition_state(topic_partition)
  }
  fn list_local_partition_states(&self) -> Result<Vec<LocalPartitionState>> {
    (**self).list_local_partition_states()
  }
}

impl<T> ConsensusLogStore for &T
where
  T: ConsensusLogStore + ?Sized,
{
  fn append_consensus_entry(&self, group: &str, index: u64, payload: &[u8]) -> Result<()> {
    (**self).append_consensus_entry(group, index, payload)
  }
  fn truncate_consensus_from(&self, group: &str, index: u64) -> Result<()> {
    (**self).truncate_consensus_from(group, index)
  }
}
