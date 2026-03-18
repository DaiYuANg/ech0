use std::sync::Arc;

use crate::{
  Result,
  model::{BrokerState, LocalPartitionState, TopicConfig, TopicPartition},
};

use super::super::{BrokerStateStore, LocalPartitionStateStore, OffsetStore, TopicCatalogStore};

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

impl<T> OffsetStore for Arc<T>
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

impl<T> TopicCatalogStore for Arc<T>
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

impl<T> BrokerStateStore for Arc<T>
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

impl<T> LocalPartitionStateStore for Arc<T>
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
