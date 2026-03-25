use std::sync::Arc;

use crate::{
  Result,
  model::{
    BrokerState, ConsumerGroupAssignment, ConsumerGroupMember, LocalPartitionState, TopicConfig,
    TopicPartition,
  },
};

use super::super::{
  BrokerStateStore, ConsumerGroupStore, LocalPartitionStateStore, OffsetStore, TopicCatalogStore,
};

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

impl<T> ConsumerGroupStore for &T
where
  T: ConsumerGroupStore + ?Sized,
{
  fn save_group_member(&self, member: &ConsumerGroupMember) -> Result<()> {
    (**self).save_group_member(member)
  }

  fn load_group_member(&self, group: &str, member_id: &str) -> Result<Option<ConsumerGroupMember>> {
    (**self).load_group_member(group, member_id)
  }

  fn list_group_members(&self, group: &str) -> Result<Vec<ConsumerGroupMember>> {
    (**self).list_group_members(group)
  }

  fn delete_group_member(&self, group: &str, member_id: &str) -> Result<()> {
    (**self).delete_group_member(group, member_id)
  }

  fn delete_expired_group_members(&self, now_ms: u64) -> Result<usize> {
    (**self).delete_expired_group_members(now_ms)
  }

  fn save_group_assignment(&self, assignment: &ConsumerGroupAssignment) -> Result<()> {
    (**self).save_group_assignment(assignment)
  }

  fn load_group_assignment(&self, group: &str) -> Result<Option<ConsumerGroupAssignment>> {
    (**self).load_group_assignment(group)
  }
}

impl<T> ConsumerGroupStore for Arc<T>
where
  T: ConsumerGroupStore + ?Sized,
{
  fn save_group_member(&self, member: &ConsumerGroupMember) -> Result<()> {
    (**self).save_group_member(member)
  }

  fn load_group_member(&self, group: &str, member_id: &str) -> Result<Option<ConsumerGroupMember>> {
    (**self).load_group_member(group, member_id)
  }

  fn list_group_members(&self, group: &str) -> Result<Vec<ConsumerGroupMember>> {
    (**self).list_group_members(group)
  }

  fn delete_group_member(&self, group: &str, member_id: &str) -> Result<()> {
    (**self).delete_group_member(group, member_id)
  }

  fn delete_expired_group_members(&self, now_ms: u64) -> Result<usize> {
    (**self).delete_expired_group_members(now_ms)
  }

  fn save_group_assignment(&self, assignment: &ConsumerGroupAssignment) -> Result<()> {
    (**self).save_group_assignment(assignment)
  }

  fn load_group_assignment(&self, group: &str) -> Result<Option<ConsumerGroupAssignment>> {
    (**self).load_group_assignment(group)
  }
}
