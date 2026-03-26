use std::sync::Arc;

use crate::{
  Result,
  model::{LocalPartitionState, Record, RecordAppend, TopicConfig, TopicPartition},
};

use super::super::{MessageLogStore, MutablePartitionLogStore};

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
  fn append_record(&self, topic_partition: &TopicPartition, record: RecordAppend) -> Result<Record> {
    (**self).append_record(topic_partition, record)
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

impl<T> MessageLogStore for Arc<T>
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
  fn append_record(&self, topic_partition: &TopicPartition, record: RecordAppend) -> Result<Record> {
    (**self).append_record(topic_partition, record)
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

impl<T> MutablePartitionLogStore for Arc<T>
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
