use std::{fmt, sync::Arc};

use store::{
  ConsumerGroupStore, MessageLogStore, OffsetStore, Record, RecordAppend, Result, TopicCatalogStore, TopicConfig,
  TopicPartition,
};

use crate::service::BrokerRuntimeMode;

use super::traits::{MetadataWriter, PartitionAppender};

pub(in crate::service) struct ModeAwareLogStore<L> {
  inner: Arc<L>,
  appender: Arc<dyn PartitionAppender>,
  runtime_mode: BrokerRuntimeMode,
}

impl<L> ModeAwareLogStore<L> {
  pub(in crate::service) fn new(
    inner: Arc<L>,
    appender: Arc<dyn PartitionAppender>,
    runtime_mode: BrokerRuntimeMode,
  ) -> Self {
    Self {
      inner,
      appender,
      runtime_mode,
    }
  }
}

pub(in crate::service) struct ModeAwareMetadataStore<M> {
  inner: Arc<M>,
  writer: Arc<dyn MetadataWriter>,
  runtime_mode: BrokerRuntimeMode,
}

impl<M> ModeAwareMetadataStore<M> {
  pub(in crate::service) fn new(
    inner: Arc<M>,
    writer: Arc<dyn MetadataWriter>,
    runtime_mode: BrokerRuntimeMode,
  ) -> Self {
    Self {
      inner,
      writer,
      runtime_mode,
    }
  }
}

impl<L> fmt::Debug for ModeAwareLogStore<L> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ModeAwareLogStore")
      .field("runtime_mode", &self.runtime_mode.label())
      .finish()
  }
}

impl<M> fmt::Debug for ModeAwareMetadataStore<M> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ModeAwareMetadataStore")
      .field("runtime_mode", &self.runtime_mode.label())
      .finish()
  }
}

impl<L> MessageLogStore for ModeAwareLogStore<L>
where
  L: MessageLogStore,
{
  fn create_topic(&self, topic: TopicConfig) -> Result<()> {
    self.appender.create_topic(topic)
  }

  fn topic_exists(&self, topic: &str) -> Result<bool> {
    self.inner.topic_exists(topic)
  }

  fn append(&self, topic_partition: &TopicPartition, payload: &[u8]) -> Result<Record> {
    self.appender.append(topic_partition, payload)
  }

  fn append_record(&self, topic_partition: &TopicPartition, record: RecordAppend) -> Result<Record> {
    self.appender.append_record(topic_partition, record)
  }

  fn append_records_batch(
    &self,
    topic_partition: &TopicPartition,
    records: Vec<RecordAppend>,
  ) -> Result<Vec<Record>> {
    self.appender.append_records_batch(topic_partition, records)
  }

  fn read_from(
    &self,
    topic_partition: &TopicPartition,
    offset: u64,
    max_records: usize,
  ) -> Result<Vec<Record>> {
    self.inner.read_from(topic_partition, offset, max_records)
  }

  fn last_offset(&self, topic_partition: &TopicPartition) -> Result<Option<u64>> {
    self.inner.last_offset(topic_partition)
  }
}

impl<M> OffsetStore for ModeAwareMetadataStore<M>
where
  M: OffsetStore,
{
  fn load_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
  ) -> Result<Option<u64>> {
    self.inner.load_consumer_offset(consumer, topic_partition)
  }

  fn save_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> Result<()> {
    self
      .writer
      .save_consumer_offset(consumer, topic_partition, next_offset)
  }
}

impl<M> TopicCatalogStore for ModeAwareMetadataStore<M>
where
  M: TopicCatalogStore,
{
  fn save_topic_config(&self, topic: &TopicConfig) -> Result<()> {
    self.writer.save_topic_config(topic)
  }

  fn load_topic_config(&self, topic: &str) -> Result<Option<TopicConfig>> {
    self.inner.load_topic_config(topic)
  }

  fn list_topics(&self) -> Result<Vec<TopicConfig>> {
    self.inner.list_topics()
  }
}

impl<M> ConsumerGroupStore for ModeAwareMetadataStore<M>
where
  M: ConsumerGroupStore,
{
  fn save_group_member(&self, member: &store::ConsumerGroupMember) -> Result<()> {
    self.inner.save_group_member(member)
  }

  fn load_group_member(
    &self,
    group: &str,
    member_id: &str,
  ) -> Result<Option<store::ConsumerGroupMember>> {
    self.inner.load_group_member(group, member_id)
  }

  fn list_group_members(&self, group: &str) -> Result<Vec<store::ConsumerGroupMember>> {
    self.inner.list_group_members(group)
  }

  fn delete_group_member(&self, group: &str, member_id: &str) -> Result<()> {
    self.inner.delete_group_member(group, member_id)
  }

  fn delete_expired_group_members(&self, now_ms: u64) -> Result<usize> {
    self.inner.delete_expired_group_members(now_ms)
  }

  fn save_group_assignment(&self, assignment: &store::ConsumerGroupAssignment) -> Result<()> {
    self.inner.save_group_assignment(assignment)
  }

  fn load_group_assignment(&self, group: &str) -> Result<Option<store::ConsumerGroupAssignment>> {
    self.inner.load_group_assignment(group)
  }

  fn list_group_assignments(&self) -> Result<Vec<store::ConsumerGroupAssignment>> {
    self.inner.list_group_assignments()
  }
}
