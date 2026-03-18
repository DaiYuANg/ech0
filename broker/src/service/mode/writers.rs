use std::sync::Arc;

use store::{
  ConsensusLogStore, ConsensusMetadataStore, LocalPartitionStateStore, MessageLogStore,
  MutablePartitionLogStore, OffsetStore, Result, TopicCatalogStore, TopicConfig, TopicPartition,
};

use crate::raft::BrokerRaftRuntime;
use crate::service::BrokerRuntimeMode;

use super::traits::{MetadataWriter, OffsetCommitter};

struct StandaloneMetadataWriter<M> {
  inner: Arc<M>,
}

impl<M> StandaloneMetadataWriter<M> {
  fn new(inner: Arc<M>) -> Self {
    Self { inner }
  }
}

impl<M> MetadataWriter for StandaloneMetadataWriter<M>
where
  M: OffsetStore + TopicCatalogStore,
{
  fn save_topic_config(&self, topic: &TopicConfig) -> Result<()> {
    self.inner.save_topic_config(topic)
  }

  fn save_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> Result<()> {
    self
      .inner
      .save_consumer_offset(consumer, topic_partition, next_offset)
  }
}

struct RaftMetadataWriter<M> {
  _inner: Arc<M>,
  offset_writer: Arc<dyn OffsetCommitter>,
}

impl<M> RaftMetadataWriter<M> {
  fn new(inner: Arc<M>, offset_writer: Arc<dyn OffsetCommitter>) -> Self {
    Self {
      _inner: inner,
      offset_writer,
    }
  }
}

impl<M> MetadataWriter for RaftMetadataWriter<M>
where
  M: OffsetStore + TopicCatalogStore,
{
  fn save_topic_config(&self, _topic: &TopicConfig) -> Result<()> {
    Ok(())
  }

  fn save_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> Result<()> {
    self
      .offset_writer
      .save_consumer_offset(consumer, topic_partition, next_offset)
  }
}

impl<L, M> OffsetCommitter for BrokerRaftRuntime<L, M>
where
  L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
  M: OffsetStore
    + TopicCatalogStore
    + LocalPartitionStateStore
    + ConsensusLogStore
    + ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  fn save_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> Result<()> {
    self.save_consumer_offset(consumer, topic_partition, next_offset)
  }
}

pub(in crate::service) fn build_metadata_writer<L, M>(
  runtime_mode: &BrokerRuntimeMode,
  meta: Arc<M>,
  raft_runtime: Option<Arc<BrokerRaftRuntime<L, M>>>,
) -> Arc<dyn MetadataWriter>
where
  L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
  M: OffsetStore
    + TopicCatalogStore
    + LocalPartitionStateStore
    + ConsensusLogStore
    + ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  match runtime_mode {
    BrokerRuntimeMode::Standalone => Arc::new(StandaloneMetadataWriter::new(meta)),
    BrokerRuntimeMode::Raft(_) => Arc::new(RaftMetadataWriter::new(
      meta,
      raft_runtime.expect("raft runtime must be initialized"),
    )),
  }
}
