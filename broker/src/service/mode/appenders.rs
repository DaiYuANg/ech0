use std::sync::Arc;

use store::{
  ApplyResult, CommandSource, ConsensusLogStore, ConsensusMetadataStore, LocalPartitionCommand,
  LocalPartitionCommandExecutor, LocalPartitionStateMachine, LocalPartitionStateStore,
  MessageLogStore, MutablePartitionLogStore, OffsetStore, PartitionCommandEnvelope,
  PartitionStateMachine, Record, Result, StoreError, TopicCatalogStore, TopicConfig,
  TopicPartition,
};

use crate::raft::BrokerRaftRuntime;
use crate::service::BrokerRuntimeMode;

use super::traits::PartitionAppender;

struct StandalonePartitionAppender<L, M> {
  log: Arc<L>,
  state_machine: LocalPartitionStateMachine<LocalPartitionCommandExecutor<Arc<L>, Arc<M>>>,
}

impl<L, M> StandalonePartitionAppender<L, M>
where
  L: MessageLogStore + MutablePartitionLogStore,
  M: LocalPartitionStateStore,
{
  fn new(log: Arc<L>, meta: Arc<M>) -> Self {
    let state_machine =
      LocalPartitionStateMachine::new(LocalPartitionCommandExecutor::new(Arc::clone(&log), meta));
    Self { log, state_machine }
  }
}

impl<L, M> PartitionAppender for StandalonePartitionAppender<L, M>
where
  L: MessageLogStore + MutablePartitionLogStore,
  M: LocalPartitionStateStore,
{
  fn create_topic(&self, topic: TopicConfig) -> Result<()> {
    self.log.create_topic(topic)
  }

  fn append(&self, topic_partition: &TopicPartition, payload: &[u8]) -> Result<Record> {
    let applied = self.state_machine.apply_local_command(
      LocalPartitionCommand::Append {
        topic_partition: topic_partition.clone(),
        payload: payload.to_vec(),
      },
      CommandSource::Client,
    )?;
    appended_record_from_apply_result(self.log.as_ref(), topic_partition, applied.result)
  }
}

struct RaftPartitionAppender<L, M> {
  log: Arc<L>,
  runtime: Arc<BrokerRaftRuntime<L, M>>,
}

impl<L, M> RaftPartitionAppender<L, M>
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
  fn new(log: Arc<L>, runtime: Arc<BrokerRaftRuntime<L, M>>) -> Self {
    Self { log, runtime }
  }
}

impl<L, M> PartitionAppender for RaftPartitionAppender<L, M>
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
  fn create_topic(&self, topic: TopicConfig) -> Result<()> {
    self.runtime.ensure_topic(topic)
  }

  fn append(&self, topic_partition: &TopicPartition, payload: &[u8]) -> Result<Record> {
    let envelope = PartitionCommandEnvelope::new(
      LocalPartitionCommand::Append {
        topic_partition: topic_partition.clone(),
        payload: payload.to_vec(),
      },
      CommandSource::Client,
    );
    let applied = self
      .runtime
      .apply_partition(envelope.replicated_command()?)?;
    appended_record_from_apply_result(self.log.as_ref(), topic_partition, applied)
  }
}

fn appended_record_from_apply_result<L>(
  log: &L,
  topic_partition: &TopicPartition,
  result: ApplyResult,
) -> Result<Record>
where
  L: MessageLogStore,
{
  let next_offset = match result {
    ApplyResult::Appended { next_offset } => next_offset,
    other => {
      return Err(StoreError::Corruption(format!(
        "unexpected apply result for append: {other:?}"
      )));
    }
  };
  let offset = next_offset.checked_sub(1).ok_or_else(|| {
    StoreError::Corruption("append result returned invalid next offset 0".to_owned())
  })?;
  log
    .read_from(topic_partition, offset, 1)?
    .into_iter()
    .next()
    .ok_or_else(|| {
      StoreError::Corruption(format!(
        "append succeeded but record {offset} was not readable from topic {} partition {}",
        topic_partition.topic, topic_partition.partition
      ))
    })
}

pub(in crate::service) fn build_partition_appender<L, M>(
  runtime_mode: &BrokerRuntimeMode,
  log: Arc<L>,
  meta: Arc<M>,
  raft_runtime: Option<Arc<BrokerRaftRuntime<L, M>>>,
) -> Arc<dyn PartitionAppender>
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
    BrokerRuntimeMode::Standalone => Arc::new(StandalonePartitionAppender::new(log, meta)),
    BrokerRuntimeMode::Raft(_) => Arc::new(RaftPartitionAppender::new(
      log,
      raft_runtime.expect("raft runtime must be initialized"),
    )),
  }
}
