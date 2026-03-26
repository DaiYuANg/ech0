use std::sync::Arc;

use store::{
  ApplyResult, CommandSource, ConsensusLogStore, ConsensusMetadataStore, LocalPartitionCommand,
  LocalPartitionCommandExecutor, LocalPartitionStateMachine, LocalPartitionStateStore,
  MessageLogStore, MutablePartitionLogStore, OffsetStore, PartitionCommandEnvelope,
  PartitionStateMachine, Record, RecordAppend, Result, StoreError, TopicCatalogStore,
  TopicConfig, TopicPartition,
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
    self.append_record(topic_partition, RecordAppend::new(payload.to_vec()))
  }

  fn append_record(&self, topic_partition: &TopicPartition, record: RecordAppend) -> Result<Record> {
    let applied = self.state_machine.apply_local_command(
      LocalPartitionCommand::Append {
        topic_partition: topic_partition.clone(),
        record,
      },
      CommandSource::Client,
    )?;
    appended_record_from_apply_result(topic_partition, applied.result)
  }

  fn append_records_batch(
    &self,
    topic_partition: &TopicPartition,
    records: Vec<RecordAppend>,
  ) -> Result<Vec<Record>> {
    let expected_count = records.len();
    let applied = self.state_machine.apply_local_command(
      LocalPartitionCommand::AppendBatch {
        topic_partition: topic_partition.clone(),
        records,
      },
      CommandSource::Client,
    )?;
    appended_records_from_apply_result(
      topic_partition,
      expected_count,
      applied.result,
    )
  }
}

struct RaftPartitionAppender<L, M> {
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
    let _ = log;
    Self { runtime }
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
    self.append_record(topic_partition, RecordAppend::new(payload.to_vec()))
  }

  fn append_record(&self, topic_partition: &TopicPartition, record: RecordAppend) -> Result<Record> {
    let envelope = PartitionCommandEnvelope::new(
      LocalPartitionCommand::Append {
        topic_partition: topic_partition.clone(),
        record,
      },
      CommandSource::Client,
    );
    let applied = self
      .runtime
      .apply_partition(envelope.replicated_command()?)?;
    appended_record_from_apply_result(topic_partition, applied)
  }

  fn append_records_batch(
    &self,
    topic_partition: &TopicPartition,
    records: Vec<RecordAppend>,
  ) -> Result<Vec<Record>> {
    let expected_count = records.len();
    let envelope = PartitionCommandEnvelope::new(
      LocalPartitionCommand::AppendBatch {
        topic_partition: topic_partition.clone(),
        records,
      },
      CommandSource::Client,
    );
    let applied = self
      .runtime
      .apply_partition(envelope.replicated_command()?)?;
    appended_records_from_apply_result(topic_partition, expected_count, applied)
  }
}

fn appended_record_from_apply_result(
  topic_partition: &TopicPartition,
  result: ApplyResult,
) -> Result<Record> {
  appended_records_from_apply_result(topic_partition, 1, result)?
    .into_iter()
    .next()
    .ok_or_else(|| {
      StoreError::Corruption(format!(
        "append succeeded but no record was returned for topic {} partition {}",
        topic_partition.topic, topic_partition.partition
      ))
    })
}

fn appended_records_from_apply_result(
  topic_partition: &TopicPartition,
  expected_count: usize,
  result: ApplyResult,
) -> Result<Vec<Record>> {
  if expected_count == 0 {
    return Ok(Vec::new());
  }
  let (records, next_offset) = match result {
    ApplyResult::Appended { records, next_offset } => (records, next_offset),
    other => {
      return Err(StoreError::Corruption(format!(
        "unexpected apply result for append: {other:?}"
      )));
    }
  };
  if records.len() != expected_count {
    return Err(StoreError::Corruption(format!(
      "append succeeded but expected {expected_count} records from topic {} partition {}, found {}",
      topic_partition.topic,
      topic_partition.partition,
      records.len()
    )));
  }
  let expected_next_offset = records
    .last()
    .map(|record| record.offset + 1)
    .ok_or_else(|| StoreError::Corruption("append result returned no records".to_owned()))?;
  if next_offset != expected_next_offset {
    return Err(StoreError::Corruption(format!(
      "append result returned inconsistent next offset for topic {} partition {}: expected {}, got {}",
      topic_partition.topic,
      topic_partition.partition,
      expected_next_offset,
      next_offset
    )));
  }
  Ok(records)
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
