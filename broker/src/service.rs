use std::sync::Arc;

use direct::{DirectRuntime, FetchInboxResult, SendDirectResult, is_internal_topic_name};
use queue::QueueRuntime;
use store::{
  ConsensusLogStore, ConsensusMetadataStore, LocalPartitionStateStore, MessageLogStore,
  MutablePartitionLogStore, OffsetStore, Result, StoreError, TopicCatalogStore, TopicConfig,
};

use crate::{config::RaftReadPolicy, raft::BrokerRaftRuntime};

mod mode;
#[cfg(test)]
mod tests;
mod types;

use mode::{
  ModeAwareLogStore, ModeAwareMetadataStore, build_metadata_writer, build_partition_appender,
};
pub use types::{BrokerIdentity, BrokerRuntimeMode, FetchResult, FetchedRecord};

#[derive(Debug)]
pub struct BrokerService<L, M> {
  identity: BrokerIdentity,
  runtime_mode: BrokerRuntimeMode,
  raft_runtime: Option<Arc<BrokerRaftRuntime<L, M>>>,
  queue: QueueRuntime<Arc<ModeAwareLogStore<L>>, Arc<ModeAwareMetadataStore<M>>>,
  direct: DirectRuntime<Arc<ModeAwareLogStore<L>>, Arc<ModeAwareMetadataStore<M>>>,
}

impl<L, M> BrokerService<L, M> {
  pub fn new(identity: BrokerIdentity, log: L, meta: M) -> Result<Self>
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
    Self::new_with_mode(identity, log, meta, BrokerRuntimeMode::Standalone)
  }

  pub fn new_with_mode(
    identity: BrokerIdentity,
    log: L,
    meta: M,
    runtime_mode: BrokerRuntimeMode,
  ) -> Result<Self>
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
    let log = Arc::new(log);
    let meta = Arc::new(meta);
    let raft_runtime = match &runtime_mode {
      BrokerRuntimeMode::Standalone => None,
      BrokerRuntimeMode::Raft(config) => Some(Arc::new(BrokerRaftRuntime::new(
        config.clone(),
        Arc::clone(&log),
        Arc::clone(&meta),
      )?)),
    };
    let mode_aware_meta = Arc::new(ModeAwareMetadataStore::new(
      Arc::clone(&meta),
      build_metadata_writer(&runtime_mode, Arc::clone(&meta), raft_runtime.clone()),
      runtime_mode.clone(),
    ));
    let mode_aware_log = Arc::new(ModeAwareLogStore::new(
      Arc::clone(&log),
      build_partition_appender(
        &runtime_mode,
        Arc::clone(&log),
        Arc::clone(&meta),
        raft_runtime.clone(),
      ),
      runtime_mode.clone(),
    ));
    Ok(Self {
      identity,
      runtime_mode,
      raft_runtime: raft_runtime.clone(),
      queue: QueueRuntime::new(Arc::clone(&mode_aware_log), Arc::clone(&mode_aware_meta)),
      direct: DirectRuntime::new(mode_aware_log, mode_aware_meta),
    })
  }

  pub fn identity(&self) -> &BrokerIdentity {
    &self.identity
  }

  pub fn runtime_mode(&self) -> &BrokerRuntimeMode {
    &self.runtime_mode
  }
}

impl<L, M> BrokerService<L, M>
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
  fn read_policy(&self) -> RaftReadPolicy {
    match &self.runtime_mode {
      BrokerRuntimeMode::Standalone => RaftReadPolicy::Local,
      BrokerRuntimeMode::Raft(config) => config.read_policy,
    }
  }

  fn ensure_read_allowed(&self) -> Result<()> {
    match self.read_policy() {
      RaftReadPolicy::Local => Ok(()),
      RaftReadPolicy::Leader => self.ensure_local_leader(),
      RaftReadPolicy::Linearizable => {
        self.ensure_local_leader()?;
        self
          .raft_runtime
          .as_ref()
          .expect("raft runtime must be initialized for linearizable reads")
          .ensure_linearizable_read()
      }
    }
  }

  fn ensure_local_leader(&self) -> Result<()> {
    let leader_id = self
      .raft_runtime
      .as_ref()
      .expect("raft runtime must be initialized for leader reads")
      .current_leader()?;

    if leader_id == Some(self.identity.node_id) {
      Ok(())
    } else {
      Err(StoreError::NotLeader { leader_id })
    }
  }

  pub fn create_topic(&self, topic: impl Into<String>, partitions: u32) -> Result<TopicConfig> {
    let topic = topic.into();
    if partitions == 0 {
      return Err(StoreError::Codec(
        "partitions must be greater than zero".to_owned(),
      ));
    }

    let mut config = TopicConfig::new(topic.clone());
    config.partitions = partitions;
    self.queue.create_topic(config.clone())?;
    Ok(config)
  }

  pub fn publish(
    &self,
    topic: impl Into<String>,
    partition: u32,
    payload: Vec<u8>,
  ) -> Result<(u64, u64)> {
    let topic = topic.into();
    let record = self.queue.publish(&topic, partition, &payload)?;
    Ok((record.offset, record.offset + 1))
  }

  pub fn fetch(
    &self,
    consumer: &str,
    topic: impl Into<String>,
    partition: u32,
    offset: Option<u64>,
    max_records: usize,
  ) -> Result<FetchResult> {
    self.ensure_read_allowed()?;
    let topic = topic.into();
    let fetched = self
      .queue
      .fetch(consumer, &topic, partition, offset, max_records)?;

    Ok(FetchResult {
      topic,
      partition,
      records: fetched
        .records
        .into_iter()
        .map(|record| FetchedRecord {
          offset: record.offset,
          timestamp_ms: record.timestamp_ms,
          payload: record.payload.to_vec(),
        })
        .collect(),
      next_offset: fetched.next_offset,
      high_watermark: fetched.high_watermark,
    })
  }

  pub fn commit_offset(
    &self,
    consumer: &str,
    topic: impl Into<String>,
    partition: u32,
    next_offset: u64,
  ) -> Result<()> {
    self
      .queue
      .ack(consumer, &topic.into(), partition, next_offset)
  }

  pub fn list_topics(&self) -> Result<Vec<TopicConfig>> {
    self.ensure_read_allowed()?;
    let mut topics = self.queue.list_topics()?;
    topics.retain(|topic| !is_internal_topic_name(&topic.name));
    Ok(topics)
  }

  pub fn send_direct(
    &self,
    sender: &str,
    recipient: &str,
    conversation_id: Option<String>,
    payload: Vec<u8>,
  ) -> Result<SendDirectResult> {
    self
      .direct
      .send(sender, recipient, conversation_id, payload)
  }

  pub fn fetch_inbox(&self, recipient: &str, max_records: usize) -> Result<FetchInboxResult> {
    self.ensure_read_allowed()?;
    self.direct.fetch_inbox(recipient, max_records)
  }

  pub fn ack_direct(&self, recipient: &str, next_offset: u64) -> Result<()> {
    self.direct.ack_inbox(recipient, next_offset)
  }
}
