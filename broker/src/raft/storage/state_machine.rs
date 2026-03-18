use std::{
  io::Cursor,
  sync::{Arc, Mutex},
};

use openraft::{
  BasicNode, EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
  StorageIOError, StoredMembership, storage::RaftStateMachine,
};
use serde::{Deserialize, Serialize};
use store::{
  ConsensusMetadataStore, LocalPartitionCommandExecutor, LocalPartitionStateMachine,
  LocalPartitionStateStore, MessageLogStore, MutablePartitionLogStore, OffsetStore,
  PartitionStateMachine, Result as StoreResult, StoreError, TopicCatalogStore,
};

use super::codec::{
  CONSENSUS_CURRENT_SNAPSHOT_KEY, CONSENSUS_LAST_APPLIED_LOG_ID_KEY, CONSENSUS_LAST_MEMBERSHIP_KEY,
  load_consensus_json_value, save_consensus_json_value,
};
use crate::raft::{BrokerRaftRequest, BrokerRaftResponse, EchoRaftTypeConfig};

#[derive(Debug, Clone)]
pub(in crate::raft) struct BrokerRaftStateMachine<L, M> {
  inner: Arc<Mutex<BrokerRaftStateMachineInner<L, M>>>,
}

#[derive(Debug)]
struct BrokerRaftStateMachineInner<L, M> {
  consensus_group: String,
  log_store: Arc<L>,
  meta_store: Arc<M>,
  executor: LocalPartitionStateMachine<LocalPartitionCommandExecutor<Arc<L>, Arc<M>>>,
  last_applied_log_id: Option<LogId<u64>>,
  last_membership: StoredMembership<u64, BasicNode>,
  current_snapshot: Option<StoredRaftSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRaftSnapshot {
  meta: SnapshotMeta<u64, BasicNode>,
  data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrokerRaftSnapshotData {
  last_applied_log_id: Option<LogId<u64>>,
  last_membership: StoredMembership<u64, BasicNode>,
}

impl<L, M> BrokerRaftStateMachine<L, M>
where
  L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
  M: OffsetStore
    + TopicCatalogStore
    + LocalPartitionStateStore
    + ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  pub(in crate::raft) fn new(
    log_store: Arc<L>,
    meta_store: Arc<M>,
    consensus_group: String,
  ) -> StoreResult<Self> {
    let executor = LocalPartitionStateMachine::new(LocalPartitionCommandExecutor::new(
      Arc::clone(&log_store),
      Arc::clone(&meta_store),
    ));
    let last_applied_log_id = load_consensus_json_value(
      meta_store.as_ref(),
      &consensus_group,
      CONSENSUS_LAST_APPLIED_LOG_ID_KEY,
    )?;
    let last_membership = load_consensus_json_value(
      meta_store.as_ref(),
      &consensus_group,
      CONSENSUS_LAST_MEMBERSHIP_KEY,
    )?
    .unwrap_or_default();
    let current_snapshot = load_consensus_json_value(
      meta_store.as_ref(),
      &consensus_group,
      CONSENSUS_CURRENT_SNAPSHOT_KEY,
    )?;

    Ok(Self {
      inner: Arc::new(Mutex::new(BrokerRaftStateMachineInner {
        consensus_group,
        log_store,
        meta_store,
        executor,
        last_applied_log_id,
        last_membership,
        current_snapshot,
      })),
    })
  }
}

#[derive(Debug, Clone)]
pub(in crate::raft) struct BrokerRaftSnapshotBuilder<L, M> {
  inner: Arc<Mutex<BrokerRaftStateMachineInner<L, M>>>,
}

impl<L, M> RaftSnapshotBuilder<EchoRaftTypeConfig> for BrokerRaftSnapshotBuilder<L, M>
where
  L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
  M: OffsetStore
    + TopicCatalogStore
    + LocalPartitionStateStore
    + ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  async fn build_snapshot(&mut self) -> Result<Snapshot<EchoRaftTypeConfig>, StorageError<u64>> {
    let mut inner = self.inner.lock().expect("poisoned raft state machine lock");
    let data = BrokerRaftSnapshotData {
      last_applied_log_id: inner.last_applied_log_id.clone(),
      last_membership: inner.last_membership.clone(),
    };
    let bytes =
      serde_json::to_vec(&data).map_err(|err| StorageIOError::write_snapshot(None, &err))?;
    let snapshot_id = format!(
      "snapshot-{}",
      inner
        .last_applied_log_id
        .as_ref()
        .map(|log_id| log_id.index)
        .unwrap_or(0)
    );
    let meta = SnapshotMeta {
      last_log_id: inner.last_applied_log_id.clone(),
      last_membership: inner.last_membership.clone(),
      snapshot_id,
    };
    inner.current_snapshot = Some(StoredRaftSnapshot {
      meta: meta.clone(),
      data: bytes.clone(),
    });
    persist_current_snapshot(&inner)
      .map_err(|err| StorageIOError::write_snapshot(Some(meta.signature()), &err))?;
    Ok(Snapshot {
      meta,
      snapshot: Box::new(Cursor::new(bytes)),
    })
  }
}

impl<L, M> RaftStateMachine<EchoRaftTypeConfig> for BrokerRaftStateMachine<L, M>
where
  L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
  M: OffsetStore
    + TopicCatalogStore
    + LocalPartitionStateStore
    + ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  type SnapshotBuilder = BrokerRaftSnapshotBuilder<L, M>;

  async fn applied_state(
    &mut self,
  ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
    let inner = self.inner.lock().expect("poisoned raft state machine lock");
    Ok((
      inner.last_applied_log_id.clone(),
      inner.last_membership.clone(),
    ))
  }

  async fn apply<I>(&mut self, entries: I) -> Result<Vec<BrokerRaftResponse>, StorageError<u64>>
  where
    I: IntoIterator<Item = openraft::Entry<EchoRaftTypeConfig>> + openraft::OptionalSend,
    I::IntoIter: openraft::OptionalSend,
  {
    let mut inner = self.inner.lock().expect("poisoned raft state machine lock");
    let mut responses = Vec::new();

    for entry in entries {
      let log_id = entry.log_id.clone();
      match entry.payload {
        EntryPayload::Blank => {
          inner.last_applied_log_id = Some(log_id);
          persist_applied_state(&inner).map_err(|err| StorageIOError::write_state_machine(&err))?;
          responses.push(BrokerRaftResponse::Noop);
        }
        EntryPayload::Membership(membership) => {
          inner.last_membership = StoredMembership::new(Some(log_id.clone()), membership);
          inner.last_applied_log_id = Some(log_id);
          persist_applied_state(&inner).map_err(|err| StorageIOError::write_state_machine(&err))?;
          responses.push(BrokerRaftResponse::MembershipChanged);
        }
        EntryPayload::Normal(request) => {
          let response = apply_raft_request(&mut inner, request)
            .map_err(|err| StorageIOError::apply(log_id.clone(), &err))?;
          inner.last_applied_log_id = Some(log_id);
          persist_applied_state(&inner).map_err(|err| StorageIOError::write_state_machine(&err))?;
          responses.push(response);
        }
      }
    }

    Ok(responses)
  }

  async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
    BrokerRaftSnapshotBuilder {
      inner: Arc::clone(&self.inner),
    }
  }

  async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
    Ok(Box::new(Cursor::new(Vec::new())))
  }

  async fn install_snapshot(
    &mut self,
    meta: &SnapshotMeta<u64, BasicNode>,
    snapshot: Box<Cursor<Vec<u8>>>,
  ) -> Result<(), StorageError<u64>> {
    let snapshot = *snapshot;
    let bytes = snapshot.into_inner();
    let data = if bytes.is_empty() {
      BrokerRaftSnapshotData {
        last_applied_log_id: meta.last_log_id.clone(),
        last_membership: meta.last_membership.clone(),
      }
    } else {
      serde_json::from_slice::<BrokerRaftSnapshotData>(&bytes)
        .map_err(|err| StorageIOError::write_snapshot(Some(meta.signature()), &err))?
    };

    let mut inner = self.inner.lock().expect("poisoned raft state machine lock");
    inner.last_applied_log_id = data.last_applied_log_id;
    inner.last_membership = data.last_membership;
    inner.current_snapshot = Some(StoredRaftSnapshot {
      meta: meta.clone(),
      data: bytes,
    });
    persist_applied_state(&inner).map_err(|err| StorageIOError::write_state_machine(&err))?;
    persist_current_snapshot(&inner)
      .map_err(|err| StorageIOError::write_snapshot(Some(meta.signature()), &err))?;
    Ok(())
  }

  async fn get_current_snapshot(
    &mut self,
  ) -> Result<Option<Snapshot<EchoRaftTypeConfig>>, StorageError<u64>> {
    let inner = self.inner.lock().expect("poisoned raft state machine lock");
    Ok(inner.current_snapshot.as_ref().map(|snapshot| Snapshot {
      meta: snapshot.meta.clone(),
      snapshot: Box::new(Cursor::new(snapshot.data.clone())),
    }))
  }
}

fn persist_applied_state<L, M>(inner: &BrokerRaftStateMachineInner<L, M>) -> StoreResult<()>
where
  M: ConsensusMetadataStore,
{
  save_consensus_json_value(
    inner.meta_store.as_ref(),
    &inner.consensus_group,
    CONSENSUS_LAST_APPLIED_LOG_ID_KEY,
    &inner.last_applied_log_id,
  )?;
  save_consensus_json_value(
    inner.meta_store.as_ref(),
    &inner.consensus_group,
    CONSENSUS_LAST_MEMBERSHIP_KEY,
    &inner.last_membership,
  )
}

fn persist_current_snapshot<L, M>(inner: &BrokerRaftStateMachineInner<L, M>) -> StoreResult<()>
where
  M: ConsensusMetadataStore,
{
  save_consensus_json_value(
    inner.meta_store.as_ref(),
    &inner.consensus_group,
    CONSENSUS_CURRENT_SNAPSHOT_KEY,
    &inner.current_snapshot,
  )
}

fn apply_raft_request<L, M>(
  inner: &mut BrokerRaftStateMachineInner<L, M>,
  request: BrokerRaftRequest,
) -> StoreResult<BrokerRaftResponse>
where
  L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
  M: OffsetStore
    + TopicCatalogStore
    + LocalPartitionStateStore
    + ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  match request {
    BrokerRaftRequest::EnsureTopic { topic } => {
      match inner.log_store.create_topic(topic.clone()) {
        Ok(()) | Err(StoreError::TopicAlreadyExists(_)) => {}
        Err(err) => return Err(err),
      }
      inner.meta_store.save_topic_config(&topic)?;
      Ok(BrokerRaftResponse::TopicEnsured)
    }
    BrokerRaftRequest::ApplyPartition { envelope } => {
      let applied = inner.executor.apply_replicated_envelope(envelope)?;
      Ok(BrokerRaftResponse::PartitionApplied {
        result: applied.result,
      })
    }
    BrokerRaftRequest::SaveConsumerOffset {
      consumer,
      topic_partition,
      next_offset,
    } => {
      inner
        .meta_store
        .save_consumer_offset(&consumer, &topic_partition, next_offset)?;
      Ok(BrokerRaftResponse::ConsumerOffsetSaved)
    }
  }
}
