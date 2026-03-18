use std::{
  io,
  ops::{Bound, RangeBounds},
  sync::{Arc, Mutex},
};

use openraft::{
  LogId, LogState, RaftLogReader, StorageError, StorageIOError, Vote,
  storage::{LogFlushed, RaftLogStorage},
};
use store::{ConsensusLogStore, ConsensusMetadataStore, Result as StoreResult, StoreError};

use super::codec::{
  CONSENSUS_COMMITTED_KEY, CONSENSUS_LAST_PURGED_LOG_ID_KEY, CONSENSUS_VOTE_KEY, decode_json_value,
  load_consensus_json_value, save_consensus_json_value,
};
use crate::raft::EchoRaftTypeConfig;

pub(in crate::raft) struct BrokerRaftLogStore<M> {
  group: String,
  store: Arc<M>,
  io_lock: Arc<Mutex<()>>,
}

impl<M> Clone for BrokerRaftLogStore<M> {
  fn clone(&self) -> Self {
    Self {
      group: self.group.clone(),
      store: Arc::clone(&self.store),
      io_lock: Arc::clone(&self.io_lock),
    }
  }
}

impl<M> std::fmt::Debug for BrokerRaftLogStore<M> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("BrokerRaftLogStore")
      .field("group", &self.group)
      .finish()
  }
}

impl<M> BrokerRaftLogStore<M>
where
  M: ConsensusLogStore + ConsensusMetadataStore,
{
  pub(in crate::raft) fn new(group: String, store: Arc<M>) -> Self {
    Self {
      group,
      store,
      io_lock: Arc::new(Mutex::new(())),
    }
  }
}

impl<M> RaftLogReader<EchoRaftTypeConfig> for BrokerRaftLogStore<M>
where
  M: ConsensusLogStore + ConsensusMetadataStore + Send + Sync + 'static,
{
  async fn try_get_log_entries<RB>(
    &mut self,
    range: RB,
  ) -> Result<Vec<openraft::Entry<EchoRaftTypeConfig>>, StorageError<u64>>
  where
    RB: RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
  {
    let (start, end) = log_range_bounds(range).map_err(|err| StorageIOError::read_logs(&err))?;
    let entries = self
      .store
      .load_consensus_entries(&self.group, start, end)
      .map_err(|err| StorageIOError::read_logs(&err))?;

    entries
      .into_iter()
      .map(|(index, payload)| {
        decode_json_value::<openraft::Entry<EchoRaftTypeConfig>>("raft log entry", &payload)
          .map_err(|err| StorageIOError::read_log_at_index(index, &err).into())
      })
      .collect()
  }
}

impl<M> RaftLogStorage<EchoRaftTypeConfig> for BrokerRaftLogStore<M>
where
  M: ConsensusLogStore + ConsensusMetadataStore + Send + Sync + 'static,
{
  type LogReader = Self;

  async fn get_log_state(&mut self) -> Result<LogState<EchoRaftTypeConfig>, StorageError<u64>> {
    let last_purged_log_id = load_consensus_json_value::<LogId<u64>, _>(
      self.store.as_ref(),
      &self.group,
      CONSENSUS_LAST_PURGED_LOG_ID_KEY,
    )
    .map_err(|err| StorageIOError::read_logs(&err))?;
    let last_log_id = self
      .store
      .load_last_consensus_index(&self.group)
      .map_err(|err| StorageIOError::read_logs(&err))?
      .map(|index| {
        self
          .store
          .load_consensus_entry(&self.group, index)
          .map_err(|err| StorageIOError::read_log_at_index(index, &err))?
          .ok_or_else(|| {
            StorageIOError::read_log_at_index(
              index,
              &io::Error::other("missing last consensus log entry"),
            )
          })
          .and_then(|payload| {
            decode_json_value::<openraft::Entry<EchoRaftTypeConfig>>("raft log entry", &payload)
              .map_err(|err| StorageIOError::read_log_at_index(index, &err))
          })
          .map(|entry| entry.log_id)
      })
      .transpose()?
      .or_else(|| last_purged_log_id.clone());

    Ok(LogState {
      last_purged_log_id,
      last_log_id,
    })
  }

  async fn get_log_reader(&mut self) -> Self::LogReader {
    self.clone()
  }

  async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
    let _guard = self.io_lock.lock().expect("poisoned raft io lock");
    save_consensus_json_value(self.store.as_ref(), &self.group, CONSENSUS_VOTE_KEY, vote)
      .map_err(|err| StorageIOError::write_vote(&err).into())
  }

  async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
    load_consensus_json_value(self.store.as_ref(), &self.group, CONSENSUS_VOTE_KEY)
      .map_err(|err| StorageIOError::read_vote(&err).into())
  }

  async fn save_committed(
    &mut self,
    committed: Option<LogId<u64>>,
  ) -> Result<(), StorageError<u64>> {
    let _guard = self.io_lock.lock().expect("poisoned raft io lock");
    save_consensus_json_value(
      self.store.as_ref(),
      &self.group,
      CONSENSUS_COMMITTED_KEY,
      &committed,
    )
    .map_err(|err| StorageIOError::write_logs(&err).into())
  }

  async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
    load_consensus_json_value(self.store.as_ref(), &self.group, CONSENSUS_COMMITTED_KEY)
      .map_err(|err| StorageIOError::read_logs(&err).into())
  }

  async fn append<I>(
    &mut self,
    entries: I,
    callback: LogFlushed<EchoRaftTypeConfig>,
  ) -> Result<(), StorageError<u64>>
  where
    I: IntoIterator<Item = openraft::Entry<EchoRaftTypeConfig>> + openraft::OptionalSend,
    I::IntoIter: openraft::OptionalSend,
  {
    let _guard = self.io_lock.lock().expect("poisoned raft io lock");
    for entry in entries {
      let payload = super::codec::encode_json_value("raft log entry", &entry)
        .map_err(|err| StorageIOError::write_log_entry(entry.log_id.clone(), &err))?;
      self
        .store
        .append_consensus_entry(&self.group, entry.log_id.index, &payload)
        .map_err(|err| StorageIOError::write_log_entry(entry.log_id.clone(), &err))?;
    }
    callback.log_io_completed(Ok(()));
    Ok(())
  }

  async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
    let _guard = self.io_lock.lock().expect("poisoned raft io lock");
    self
      .store
      .truncate_consensus_from(&self.group, log_id.index)
      .map_err(|err| StorageIOError::write_logs(&err).into())
  }

  async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
    let _guard = self.io_lock.lock().expect("poisoned raft io lock");
    self
      .store
      .purge_consensus_to(&self.group, log_id.index)
      .map_err(|err| StorageIOError::write_logs(&err))?;
    save_consensus_json_value(
      self.store.as_ref(),
      &self.group,
      CONSENSUS_LAST_PURGED_LOG_ID_KEY,
      &log_id,
    )
    .map_err(|err| StorageIOError::write_logs(&err).into())
  }
}

fn log_range_bounds<RB>(range: RB) -> StoreResult<(u64, Option<u64>)>
where
  RB: RangeBounds<u64>,
{
  let start = match range.start_bound() {
    Bound::Included(index) => *index,
    Bound::Excluded(index) => index
      .checked_add(1)
      .ok_or_else(|| StoreError::Corruption("raft log range start overflowed".to_owned()))?,
    Bound::Unbounded => 0,
  };
  let end = match range.end_bound() {
    Bound::Included(index) => Some(
      index
        .checked_add(1)
        .ok_or_else(|| StoreError::Corruption("raft log range end overflowed".to_owned()))?,
    ),
    Bound::Excluded(index) => Some(*index),
    Bound::Unbounded => None,
  };
  Ok((start, end))
}
