use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::{
  Codec, JsonCodec, LocalPartitionStateStore, MessageLogStore, MutablePartitionLogStore, Result,
  StoreError,
  model::{LocalPartitionState, PartitionAvailability, TopicPartition, now_ms},
};

static COMMAND_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandReplicationScope {
  Replicated,
  LocalOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandSource {
  Client,
  Recovery,
  Metadata,
  Consensus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalPartitionCommand {
  Append {
    topic_partition: TopicPartition,
    payload: Vec<u8>,
  },
  Truncate {
    topic_partition: TopicPartition,
    offset: u64,
  },
  UpdateAvailability {
    topic_partition: TopicPartition,
    availability: PartitionAvailability,
  },
  RecoverPartition {
    topic_partition: TopicPartition,
  },
}

impl LocalPartitionCommand {
  pub fn topic_partition(&self) -> &TopicPartition {
    match self {
      Self::Append {
        topic_partition, ..
      }
      | Self::Truncate {
        topic_partition, ..
      }
      | Self::UpdateAvailability {
        topic_partition, ..
      }
      | Self::RecoverPartition { topic_partition } => topic_partition,
    }
  }

  pub fn replication_scope(&self) -> CommandReplicationScope {
    match self {
      Self::Append { .. } | Self::Truncate { .. } => CommandReplicationScope::Replicated,
      Self::UpdateAvailability { .. } | Self::RecoverPartition { .. } => {
        CommandReplicationScope::LocalOnly
      }
    }
  }

  pub fn is_replicated_command(&self) -> bool {
    self.replication_scope() == CommandReplicationScope::Replicated
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCommandEnvelope {
  pub command_id: String,
  pub created_at_ms: u64,
  pub source: CommandSource,
  pub leader_epoch: Option<u64>,
  pub command: LocalPartitionCommand,
}

impl PartitionCommandEnvelope {
  pub fn new(command: LocalPartitionCommand, source: CommandSource) -> Self {
    Self {
      command_id: next_command_id(),
      created_at_ms: now_ms(),
      source,
      leader_epoch: None,
      command,
    }
  }

  pub fn with_leader_epoch(mut self, leader_epoch: u64) -> Self {
    self.leader_epoch = Some(leader_epoch);
    self
  }

  pub fn replication_scope(&self) -> CommandReplicationScope {
    self.command.replication_scope()
  }

  pub fn encode_json(&self) -> Result<Vec<u8>> {
    JsonCodec::<PartitionCommandEnvelope>::new().encode(self)
  }

  pub fn decode_json(bytes: &[u8]) -> Result<Self> {
    JsonCodec::<PartitionCommandEnvelope>::new().decode(bytes)
  }

  pub fn replicated_command(self) -> Result<ReplicatedPartitionCommandEnvelope> {
    if self.replication_scope() != CommandReplicationScope::Replicated {
      return Err(StoreError::Unsupported(
        "local-only partition commands must not enter consensus replication",
      ));
    }

    Ok(ReplicatedPartitionCommandEnvelope {
      command_id: self.command_id,
      created_at_ms: self.created_at_ms,
      source: self.source,
      leader_epoch: self.leader_epoch,
      command: match self.command {
        LocalPartitionCommand::Append {
          topic_partition,
          payload,
        } => ReplicatedPartitionCommand::Append {
          topic_partition,
          payload,
        },
        LocalPartitionCommand::Truncate {
          topic_partition,
          offset,
        } => ReplicatedPartitionCommand::Truncate {
          topic_partition,
          offset,
        },
        LocalPartitionCommand::UpdateAvailability { .. }
        | LocalPartitionCommand::RecoverPartition { .. } => {
          return Err(StoreError::Unsupported(
            "local-only partition commands must not enter consensus replication",
          ));
        }
      },
    })
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicatedPartitionCommand {
  Append {
    topic_partition: TopicPartition,
    payload: Vec<u8>,
  },
  Truncate {
    topic_partition: TopicPartition,
    offset: u64,
  },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedPartitionCommandEnvelope {
  pub command_id: String,
  pub created_at_ms: u64,
  pub source: CommandSource,
  pub leader_epoch: Option<u64>,
  pub command: ReplicatedPartitionCommand,
}

impl ReplicatedPartitionCommandEnvelope {
  pub fn topic_partition(&self) -> &TopicPartition {
    match &self.command {
      ReplicatedPartitionCommand::Append {
        topic_partition, ..
      }
      | ReplicatedPartitionCommand::Truncate {
        topic_partition, ..
      } => topic_partition,
    }
  }

  pub fn encode_json(&self) -> Result<Vec<u8>> {
    JsonCodec::<ReplicatedPartitionCommandEnvelope>::new().encode(self)
  }

  pub fn decode_json(bytes: &[u8]) -> Result<Self> {
    JsonCodec::<ReplicatedPartitionCommandEnvelope>::new().decode(bytes)
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyResult {
  Appended { next_offset: u64 },
  Truncated { next_offset: u64 },
  AvailabilityUpdated { state: LocalPartitionState },
  PartitionRecovered { state: LocalPartitionState },
}

pub trait LocalPartitionCommandHandler {
  fn apply(&self, command: LocalPartitionCommand) -> Result<ApplyResult>;
  fn apply_envelope(&self, envelope: PartitionCommandEnvelope) -> Result<ApplyResult> {
    self.apply(envelope.command)
  }
}

#[derive(Debug)]
pub struct LocalPartitionCommandExecutor<L, S> {
  log_store: L,
  state_store: S,
}

impl<L, S> LocalPartitionCommandExecutor<L, S> {
  pub fn new(log_store: L, state_store: S) -> Self {
    Self {
      log_store,
      state_store,
    }
  }

  pub fn stores(&self) -> (&L, &S) {
    (&self.log_store, &self.state_store)
  }
}

impl<L, S> LocalPartitionCommandHandler for LocalPartitionCommandExecutor<L, S>
where
  L: MessageLogStore + MutablePartitionLogStore,
  S: LocalPartitionStateStore,
{
  fn apply(&self, command: LocalPartitionCommand) -> Result<ApplyResult> {
    match command {
      LocalPartitionCommand::Append {
        topic_partition,
        payload,
      } => {
        let record = self
          .log_store
          .append(&topic_partition, payload.as_slice())?;
        let state = self.log_store.local_partition_state(&topic_partition)?;
        self.state_store.save_local_partition_state(&state)?;
        Ok(ApplyResult::Appended {
          next_offset: record.offset + 1,
        })
      }
      LocalPartitionCommand::Truncate {
        topic_partition,
        offset,
      } => {
        self.log_store.truncate_from(&topic_partition, offset)?;
        let state = self.log_store.local_partition_state(&topic_partition)?;
        self.state_store.save_local_partition_state(&state)?;
        Ok(ApplyResult::Truncated {
          next_offset: offset,
        })
      }
      LocalPartitionCommand::UpdateAvailability {
        topic_partition,
        availability,
      } => {
        let mut state = self
          .state_store
          .load_local_partition_state(&topic_partition)?
          .unwrap_or_else(|| LocalPartitionState::online(topic_partition.clone(), None));
        state.availability = availability;
        self.state_store.save_local_partition_state(&state)?;
        Ok(ApplyResult::AvailabilityUpdated { state })
      }
      LocalPartitionCommand::RecoverPartition { topic_partition } => {
        let mut state = self.log_store.local_partition_state(&topic_partition)?;
        state.availability = PartitionAvailability::Recovering;
        self.state_store.save_local_partition_state(&state)?;
        let mut recovered = self.log_store.local_partition_state(&topic_partition)?;
        recovered.availability = PartitionAvailability::Online;
        self.state_store.save_local_partition_state(&recovered)?;
        Ok(ApplyResult::PartitionRecovered { state: recovered })
      }
    }
  }
}

fn next_command_id() -> String {
  let seq = COMMAND_SEQUENCE.fetch_add(1, Ordering::Relaxed);
  format!("cmd-{:020}-{:08}", now_ms(), seq)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    InMemoryStore, LocalPartitionStateStore, MessageLogStore, PartitionAvailability, TopicConfig,
  };

  #[test]
  fn append_command_updates_local_partition_state() {
    let log = InMemoryStore::new();
    let states = InMemoryStore::new();
    log.create_topic(TopicConfig::new("orders")).unwrap();
    let executor = LocalPartitionCommandExecutor::new(log, states);
    let tp = TopicPartition::new("orders", 0);

    let result = executor
      .apply(LocalPartitionCommand::Append {
        topic_partition: tp.clone(),
        payload: b"hello".to_vec(),
      })
      .unwrap();

    assert_eq!(result, ApplyResult::Appended { next_offset: 1 });
    assert_eq!(executor.stores().0.last_offset(&tp).unwrap(), Some(0));
    let state = executor
      .stores()
      .1
      .load_local_partition_state(&tp)
      .unwrap()
      .unwrap();
    assert_eq!(state.availability, PartitionAvailability::Online);
    assert_eq!(state.state.last_appended_offset, Some(0));
  }

  #[test]
  fn truncate_command_rewrites_partition_state() {
    let log = InMemoryStore::new();
    let states = InMemoryStore::new();
    let mut topic = TopicConfig::new("orders");
    topic.partitions = 2;
    log.create_topic(topic).unwrap();
    let executor = LocalPartitionCommandExecutor::new(log, states);
    let tp = TopicPartition::new("orders", 1);

    executor
      .apply(LocalPartitionCommand::Append {
        topic_partition: tp.clone(),
        payload: b"a".to_vec(),
      })
      .unwrap();
    executor
      .apply(LocalPartitionCommand::Append {
        topic_partition: tp.clone(),
        payload: b"b".to_vec(),
      })
      .unwrap();

    let result = executor
      .apply(LocalPartitionCommand::Truncate {
        topic_partition: tp.clone(),
        offset: 1,
      })
      .unwrap();

    assert_eq!(result, ApplyResult::Truncated { next_offset: 1 });
    assert_eq!(executor.stores().0.last_offset(&tp).unwrap(), Some(0));
    let state = executor
      .stores()
      .1
      .load_local_partition_state(&tp)
      .unwrap()
      .unwrap();
    assert_eq!(state.state.high_watermark, Some(0));
  }

  #[test]
  fn envelope_json_round_trip_preserves_replicated_command_shape() {
    let envelope = PartitionCommandEnvelope::new(
      LocalPartitionCommand::Append {
        topic_partition: TopicPartition::new("orders", 2),
        payload: b"hello".to_vec(),
      },
      CommandSource::Consensus,
    )
    .with_leader_epoch(7);

    let bytes = envelope.encode_json().unwrap();
    let decoded = PartitionCommandEnvelope::decode_json(&bytes).unwrap();

    assert_eq!(decoded.source, CommandSource::Consensus);
    assert_eq!(decoded.leader_epoch, Some(7));
    assert!(decoded.command.is_replicated_command());
    assert_eq!(
      decoded.command.topic_partition(),
      &TopicPartition::new("orders", 2)
    );
  }

  #[test]
  fn local_only_commands_are_rejected_from_replicated_envelopes() {
    let envelope = PartitionCommandEnvelope::new(
      LocalPartitionCommand::RecoverPartition {
        topic_partition: TopicPartition::new("orders", 0),
      },
      CommandSource::Recovery,
    );

    let err = envelope.replicated_command().unwrap_err();
    assert!(matches!(err, StoreError::Unsupported(_)));
  }
}
