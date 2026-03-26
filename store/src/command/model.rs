use super::*;
use crate::Record;

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
    record: RecordAppend,
  },
  AppendBatch {
    topic_partition: TopicPartition,
    records: Vec<RecordAppend>,
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
      | Self::AppendBatch {
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
      Self::Append { .. } | Self::AppendBatch { .. } | Self::Truncate { .. } => {
        CommandReplicationScope::Replicated
      }
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
          record,
        } => ReplicatedPartitionCommand::Append {
          topic_partition,
          record,
        },
        LocalPartitionCommand::AppendBatch {
          topic_partition,
          records,
        } => ReplicatedPartitionCommand::AppendBatch {
          topic_partition,
          records,
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
    record: RecordAppend,
  },
  AppendBatch {
    topic_partition: TopicPartition,
    records: Vec<RecordAppend>,
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
      | ReplicatedPartitionCommand::AppendBatch {
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
  Appended { records: Vec<Record>, next_offset: u64 },
  Truncated { next_offset: u64 },
  AvailabilityUpdated { state: LocalPartitionState },
  PartitionRecovered { state: LocalPartitionState },
}
