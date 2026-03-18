use crate::{
  ApplyResult, CommandReplicationScope, CommandSource, LocalPartitionCommand,
  LocalPartitionCommandExecutor, LocalPartitionCommandHandler, ReplicatedPartitionCommand,
  ReplicatedPartitionCommandEnvelope, Result,
  model::{TopicPartition, now_ms},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionApplyContext {
  pub command_id: String,
  pub created_at_ms: u64,
  pub applied_at_ms: u64,
  pub source: CommandSource,
  pub leader_epoch: Option<u64>,
  pub replication_scope: CommandReplicationScope,
}

impl PartitionApplyContext {
  pub fn from_local_envelope(envelope: &crate::PartitionCommandEnvelope) -> Self {
    Self {
      command_id: envelope.command_id.clone(),
      created_at_ms: envelope.created_at_ms,
      applied_at_ms: now_ms(),
      source: envelope.source,
      leader_epoch: envelope.leader_epoch,
      replication_scope: envelope.replication_scope(),
    }
  }

  pub fn from_replicated_envelope(envelope: &ReplicatedPartitionCommandEnvelope) -> Self {
    Self {
      command_id: envelope.command_id.clone(),
      created_at_ms: envelope.created_at_ms,
      applied_at_ms: now_ms(),
      source: envelope.source,
      leader_epoch: envelope.leader_epoch,
      replication_scope: CommandReplicationScope::Replicated,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedPartitionCommand {
  pub topic_partition: TopicPartition,
  pub result: ApplyResult,
  pub context: PartitionApplyContext,
}

pub trait PartitionStateMachine {
  fn apply_local_envelope(
    &self,
    envelope: crate::PartitionCommandEnvelope,
  ) -> Result<AppliedPartitionCommand>;

  fn apply_replicated_envelope(
    &self,
    envelope: ReplicatedPartitionCommandEnvelope,
  ) -> Result<AppliedPartitionCommand>;

  fn apply_local_command(
    &self,
    command: LocalPartitionCommand,
    source: CommandSource,
  ) -> Result<AppliedPartitionCommand> {
    self.apply_local_envelope(crate::PartitionCommandEnvelope::new(command, source))
  }
}

#[derive(Debug)]
pub struct LocalPartitionStateMachine<H> {
  handler: H,
}

impl<H> LocalPartitionStateMachine<H> {
  pub fn new(handler: H) -> Self {
    Self { handler }
  }

  pub fn handler(&self) -> &H {
    &self.handler
  }
}

impl<L, S> From<LocalPartitionCommandExecutor<L, S>>
  for LocalPartitionStateMachine<LocalPartitionCommandExecutor<L, S>>
{
  fn from(value: LocalPartitionCommandExecutor<L, S>) -> Self {
    Self::new(value)
  }
}

impl<H> PartitionStateMachine for LocalPartitionStateMachine<H>
where
  H: LocalPartitionCommandHandler,
{
  fn apply_local_envelope(
    &self,
    envelope: crate::PartitionCommandEnvelope,
  ) -> Result<AppliedPartitionCommand> {
    let topic_partition = envelope.command.topic_partition().clone();
    let context = PartitionApplyContext::from_local_envelope(&envelope);
    let result = self.handler.apply_envelope(envelope)?;
    Ok(AppliedPartitionCommand {
      topic_partition,
      result,
      context,
    })
  }

  fn apply_replicated_envelope(
    &self,
    envelope: ReplicatedPartitionCommandEnvelope,
  ) -> Result<AppliedPartitionCommand> {
    let topic_partition = envelope.topic_partition().clone();
    let context = PartitionApplyContext::from_replicated_envelope(&envelope);
    let result = self.handler.apply(replicated_to_local(envelope.command))?;
    Ok(AppliedPartitionCommand {
      topic_partition,
      result,
      context,
    })
  }
}

fn replicated_to_local(command: ReplicatedPartitionCommand) -> LocalPartitionCommand {
  match command {
    ReplicatedPartitionCommand::Append {
      topic_partition,
      payload,
    } => LocalPartitionCommand::Append {
      topic_partition,
      payload,
    },
    ReplicatedPartitionCommand::Truncate {
      topic_partition,
      offset,
    } => LocalPartitionCommand::Truncate {
      topic_partition,
      offset,
    },
  }
}

#[cfg(test)]
mod tests {
  use crate::{
    CommandSource, InMemoryStore, LocalPartitionCommand, LocalPartitionCommandExecutor,
    PartitionAvailability, PartitionCommandEnvelope, TopicConfig, TopicPartition,
  };

  use super::{LocalPartitionStateMachine, PartitionStateMachine};

  #[test]
  fn state_machine_applies_local_envelope_with_context() {
    let log = InMemoryStore::new();
    let states = InMemoryStore::new();
    log.create_topic(TopicConfig::new("orders")).unwrap();
    let state_machine =
      LocalPartitionStateMachine::new(LocalPartitionCommandExecutor::new(log, states));
    let tp = TopicPartition::new("orders", 0);
    let envelope = PartitionCommandEnvelope::new(
      LocalPartitionCommand::Append {
        topic_partition: tp.clone(),
        payload: b"hello".to_vec(),
      },
      CommandSource::Client,
    )
    .with_leader_epoch(3);

    let applied = state_machine.apply_local_envelope(envelope).unwrap();

    assert_eq!(applied.topic_partition, tp);
    assert_eq!(applied.context.source, CommandSource::Client);
    assert_eq!(applied.context.leader_epoch, Some(3));
    assert_eq!(
      applied.context.replication_scope,
      crate::CommandReplicationScope::Replicated
    );
    assert_eq!(
      applied.result,
      crate::ApplyResult::Appended { next_offset: 1 }
    );
  }

  #[test]
  fn state_machine_applies_replicated_envelope() {
    let log = InMemoryStore::new();
    let states = InMemoryStore::new();
    log.create_topic(TopicConfig::new("orders")).unwrap();
    let state_machine =
      LocalPartitionStateMachine::new(LocalPartitionCommandExecutor::new(log, states));
    let tp = TopicPartition::new("orders", 0);
    let local_envelope = PartitionCommandEnvelope::new(
      LocalPartitionCommand::Append {
        topic_partition: tp.clone(),
        payload: b"hello".to_vec(),
      },
      CommandSource::Consensus,
    )
    .with_leader_epoch(9);
    let replicated = local_envelope.replicated_command().unwrap();

    let applied = state_machine.apply_replicated_envelope(replicated).unwrap();

    assert_eq!(applied.topic_partition, tp);
    assert_eq!(applied.context.source, CommandSource::Consensus);
    assert_eq!(applied.context.leader_epoch, Some(9));
    assert_eq!(
      applied.context.replication_scope,
      crate::CommandReplicationScope::Replicated
    );
    assert_eq!(
      applied.result,
      crate::ApplyResult::Appended { next_offset: 1 }
    );
  }

  #[test]
  fn state_machine_applies_local_only_command_with_local_scope() {
    let log = InMemoryStore::new();
    let states = InMemoryStore::new();
    log.create_topic(TopicConfig::new("orders")).unwrap();
    let state_machine =
      LocalPartitionStateMachine::new(LocalPartitionCommandExecutor::new(log, states));
    let tp = TopicPartition::new("orders", 0);

    let applied = state_machine
      .apply_local_command(
        LocalPartitionCommand::UpdateAvailability {
          topic_partition: tp.clone(),
          availability: PartitionAvailability::Fenced,
        },
        CommandSource::Metadata,
      )
      .unwrap();

    assert_eq!(applied.topic_partition, tp);
    assert_eq!(
      applied.context.replication_scope,
      crate::CommandReplicationScope::LocalOnly
    );
    assert_eq!(applied.context.source, CommandSource::Metadata);
    match applied.result {
      crate::ApplyResult::AvailabilityUpdated { state } => {
        assert_eq!(state.availability, PartitionAvailability::Fenced);
      }
      other => panic!("unexpected result: {other:?}"),
    }
  }
}
