use super::*;

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
