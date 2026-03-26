mod executor;
mod model;
#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::{
  Codec, JsonCodec, LocalPartitionStateStore, MessageLogStore, MutablePartitionLogStore, Result,
  StoreError,
  model::{LocalPartitionState, PartitionAvailability, RecordAppend, TopicPartition, now_ms},
};

static COMMAND_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub use executor::{LocalPartitionCommandExecutor, LocalPartitionCommandHandler};
pub use model::{
  ApplyResult, CommandReplicationScope, CommandSource, LocalPartitionCommand,
  PartitionCommandEnvelope, ReplicatedPartitionCommand, ReplicatedPartitionCommandEnvelope,
};

fn next_command_id() -> String {
  let seq = COMMAND_SEQUENCE.fetch_add(1, Ordering::Relaxed);
  format!("cmd-{:020}-{:08}", now_ms(), seq)
}
