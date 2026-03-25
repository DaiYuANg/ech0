mod catalog;
mod consensus;

use std::{
  collections::{BTreeMap, HashMap},
  sync::RwLock,
};

use crate::{
  Result,
  model::{
    ConsumerGroupAssignment, ConsumerGroupMember, LocalPartitionState, Record, TopicConfig,
    TopicPartition,
  },
  traits::{
    ConsensusLogStore, ConsensusMetadataStore, ConsumerGroupStore, LocalPartitionStateStore,
    MessageLogStore, MutablePartitionLogStore, OffsetStore, TopicCatalogStore,
  },
};

#[derive(Debug, Default)]
pub struct InMemoryStore {
  topics: RwLock<HashMap<TopicPartition, Vec<Record>>>,
  topic_configs: RwLock<HashMap<String, TopicConfig>>,
  consumer_offsets: RwLock<HashMap<(String, TopicPartition), u64>>,
  local_partition_states: RwLock<HashMap<TopicPartition, LocalPartitionState>>,
  consumer_group_members: RwLock<HashMap<(String, String), ConsumerGroupMember>>,
  consumer_group_assignments: RwLock<HashMap<String, ConsumerGroupAssignment>>,
  consensus_metadata: RwLock<HashMap<(String, String), Vec<u8>>>,
  consensus_logs: RwLock<HashMap<String, BTreeMap<u64, Vec<u8>>>>,
  consensus_purged_indices: RwLock<HashMap<String, u64>>,
}

impl InMemoryStore {
  pub fn new() -> Self {
    Self::default()
  }
}
