use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use openraft::{BasicNode, Config as OpenRaftConfig};
use serde::{Deserialize, Serialize};
use store::{
  AppliedPartitionCommand, ApplyResult, PartitionStateMachine, ReplicatedPartitionCommandEnvelope,
  TopicConfig, TopicPartition,
};

use crate::config::{AppConfig, RaftPeerConfig, RaftReadPolicy};

mod network;
mod runtime;
mod storage;
#[cfg(test)]
mod tests;

pub use runtime::BrokerRaftRuntime;

openraft::declare_raft_types!(
    pub EchoRaftTypeConfig:
        D = BrokerRaftRequest,
        R = BrokerRaftResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<EchoRaftTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        Responder = openraft::impls::OneshotResponder<EchoRaftTypeConfig>,
        AsyncRuntime = openraft::TokioRuntime,
);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftNodeDescriptor {
  pub node_id: u64,
  pub addr: String,
}

impl From<&RaftPeerConfig> for RaftNodeDescriptor {
  fn from(value: &RaftPeerConfig) -> Self {
    Self {
      node_id: value.node_id,
      addr: value.addr.clone(),
    }
  }
}

impl RaftNodeDescriptor {
  pub fn basic_node(&self) -> BasicNode {
    BasicNode::new(self.addr.clone())
  }
}

#[derive(Debug, Clone)]
pub struct OpenRaftRuntimeConfig {
  pub node_id: u64,
  pub bind_addr: String,
  pub read_policy: RaftReadPolicy,
  pub config: Arc<OpenRaftConfig>,
  pub known_nodes: BTreeMap<u64, BasicNode>,
}

impl OpenRaftRuntimeConfig {
  pub fn from_app_config(app: &AppConfig) -> Result<Self, openraft::ConfigError> {
    let known_nodes = app
      .raft
      .cluster
      .iter()
      .map(|node| (node.node_id, BasicNode::new(node.addr.clone())))
      .collect::<BTreeMap<_, _>>();

    let config = OpenRaftConfig {
      cluster_name: app.broker.cluster_name.clone(),
      heartbeat_interval: app.raft.heartbeat_interval_ms,
      election_timeout_min: app.raft.election_timeout_min_ms,
      election_timeout_max: app.raft.election_timeout_max_ms,
      max_in_snapshot_log_to_keep: 1_024,
      snapshot_max_chunk_size: app.raft.snapshot_max_chunk_size,
      ..Default::default()
    }
    .validate()?;

    Ok(Self {
      node_id: app.broker.node_id,
      bind_addr: app.raft.bind_addr.clone(),
      read_policy: app.raft.read_policy,
      config: Arc::new(config),
      known_nodes,
    })
  }

  pub fn network_enabled(&self) -> bool {
    self.known_nodes.len() > 1
  }

  pub fn consensus_group(&self) -> String {
    format!("raft:{}", self.config.cluster_name)
  }
}

#[derive(Debug)]
pub struct OpenRaftPartitionStateMachineAdapter<SM> {
  inner: SM,
}

impl<SM> OpenRaftPartitionStateMachineAdapter<SM> {
  pub fn new(inner: SM) -> Self {
    Self { inner }
  }

  pub fn inner(&self) -> &SM {
    &self.inner
  }
}

impl<SM> OpenRaftPartitionStateMachineAdapter<SM>
where
  SM: PartitionStateMachine,
{
  pub fn apply_replicated_entry(
    &self,
    envelope: ReplicatedPartitionCommandEnvelope,
  ) -> store::Result<AppliedPartitionCommand> {
    self.inner.apply_replicated_envelope(envelope)
  }

  pub fn decode_and_apply_json(&self, bytes: &[u8]) -> store::Result<AppliedPartitionCommand> {
    let envelope = ReplicatedPartitionCommandEnvelope::decode_json(bytes)?;
    self.apply_replicated_entry(envelope)
  }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenRaftEntryPayload {
  pub envelope: ReplicatedPartitionCommandEnvelope,
}

impl OpenRaftEntryPayload {
  pub fn encode_json(&self) -> store::Result<Vec<u8>> {
    serde_json::to_vec(self).map_err(|err| {
      store::StoreError::Codec(format!("failed to encode raft entry payload: {err}"))
    })
  }

  pub fn decode_json(bytes: &[u8]) -> store::Result<Self> {
    serde_json::from_slice(bytes).map_err(|err| {
      store::StoreError::Codec(format!("failed to decode raft entry payload: {err}"))
    })
  }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerRaftRequest {
  EnsureTopic {
    topic: TopicConfig,
  },
  ApplyPartition {
    envelope: ReplicatedPartitionCommandEnvelope,
  },
  SaveConsumerOffset {
    consumer: String,
    topic_partition: TopicPartition,
    next_offset: u64,
  },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerRaftResponse {
  Noop,
  MembershipChanged,
  TopicEnsured,
  PartitionApplied { result: ApplyResult },
  ConsumerOffsetSaved,
}
