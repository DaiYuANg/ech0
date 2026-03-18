use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use openraft::{BasicNode, Config as OpenRaftConfig};
use serde::{Deserialize, Serialize};
use store::{AppliedPartitionCommand, PartitionStateMachine, ReplicatedPartitionCommandEnvelope};

use crate::config::{AppConfig, RaftPeerConfig};

openraft::declare_raft_types!(
    pub EchoRaftTypeConfig:
        D = ReplicatedPartitionCommandEnvelope,
        R = store::ApplyResult,
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
      config: Arc::new(config),
      known_nodes,
    })
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::config::AppConfig;
  use store::{
    CommandSource, InMemoryStore, LocalPartitionCommand, LocalPartitionCommandExecutor,
    LocalPartitionStateMachine, PartitionStateMachine, TopicConfig, TopicPartition,
  };

  #[test]
  fn openraft_runtime_config_maps_cluster_nodes() {
    let mut app = AppConfig::default();
    app.broker.node_id = 2;
    app.raft.cluster = vec![
      crate::config::RaftPeerConfig {
        node_id: 1,
        addr: "127.0.0.1:3210".to_owned(),
      },
      crate::config::RaftPeerConfig {
        node_id: 2,
        addr: "127.0.0.1:3211".to_owned(),
      },
    ];

    let runtime = OpenRaftRuntimeConfig::from_app_config(&app).unwrap();
    assert_eq!(runtime.node_id, 2);
    assert_eq!(runtime.known_nodes.len(), 2);
    assert_eq!(runtime.known_nodes.get(&1).unwrap().addr, "127.0.0.1:3210");
  }

  #[test]
  fn replicated_entry_payload_round_trip_and_apply() {
    let log = InMemoryStore::new();
    let state = InMemoryStore::new();
    log.create_topic(TopicConfig::new("orders")).unwrap();
    let machine = LocalPartitionStateMachine::new(LocalPartitionCommandExecutor::new(log, state));
    let adapter = OpenRaftPartitionStateMachineAdapter::new(machine);
    let tp = TopicPartition::new("orders", 0);

    let local = store::PartitionCommandEnvelope::new(
      LocalPartitionCommand::Append {
        topic_partition: tp.clone(),
        payload: b"hello".to_vec(),
      },
      CommandSource::Consensus,
    );
    let replicated = local.replicated_command().unwrap();
    let payload = OpenRaftEntryPayload {
      envelope: replicated,
    };
    let wire = payload.encode_json().unwrap();
    let decoded = OpenRaftEntryPayload::decode_json(&wire).unwrap();
    let applied = adapter.apply_replicated_entry(decoded.envelope).unwrap();

    assert_eq!(applied.topic_partition, tp);
    assert_eq!(
      applied.result,
      store::ApplyResult::Appended { next_offset: 1 }
    );
  }
}
