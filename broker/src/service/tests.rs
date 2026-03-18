use crate::config::{AppConfig, RaftReadPolicy};
use store::{InMemoryStore, StoreError};

use super::{BrokerIdentity, BrokerRuntimeMode, BrokerService};

fn raft_mode() -> BrokerRuntimeMode {
  raft_mode_with_policy(RaftReadPolicy::Local)
}

fn raft_mode_with_policy(read_policy: RaftReadPolicy) -> BrokerRuntimeMode {
  let mut app = AppConfig::default();
  app.raft.read_policy = read_policy;
  BrokerRuntimeMode::Raft(crate::raft::OpenRaftRuntimeConfig::from_app_config(&app).unwrap())
}

#[test]
fn fetch_uses_committed_offset_when_request_offset_missing() {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
  )
  .unwrap();

  service.create_topic("orders", 1).unwrap();
  service.publish("orders", 0, b"a".to_vec()).unwrap();
  service.publish("orders", 0, b"b".to_vec()).unwrap();
  service.commit_offset("c1", "orders", 0, 1).unwrap();

  let fetched = service.fetch("c1", "orders", 0, None, 10).unwrap();

  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].offset, 1);
  assert_eq!(fetched.next_offset, 2);
}

#[test]
fn create_topic_is_visible_in_catalog() {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
  )
  .unwrap();

  service.create_topic("orders", 3).unwrap();

  let topics = service.list_topics().unwrap();
  assert_eq!(topics.len(), 1);
  assert_eq!(topics[0].name, "orders");
  assert_eq!(topics[0].partitions, 3);
}

#[test]
fn direct_messages_use_hidden_inbox_topics() {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
  )
  .unwrap();

  let sent = service
    .send_direct("alice", "bob", None, b"hello".to_vec())
    .unwrap();
  assert_eq!(sent.offset, 0);

  let fetched = service.fetch_inbox("bob", 10).unwrap();
  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].message.sender, "alice");

  let topics = service.list_topics().unwrap();
  assert!(topics.is_empty());
}

#[test]
fn raft_mode_uses_replicated_write_path_for_queue_publish() {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new_with_mode(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
    raft_mode(),
  )
  .unwrap();

  assert!(service.runtime_mode().is_raft());
  service.create_topic("orders", 1).unwrap();
  service.publish("orders", 0, b"hello".to_vec()).unwrap();

  let fetched = service.fetch("c1", "orders", 0, Some(0), 10).unwrap();
  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].payload, b"hello".to_vec());
}

#[test]
fn raft_mode_persists_topic_catalog_and_consumer_offsets() {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new_with_mode(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
    raft_mode(),
  )
  .unwrap();

  service.create_topic("orders", 2).unwrap();
  service.publish("orders", 0, b"a".to_vec()).unwrap();
  service.publish("orders", 0, b"b".to_vec()).unwrap();
  service.commit_offset("g1", "orders", 0, 1).unwrap();

  let topics = service.list_topics().unwrap();
  assert_eq!(topics.len(), 1);
  assert_eq!(topics[0].name, "orders");
  assert_eq!(topics[0].partitions, 2);

  let fetched = service.fetch("g1", "orders", 0, None, 10).unwrap();
  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].offset, 1);
  assert_eq!(fetched.records[0].payload, b"b".to_vec());
}

#[test]
fn raft_mode_supports_direct_inbox_flow() {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new_with_mode(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
    raft_mode(),
  )
  .unwrap();

  service
    .send_direct("alice", "bob", None, b"hello".to_vec())
    .unwrap();

  let fetched = service.fetch_inbox("bob", 10).unwrap();
  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].message.sender, "alice");
  assert_eq!(fetched.records[0].message.payload, b"hello".to_vec());

  service.ack_direct("bob", fetched.next_offset).unwrap();
  let after_ack = service.fetch_inbox("bob", 10).unwrap();
  assert!(after_ack.records.is_empty());
}

#[test]
fn raft_leader_read_policy_allows_reads_on_single_node_leader() {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new_with_mode(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
    raft_mode_with_policy(RaftReadPolicy::Leader),
  )
  .unwrap();

  service.create_topic("orders", 1).unwrap();
  service.publish("orders", 0, b"hello".to_vec()).unwrap();

  let fetched = service.fetch("c1", "orders", 0, Some(0), 10).unwrap();
  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].payload, b"hello".to_vec());
}

#[test]
fn raft_linearizable_read_policy_allows_reads_on_single_node_leader() {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new_with_mode(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
    raft_mode_with_policy(RaftReadPolicy::Linearizable),
  )
  .unwrap();

  service.create_topic("orders", 1).unwrap();
  let topics = service.list_topics().unwrap();
  assert_eq!(topics.len(), 1);
  assert_eq!(topics[0].name, "orders");
}

#[test]
fn raft_leader_read_policy_rejects_non_leader_identity() {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new_with_mode(
    BrokerIdentity {
      node_id: 9,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
    raft_mode_with_policy(RaftReadPolicy::Leader),
  )
  .unwrap();

  service.create_topic("orders", 1).unwrap();

  let err = service.list_topics().unwrap_err();
  match err {
    StoreError::NotLeader { leader_id } => assert_eq!(leader_id, Some(1)),
    other => panic!("expected not leader error, got {other:?}"),
  }
}
