use std::{
  path::PathBuf,
  sync::Arc,
  thread,
  time::{Duration, SystemTime, UNIX_EPOCH},
};

use openraft::{
  LogId, RaftLogReader, Vote,
  entry::RaftEntry,
  storage::{RaftLogStorage, RaftLogStorageExt},
};
use store::RedbMetadataStore;
use store::{
  ApplyResult, CommandSource, InMemoryStore, LocalPartitionCommand, LocalPartitionCommandExecutor,
  LocalPartitionStateMachine, MessageLogStore, OffsetStore, PartitionCommandEnvelope, RecordAppend,
  TopicConfig, TopicPartition,
};

use super::{
  BrokerRaftRuntime, EchoRaftTypeConfig, OpenRaftEntryPayload,
  OpenRaftPartitionStateMachineAdapter, OpenRaftRuntimeConfig, storage::BrokerRaftLogStore,
};
use crate::config::AppConfig;

fn alloc_addr() -> String {
  let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap();
  drop(listener);
  addr.to_string()
}

fn temp_metadata_path(name: &str) -> PathBuf {
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_nanos();
  std::env::temp_dir().join(format!("ech0-broker-raft-{name}-{nanos}.redb"))
}

fn make_app(node_id: u64, bind_addr: String, cluster: &[(u64, String)]) -> AppConfig {
  let mut app = AppConfig::default();
  app.broker.node_id = node_id;
  app.raft.bind_addr = bind_addr;
  app.raft.cluster = cluster
    .iter()
    .map(|(id, addr)| crate::config::RaftPeerConfig {
      node_id: *id,
      addr: addr.clone(),
    })
    .collect();
  app
}

#[test]
fn openraft_runtime_config_maps_cluster_nodes() {
  let mut app = AppConfig::default();
  app.broker.node_id = 2;
  app.raft.bind_addr = "127.0.0.1:4210".to_owned();
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
  assert_eq!(runtime.bind_addr, "127.0.0.1:4210");
  assert_eq!(runtime.known_nodes.len(), 2);
  assert_eq!(runtime.known_nodes.get(&1).unwrap().addr, "127.0.0.1:3210");
}

#[test]
fn redb_backed_raft_log_store_restores_vote_and_entries() {
  let path = temp_metadata_path("log-store");
  let meta = Arc::new(RedbMetadataStore::create(&path).unwrap());
  let group = "raft:test".to_owned();
  let log_id = LogId::new(openraft::CommittedLeaderId::new(3, 1), 7);
  let vote = Vote::new(3, 1);

  let runtime = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap();

  runtime.block_on(async {
    let mut store = BrokerRaftLogStore::new(group.clone(), Arc::clone(&meta));
    store.save_vote(&vote).await.unwrap();
    store.save_committed(Some(log_id.clone())).await.unwrap();
    store
      .blocking_append(vec![openraft::Entry::<EchoRaftTypeConfig>::new_blank(
        log_id.clone(),
      )])
      .await
      .unwrap();
  });

  runtime.block_on(async {
    let mut reopened = BrokerRaftLogStore::new(group.clone(), meta);
    assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
    assert_eq!(
      reopened.read_committed().await.unwrap(),
      Some(log_id.clone())
    );

    let state = reopened.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, None);
    assert_eq!(state.last_log_id, Some(log_id.clone()));

    let entries = reopened.try_get_log_entries(7..=7).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].log_id, log_id);
  });
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
      record: RecordAppend::new(b"hello".to_vec()),
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
  assert!(matches!(
    applied.result,
    store::ApplyResult::Appended {
      next_offset: 1,
      records
    } if records.len() == 1 && records[0].offset == 0 && records[0].payload.as_ref() == b"hello"
  ));
}

#[test]
fn single_node_runtime_applies_real_client_writes() {
  let app = AppConfig::default();
  let runtime_config = OpenRaftRuntimeConfig::from_app_config(&app).unwrap();
  let log = Arc::new(InMemoryStore::new());
  let meta = Arc::new(InMemoryStore::new());
  let runtime =
    BrokerRaftRuntime::new(runtime_config, Arc::clone(&log), Arc::clone(&meta)).unwrap();

  runtime.ensure_topic(TopicConfig::new("orders")).unwrap();

  let tp = TopicPartition::new("orders", 0);
  let envelope = PartitionCommandEnvelope::new(
    LocalPartitionCommand::Append {
      topic_partition: tp.clone(),
      record: RecordAppend::new(b"hello".to_vec()),
    },
    CommandSource::Client,
  )
  .replicated_command()
  .unwrap();

  let applied = runtime.apply_partition(envelope).unwrap();
  assert!(matches!(
    applied,
    ApplyResult::Appended {
      next_offset: 1,
      records
    } if records.len() == 1 && records[0].offset == 0 && records[0].payload.as_ref() == b"hello"
  ));
  assert_eq!(log.read_from(&tp, 0, 10).unwrap().len(), 1);

  runtime.save_consumer_offset("c1", &tp, 1).unwrap();
  assert_eq!(meta.load_consumer_offset("c1", &tp).unwrap(), Some(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_runtime_can_be_initialized_inside_tokio_runtime() {
  let app = AppConfig::default();
  let runtime_config = OpenRaftRuntimeConfig::from_app_config(&app).unwrap();
  let log = Arc::new(InMemoryStore::new());
  let meta = Arc::new(InMemoryStore::new());
  let runtime =
    BrokerRaftRuntime::new(runtime_config, Arc::clone(&log), Arc::clone(&meta)).unwrap();

  runtime.ensure_topic(TopicConfig::new("orders")).unwrap();

  let tp = TopicPartition::new("orders", 0);
  let envelope = PartitionCommandEnvelope::new(
    LocalPartitionCommand::Append {
      topic_partition: tp.clone(),
      record: RecordAppend::new(b"hello".to_vec()),
    },
    CommandSource::Client,
  )
  .replicated_command()
  .unwrap();

  let applied = runtime.apply_partition(envelope).unwrap();
  assert!(matches!(
    applied,
    ApplyResult::Appended {
      next_offset: 1,
      records
    } if records.len() == 1 && records[0].offset == 0 && records[0].payload.as_ref() == b"hello"
  ));
  assert_eq!(log.read_from(&tp, 0, 10).unwrap().len(), 1);
}

#[test]
fn multi_node_runtime_replicates_over_tcp() {
  let addr1 = alloc_addr();
  let addr2 = alloc_addr();
  let addr3 = alloc_addr();
  let cluster = vec![
    (1_u64, addr1.clone()),
    (2_u64, addr2.clone()),
    (3_u64, addr3.clone()),
  ];

  let node1 = BrokerRaftRuntime::new(
    OpenRaftRuntimeConfig::from_app_config(&make_app(1, addr1.clone(), &cluster)).unwrap(),
    Arc::new(InMemoryStore::new()),
    Arc::new(InMemoryStore::new()),
  )
  .unwrap();
  let node2 = BrokerRaftRuntime::new(
    OpenRaftRuntimeConfig::from_app_config(&make_app(2, addr2.clone(), &cluster)).unwrap(),
    Arc::new(InMemoryStore::new()),
    Arc::new(InMemoryStore::new()),
  )
  .unwrap();
  let log3 = Arc::new(InMemoryStore::new());
  let meta3 = Arc::new(InMemoryStore::new());
  let node3 = BrokerRaftRuntime::new(
    OpenRaftRuntimeConfig::from_app_config(&make_app(3, addr3.clone(), &cluster)).unwrap(),
    Arc::clone(&log3),
    Arc::clone(&meta3),
  )
  .unwrap();

  let mut leader = None;
  for _ in 0..50 {
    for node in [&node1, &node2, &node3] {
      if let Some(node_id) = node.current_leader().unwrap() {
        leader = Some(node_id);
        break;
      }
    }
    if leader.is_some() {
      break;
    }
    thread::sleep(Duration::from_millis(100));
  }

  let leader = leader.expect("expected an elected leader");
  let leader_node = match leader {
    1 => &node1,
    2 => &node2,
    3 => &node3,
    other => panic!("unexpected leader node id: {other}"),
  };

  leader_node
    .ensure_topic(TopicConfig::new("orders"))
    .unwrap();
  let tp = TopicPartition::new("orders", 0);
  let envelope = PartitionCommandEnvelope::new(
    LocalPartitionCommand::Append {
      topic_partition: tp.clone(),
      record: RecordAppend::new(b"hello".to_vec()),
    },
    CommandSource::Client,
  )
  .replicated_command()
  .unwrap();

  leader_node.apply_partition(envelope).unwrap();
  leader_node.save_consumer_offset("g1", &tp, 1).unwrap();

  let mut replicated = false;
  for _ in 0..50 {
    if log3.read_from(&tp, 0, 10).unwrap_or_default().len() == 1
      && meta3.load_consumer_offset("g1", &tp).unwrap() == Some(1)
    {
      replicated = true;
      break;
    }
    thread::sleep(Duration::from_millis(100));
  }

  assert!(replicated);
}
