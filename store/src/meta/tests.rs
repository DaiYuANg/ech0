use std::time::{SystemTime, UNIX_EPOCH};

use super::*;
use crate::model::{PartitionAvailability, PartitionState};

fn temp_path(name: &str) -> PathBuf {
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_nanos();
  std::env::temp_dir().join(format!("ech0-meta-{name}-{nanos}.redb"))
}

#[test]
fn local_partition_states_round_trip_in_sorted_order() {
  let path = temp_path("local-partition-state");
  let store = RedbMetadataStore::create(path).unwrap();

  let second = LocalPartitionState {
    topic_partition: TopicPartition::new("orders", 1),
    availability: PartitionAvailability::Recovering,
    state: PartitionState {
      leader_epoch: 3,
      high_watermark: Some(8),
      last_appended_offset: Some(10),
    },
  };
  let first = LocalPartitionState::online(TopicPartition::new("orders", 0), Some(4));

  store.save_local_partition_state(&second).unwrap();
  store.save_local_partition_state(&first).unwrap();

  let listed = store.list_local_partition_states().unwrap();
  assert_eq!(listed, vec![first.clone(), second.clone()]);
  assert_eq!(
    store
      .load_local_partition_state(&TopicPartition::new("orders", 1))
      .unwrap(),
    Some(second)
  );
}

#[test]
fn consensus_metadata_and_logs_round_trip() {
  let path = temp_path("consensus");
  let store = RedbMetadataStore::create(path).unwrap();

  store
    .save_consensus_value("raft:test", "vote", br#"{"term":3}"#)
    .unwrap();
  store
    .append_consensus_entry("raft:test", 2, br#"{"index":2}"#)
    .unwrap();
  store
    .append_consensus_entry("raft:test", 3, br#"{"index":3}"#)
    .unwrap();
  store
    .append_consensus_entry("raft:other", 1, br#"{"index":1}"#)
    .unwrap();

  assert_eq!(
    store
      .load_consensus_value("raft:test", "vote")
      .unwrap()
      .as_deref(),
    Some(br#"{"term":3}"#.as_slice())
  );
  assert_eq!(
    store
      .load_consensus_entry("raft:test", 2)
      .unwrap()
      .as_deref(),
    Some(br#"{"index":2}"#.as_slice())
  );
  assert_eq!(
    store.load_consensus_entries("raft:test", 2, None).unwrap(),
    vec![
      (2, br#"{"index":2}"#.to_vec()),
      (3, br#"{"index":3}"#.to_vec()),
    ]
  );
  assert_eq!(
    store.load_last_consensus_index("raft:test").unwrap(),
    Some(3)
  );

  store.truncate_consensus_from("raft:test", 3).unwrap();
  assert_eq!(
    store.load_consensus_entries("raft:test", 0, None).unwrap(),
    vec![(2, br#"{"index":2}"#.to_vec())]
  );

  store.purge_consensus_to("raft:test", 2).unwrap();
  assert!(
    store
      .load_consensus_entries("raft:test", 0, None)
      .unwrap()
      .is_empty()
  );
  assert_eq!(
    store.load_purged_consensus_index("raft:test").unwrap(),
    Some(2)
  );
  assert_eq!(
    store.load_consensus_entries("raft:other", 0, None).unwrap(),
    vec![(1, br#"{"index":1}"#.to_vec())]
  );
}
