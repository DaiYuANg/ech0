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

#[test]
fn consumer_group_members_round_trip_and_expiration_sweep() {
  let path = temp_path("consumer-group-members");
  let store = RedbMetadataStore::create(path).unwrap();

  let active = crate::ConsumerGroupMember {
    group: "orders-cg".to_owned(),
    member_id: "member-a".to_owned(),
    topics: vec!["orders".to_owned()],
    session_timeout_ms: 30_000,
    joined_at_ms: 1_000,
    last_heartbeat_ms: 2_000,
  };
  let expired = crate::ConsumerGroupMember {
    group: "orders-cg".to_owned(),
    member_id: "member-b".to_owned(),
    topics: vec!["orders".to_owned()],
    session_timeout_ms: 10,
    joined_at_ms: 1_500,
    last_heartbeat_ms: 2_000,
  };

  store.save_group_member(&active).unwrap();
  store.save_group_member(&expired).unwrap();

  let listed = store.list_group_members("orders-cg").unwrap();
  assert_eq!(listed.len(), 2);
  assert_eq!(
    store.load_group_member("orders-cg", "member-a").unwrap(),
    Some(active.clone())
  );

  let removed = store.delete_expired_group_members(2_020).unwrap();
  assert_eq!(removed, 1);
  let listed_after_sweep = store.list_group_members("orders-cg").unwrap();
  assert_eq!(listed_after_sweep, vec![active]);

  store.delete_group_member("orders-cg", "member-a").unwrap();
  assert!(
    store
      .load_group_member("orders-cg", "member-a")
      .unwrap()
      .is_none()
  );
}

#[test]
fn consumer_group_assignment_round_trip() {
  let path = temp_path("consumer-group-assignment");
  let store = RedbMetadataStore::create(path).unwrap();
  let assignment = crate::ConsumerGroupAssignment {
    group: "orders-cg".to_owned(),
    generation: 3,
    updated_at_ms: 12_345,
    assignments: vec![
      crate::GroupPartitionAssignment {
        member_id: "member-a".to_owned(),
        topic: "orders".to_owned(),
        partition: 0,
      },
      crate::GroupPartitionAssignment {
        member_id: "member-b".to_owned(),
        topic: "orders".to_owned(),
        partition: 1,
      },
    ],
  };

  store.save_group_assignment(&assignment).unwrap();
  assert_eq!(
    store.load_group_assignment("orders-cg").unwrap(),
    Some(assignment)
  );
}
