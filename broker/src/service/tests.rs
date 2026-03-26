use crate::config::{AppConfig, RaftReadPolicy};
use scheduler::process_due_once;
use std::time::{SystemTime, UNIX_EPOCH};
use store::{
  InMemoryStore, MessageLogStore, RedbMetadataStore, SegmentLog, SegmentLogOptions, StoreError,
  TopicCleanupPolicy, TopicPartition, TopicRetryPolicy,
};

use super::{
  BrokerIdentity, BrokerRuntimeMode, BrokerService, GroupAssignmentStrategy,
  GroupCoordinatorOptions, TopicPolicyOverrides,
};

fn temp_path(name: &str) -> std::path::PathBuf {
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_nanos();
  std::env::temp_dir().join(format!("ech0-broker-service-{name}-{nanos}"))
}

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
fn nack_rewrites_record_to_retry_topic_with_headers() {
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
  service.publish("orders", 0, b"m1".to_vec()).unwrap();

  let retried = service
    .nack_and_retry("c1", "orders", 0, 0, Some("db timeout".to_owned()))
    .unwrap();
  assert_eq!(retried.retry_topic, "__retry.orders");
  assert_eq!(retried.retry_partition, 0);
  assert_eq!(retried.retry_offset, 0);
  assert_eq!(retried.retry_count, 1);

  let retry_tp = TopicPartition::new("__retry.orders", 0);
  let records = service.queue.stores().0.read_from(&retry_tp, 0, 1).unwrap();
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].payload, b"m1".to_vec());

  let headers = &records[0].headers;
  let get = |key: &str| {
    headers
      .iter()
      .find(|h| h.key == key)
      .map(|h| String::from_utf8(h.value.to_vec()).unwrap())
  };
  assert_eq!(get("x-retry-original-topic").as_deref(), Some("orders"));
  assert_eq!(get("x-retry-original-partition").as_deref(), Some("0"));
  assert_eq!(get("x-retry-original-offset").as_deref(), Some("0"));
  assert_eq!(get("x-retry-count").as_deref(), Some("1"));
  assert_eq!(get("x-retry-last-error").as_deref(), Some("db timeout"));
  assert_eq!(get("x-retry-failed-consumer").as_deref(), Some("c1"));
}

#[test]
fn process_retry_republishes_to_origin_and_commits_retry_offset() {
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
  service.publish("orders", 0, b"m1".to_vec()).unwrap();
  service
    .nack_and_retry("c1", "orders", 0, 0, Some("db timeout".to_owned()))
    .unwrap();

  let processed = service
    .process_retry_batch("retry-worker", "orders", 0, 10)
    .unwrap();
  assert_eq!(processed.retry_topic, "__retry.orders");
  assert_eq!(processed.moved_to_origin, 1);
  assert_eq!(processed.moved_to_dead_letter, 0);
  assert_eq!(processed.committed_next_offset, Some(1));

  let origin_tp = TopicPartition::new("orders", 0);
  let origin_records = service.queue.stores().0.read_from(&origin_tp, 1, 1).unwrap();
  assert_eq!(origin_records.len(), 1);
  assert_eq!(origin_records[0].payload, b"m1".to_vec());
}

#[test]
fn process_retry_moves_exhausted_retries_to_dlq_with_metadata_headers() {
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

  service
    .create_topic_with_policies(
      "orders",
      1,
      TopicPolicyOverrides {
        retry_policy: Some(TopicRetryPolicy {
          max_attempts: 2,
          backoff_initial_ms: 200,
          backoff_max_ms: 60_000,
        }),
        dead_letter_topic: Some("orders-dlq".to_owned()),
        ..TopicPolicyOverrides::default()
      },
    )
    .unwrap();

  service.publish("orders", 0, b"biz-payload".to_vec()).unwrap();
  service
    .nack_and_retry("c1", "orders", 0, 0, Some("first-failure".to_owned()))
    .unwrap();
  service
    .process_retry_batch("retry-worker", "orders", 0, 10)
    .unwrap();
  service
    .nack_and_retry(
      "c1",
      "orders",
      0,
      1,
      Some("final-failure".to_owned()),
    )
    .unwrap();
  let processed = service
    .process_retry_batch("retry-worker", "orders", 0, 10)
    .unwrap();
  assert_eq!(processed.moved_to_dead_letter, 1);
  assert_eq!(processed.moved_to_origin, 0);

  let dlq_tp = TopicPartition::new("orders-dlq", 0);
  let dlq_records = service.queue.stores().0.read_from(&dlq_tp, 0, 1).unwrap();
  assert_eq!(dlq_records.len(), 1);
  assert_eq!(dlq_records[0].payload, b"biz-payload".to_vec());

  let headers = &dlq_records[0].headers;
  let get = |key: &str| {
    headers
      .iter()
      .find(|h| h.key == key)
      .map(|h| String::from_utf8(h.value.to_vec()).unwrap())
  };
  assert_eq!(get("x-dlq-original-topic").as_deref(), Some("orders"));
  assert_eq!(get("x-dlq-original-partition").as_deref(), Some("0"));
  assert_eq!(get("x-dlq-original-offset").as_deref(), Some("0"));
  assert_eq!(get("x-dlq-retry-count").as_deref(), Some("2"));
  assert_eq!(get("x-dlq-error-code").as_deref(), Some("retry_exhausted"));
  assert_eq!(get("x-dlq-error-message").as_deref(), Some("final-failure"));
  assert!(
    headers.iter().all(|h| !h.key.starts_with("x-retry")),
    "DLQ record should not carry retry-chain headers"
  );
}

#[test]
fn process_retry_topics_once_scans_all_user_topics() {
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
  service.create_topic("payments", 1).unwrap();
  service.publish("orders", 0, b"o1".to_vec()).unwrap();
  service.publish("payments", 0, b"p1".to_vec()).unwrap();
  service
    .nack_and_retry("c-orders", "orders", 0, 0, Some("err-o".to_owned()))
    .unwrap();
  service
    .nack_and_retry("c-payments", "payments", 0, 0, Some("err-p".to_owned()))
    .unwrap();

  let moved = service.process_retry_topics_once("retry-worker", 10).unwrap();
  assert_eq!(moved, 2);
}

#[test]
fn scheduled_message_is_forwarded_by_delay_scheduler() {
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
  service
    .schedule_delayed("orders", 0, b"m1".to_vec(), 100)
    .unwrap();
  let moved = process_due_once(
    service.queue.stores().0,
    service.queue.stores().1,
    "__delay_scheduler",
    10,
    100,
  )
  .unwrap();
  assert_eq!(moved, 1);

  let records = service
    .queue
    .stores()
    .0
    .read_from(&TopicPartition::new("orders", 0), 0, 10)
    .unwrap();
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].payload, b"m1".to_vec());
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
fn create_topic_policies_are_persisted_in_catalog() {
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

  service
    .create_topic_with_policies(
      "orders",
      3,
      TopicPolicyOverrides {
        retention_max_bytes: Some(512 * 1024 * 1024),
        cleanup_policy: Some(TopicCleanupPolicy::CompactAndDelete),
        max_message_bytes: Some(2 * 1024 * 1024),
        max_batch_bytes: Some(12 * 1024 * 1024),
        retention_ms: Some(86_400_000),
        retry_policy: Some(TopicRetryPolicy {
          max_attempts: 8,
          backoff_initial_ms: 200,
          backoff_max_ms: 60_000,
        }),
        dead_letter_topic: Some("orders-dlq".to_owned()),
        delay_enabled: Some(true),
        compaction_enabled: Some(true),
      },
    )
    .unwrap();

  let topics = service.list_topics().unwrap();
  assert_eq!(topics.len(), 1);
  assert_eq!(topics[0].cleanup_policy, TopicCleanupPolicy::CompactAndDelete);
  assert_eq!(topics[0].max_message_bytes, 2 * 1024 * 1024);
  assert_eq!(topics[0].max_batch_bytes, 12 * 1024 * 1024);
  assert_eq!(topics[0].retention_ms, Some(86_400_000));
  assert_eq!(topics[0].dead_letter_topic.as_deref(), Some("orders-dlq"));
  assert!(topics[0].delay_enabled);
  assert!(topics[0].compaction_enabled);
  assert_eq!(
    topics[0].retry_policy,
    TopicRetryPolicy {
      max_attempts: 8,
      backoff_initial_ms: 200,
      backoff_max_ms: 60_000,
    }
  );
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
fn consumer_group_membership_join_and_heartbeat_work() {
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

  let joined = service
    .join_consumer_group("orders-cg", "member-1", vec!["orders".to_owned()], 10_000)
    .unwrap();
  assert_eq!(joined.group, "orders-cg");
  assert_eq!(joined.member_id, "member-1");
  assert_eq!(joined.topics, vec!["orders".to_owned()]);
  assert_eq!(joined.session_timeout_ms, 10_000);

  let heartbeated = service
    .heartbeat_consumer_group("orders-cg", "member-1", Some(20_000))
    .unwrap();
  assert_eq!(heartbeated.group, "orders-cg");
  assert_eq!(heartbeated.member_id, "member-1");
  assert_eq!(heartbeated.session_timeout_ms, 20_000);
  assert!(heartbeated.last_heartbeat_ms >= joined.last_heartbeat_ms);
  assert!(heartbeated.expires_at_ms >= joined.expires_at_ms);
  assert_eq!(heartbeated.joined_at_ms, joined.joined_at_ms);
}

#[test]
fn consumer_group_membership_expired_members_are_cleaned_up() {
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

  service
    .join_consumer_group("orders-cg", "member-expired", vec!["orders".to_owned()], 1)
    .unwrap();
  std::thread::sleep(std::time::Duration::from_millis(5));

  let members = service.list_active_group_members("orders-cg").unwrap();
  assert!(members.is_empty());

  let err = service
    .heartbeat_consumer_group("orders-cg", "member-expired", None)
    .unwrap_err();
  match err {
    StoreError::Codec(message) => {
      assert!(message.contains("not found"));
    }
    other => panic!("expected codec error for missing member, got {other:?}"),
  }
}

#[test]
fn consumer_group_rebalance_assigns_partitions_and_bumps_generation() {
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
  service.create_topic("payments", 2).unwrap();
  service
    .join_consumer_group(
      "orders-cg",
      "member-1",
      vec!["orders".to_owned(), "payments".to_owned()],
      30_000,
    )
    .unwrap();
  service
    .join_consumer_group("orders-cg", "member-2", vec!["orders".to_owned()], 30_000)
    .unwrap();

  let before_first = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should exist after member joins");
  let first = service.rebalance_consumer_group("orders-cg").unwrap();
  assert!(first.generation > before_first.generation);
  assert_eq!(first.assignments.len(), 5);
  assert_eq!(
    first
      .assignments
      .iter()
      .filter(|a| a.topic == "orders")
      .count(),
    3
  );
  assert_eq!(
    first
      .assignments
      .iter()
      .filter(|a| a.topic == "payments")
      .count(),
    2
  );
  assert!(
    first
      .assignments
      .iter()
      .filter(|a| a.topic == "payments")
      .all(|a| a.member_id == "member-1")
  );

  service
    .join_consumer_group("orders-cg", "member-3", vec!["orders".to_owned()], 30_000)
    .unwrap();
  let before_second = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should exist after member joins");
  let second = service.rebalance_consumer_group("orders-cg").unwrap();
  assert!(second.generation > before_second.generation);
  assert_eq!(second.assignments.len(), 5);
  assert_eq!(
    second
      .assignments
      .iter()
      .filter(|a| a.topic == "orders")
      .count(),
    3
  );
  assert_eq!(
    second
      .assignments
      .iter()
      .filter(|a| a.topic == "payments")
      .count(),
    2
  );

  let loaded = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should be persisted");
  assert_eq!(loaded.generation, second.generation);
  assert_eq!(loaded.assignments, second.assignments);
}

#[test]
fn consumer_group_rebalance_skips_unknown_topics() {
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

  service.create_topic("orders", 2).unwrap();
  service
    .join_consumer_group(
      "orders-cg",
      "member-1",
      vec!["orders".to_owned(), "unknown-topic".to_owned()],
      30_000,
    )
    .unwrap();

  let rebalance = service.rebalance_consumer_group("orders-cg").unwrap();
  assert!(rebalance.generation >= 1);
  assert_eq!(rebalance.assignments.len(), 2);
  assert!(rebalance.assignments.iter().all(|a| a.topic == "orders"));
}

#[test]
fn consumer_group_leave_triggers_auto_rebalance() {
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

  service.create_topic("orders", 2).unwrap();
  service
    .join_consumer_group("orders-cg", "member-1", vec!["orders".to_owned()], 30_000)
    .unwrap();
  service
    .join_consumer_group("orders-cg", "member-2", vec!["orders".to_owned()], 30_000)
    .unwrap();

  let before_leave = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should exist before leave");
  service
    .leave_consumer_group("orders-cg", "member-2")
    .unwrap();
  let after_leave = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should exist after leave");

  assert!(after_leave.generation > before_leave.generation);
  assert_eq!(after_leave.assignments.len(), 2);
  assert!(
    after_leave
      .assignments
      .iter()
      .all(|assignment| assignment.member_id == "member-1")
  );
}

#[test]
fn range_strategy_assigns_contiguous_partitions() {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new_with_mode_and_group_options(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
    BrokerRuntimeMode::Standalone,
    GroupCoordinatorOptions {
      assignment_strategy: GroupAssignmentStrategy::Range,
      sticky_assignments: false,
    },
  )
  .unwrap();

  service.create_topic("orders", 5).unwrap();
  service
    .join_consumer_group("orders-cg", "member-1", vec!["orders".to_owned()], 30_000)
    .unwrap();
  service
    .join_consumer_group("orders-cg", "member-2", vec!["orders".to_owned()], 30_000)
    .unwrap();

  let assignment = service.rebalance_consumer_group("orders-cg").unwrap();
  let owners: Vec<_> = assignment
    .assignments
    .iter()
    .filter(|item| item.topic == "orders")
    .map(|item| (item.partition, item.member_id.clone()))
    .collect();
  assert_eq!(
    owners,
    vec![
      (0, "member-1".to_owned()),
      (1, "member-1".to_owned()),
      (2, "member-1".to_owned()),
      (3, "member-2".to_owned()),
      (4, "member-2".to_owned()),
    ]
  );
}

#[test]
fn disabling_sticky_assignments_rebalances_from_strategy_baseline() {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new_with_mode_and_group_options(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
    BrokerRuntimeMode::Standalone,
    GroupCoordinatorOptions {
      assignment_strategy: GroupAssignmentStrategy::RoundRobin,
      sticky_assignments: false,
    },
  )
  .unwrap();

  service.create_topic("orders", 3).unwrap();
  service
    .join_consumer_group("orders-cg", "member-1", vec!["orders".to_owned()], 30_000)
    .unwrap();
  let first = service.rebalance_consumer_group("orders-cg").unwrap();
  assert!(
    first
      .assignments
      .iter()
      .all(|item| item.member_id == "member-1")
  );

  service
    .join_consumer_group("orders-cg", "member-2", vec!["orders".to_owned()], 30_000)
    .unwrap();
  let second = service.rebalance_consumer_group("orders-cg").unwrap();
  let owners: Vec<_> = second
    .assignments
    .iter()
    .filter(|item| item.topic == "orders")
    .map(|item| (item.partition, item.member_id.clone()))
    .collect();
  assert_eq!(
    owners,
    vec![
      (0, "member-1".to_owned()),
      (1, "member-2".to_owned()),
      (2, "member-1".to_owned()),
    ]
  );
}

#[test]
fn sticky_assignments_still_keep_load_balanced_when_new_member_joins() {
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

  service.create_topic("orders", 4).unwrap();
  service
    .join_consumer_group("orders-cg", "member-1", vec!["orders".to_owned()], 30_000)
    .unwrap();
  service
    .join_consumer_group("orders-cg", "member-2", vec!["orders".to_owned()], 30_000)
    .unwrap();

  let assignment = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should exist after auto-rebalance");
  let member1 = assignment
    .assignments
    .iter()
    .filter(|item| item.topic == "orders" && item.member_id == "member-1")
    .count();
  let member2 = assignment
    .assignments
    .iter()
    .filter(|item| item.topic == "orders" && item.member_id == "member-2")
    .count();
  assert_eq!(member1, 2);
  assert_eq!(member2, 2);
}

#[test]
fn rebalance_explain_reports_move_and_sticky_stats() {
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

  service.create_topic("orders", 4).unwrap();
  service
    .join_consumer_group("orders-cg", "member-1", vec!["orders".to_owned()], 30_000)
    .unwrap();
  service
    .join_consumer_group("orders-cg", "member-2", vec!["orders".to_owned()], 30_000)
    .unwrap();

  let explain = service
    .explain_consumer_group_rebalance("orders-cg")
    .unwrap();
  assert_eq!(explain.group, "orders-cg");
  assert_eq!(explain.strategy, "round_robin");
  assert!(explain.sticky_assignments);
  assert_eq!(explain.active_members, 2);
  assert_eq!(explain.total_assignments, 4);
  assert!(explain.next_generation >= 1);
  assert_eq!(explain.moved_partitions, 0);
  assert!(explain.sticky_candidates >= explain.sticky_applied);
  let total_load: usize = explain
    .member_loads
    .iter()
    .map(|item| item.partitions)
    .sum();
  assert_eq!(total_load, 4);
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

#[test]
fn standalone_mode_recovers_topics_offsets_and_direct_inbox_after_restart() {
  let root = temp_path("restart-recovery");
  let segments_dir = root.join("segments");
  let metadata_path = root.join("meta").join("metadata.redb");

  let service = BrokerService::new(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    SegmentLog::open(SegmentLogOptions::new(&segments_dir)).unwrap(),
    RedbMetadataStore::create(&metadata_path).unwrap(),
  )
  .unwrap();

  service.create_topic("orders", 2).unwrap();
  service.publish("orders", 0, b"a".to_vec()).unwrap();
  service.publish("orders", 0, b"b".to_vec()).unwrap();
  service.commit_offset("c1", "orders", 0, 1).unwrap();
  service
    .send_direct("alice", "bob", None, b"hello".to_vec())
    .unwrap();
  drop(service);

  let restarted = BrokerService::new(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    SegmentLog::open(SegmentLogOptions::new(&segments_dir)).unwrap(),
    RedbMetadataStore::create(&metadata_path).unwrap(),
  )
  .unwrap();

  let topics = restarted.list_topics().unwrap();
  assert_eq!(topics.len(), 1);
  assert_eq!(topics[0].name, "orders");
  assert_eq!(topics[0].partitions, 2);

  let fetched = restarted.fetch("c1", "orders", 0, None, 10).unwrap();
  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].offset, 1);
  assert_eq!(fetched.records[0].payload, b"b".to_vec());

  let (offset, next_offset) = restarted.publish("orders", 0, b"c".to_vec()).unwrap();
  assert_eq!(offset, 2);
  assert_eq!(next_offset, 3);

  let inbox = restarted.fetch_inbox("bob", 10).unwrap();
  assert_eq!(inbox.records.len(), 1);
  assert_eq!(inbox.records[0].message.sender, "alice");
  assert_eq!(inbox.records[0].message.payload, b"hello".to_vec());
  restarted.ack_direct("bob", inbox.next_offset).unwrap();
  let after_ack = restarted.fetch_inbox("bob", 10).unwrap();
  assert!(after_ack.records.is_empty());

  drop(restarted);
  let _ = std::fs::remove_dir_all(root);
}
