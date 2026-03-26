use std::{
  sync::{Arc, Mutex, OnceLock},
  time::{SystemTime, UNIX_EPOCH},
};

use store::{RedbMetadataStore, SegmentLog, SegmentLogOptions};

use crate::{
  config::AppConfig,
  service::{BrokerIdentity, BrokerPublishPartitioning, BrokerRuntimeMode, BrokerService},
};

use super::wire_runtime;

fn temp_path(name: &str) -> std::path::PathBuf {
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_nanos();
  std::env::temp_dir().join(format!("ech0-di-{name}-{nanos}"))
}

fn metrics_test_lock() -> &'static Mutex<()> {
  static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
  LOCK.get_or_init(|| Mutex::new(()))
}

fn build_service() -> (
  Arc<BrokerService<Arc<SegmentLog>, Arc<RedbMetadataStore>>>,
  std::path::PathBuf,
) {
  let root = temp_path("runtime");
  let log = Arc::new(SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap());
  let meta = Arc::new(RedbMetadataStore::create(root.join("meta").join("metadata.redb")).unwrap());
  let service = Arc::new(
    BrokerService::new_with_mode(
      BrokerIdentity {
        node_id: 1,
        cluster_name: "test".to_owned(),
      },
      Arc::clone(&log),
      Arc::clone(&meta),
      BrokerRuntimeMode::Standalone,
    )
    .unwrap(),
  );
  (service, root)
}

#[test]
fn wire_runtime_builds_tcp_and_admin_configs() {
  let (service, root) = build_service();
  service.create_topic("orders", 1).unwrap();

  let mut app = AppConfig::default();
  app.broker.bind_addr = "127.0.0.1:19090".to_owned();
  app.broker.max_frame_body_bytes = 1234;
  app.admin.enabled = true;
  app.admin.bind_addr = "127.0.0.1:19091".to_owned();

  let wiring = wire_runtime(&app, service).unwrap();
  assert_eq!(wiring.tcp_config.bind_addr, "127.0.0.1:19090");
  assert_eq!(wiring.tcp_config.max_frame_body_bytes, 1234);
  let admin = wiring.admin_config.expect("admin should be enabled");
  assert_eq!(admin.bind_addr, "127.0.0.1:19091");
  let health = (admin.health_snapshot)().unwrap();
  assert_eq!(health.status, "ok");
  assert_eq!(health.runtime_mode, "standalone");
  assert!(health.background.retention_cleanup.enabled);
  assert_eq!(health.background.retention_cleanup.interval_secs, 30);
  assert!(health.background.retention_cleanup.ready);
  assert!(health.background.compaction_cleanup.enabled);
  assert_eq!(health.background.compaction_cleanup.interval_secs, 60);
  assert_eq!(health.background.compaction_cleanup.sealed_segment_batch, 2);
  assert!(health.background.compaction_cleanup.ready);
  let topics = (admin.topic_snapshot)().unwrap();
  assert!(topics.iter().any(|topic| topic.name == "orders"));

  let _ = std::fs::remove_dir_all(root);
}

#[test]
fn wire_runtime_skips_admin_when_disabled() {
  let (service, root) = build_service();
  let mut app = AppConfig::default();
  app.admin.enabled = false;

  let wiring = wire_runtime(&app, service).unwrap();
  assert!(wiring.admin_config.is_none());

  let _ = std::fs::remove_dir_all(root);
}

#[test]
fn admin_topic_snapshot_includes_producer_heat_summary() {
  let (service, root) = build_service();
  let topic = format!(
    "orders-{}",
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_nanos()
  );
  service.create_topic(&topic, 2).unwrap();
  service
    .publish_with_partitioning(
      &topic,
      BrokerPublishPartitioning::RoundRobin,
      None,
      false,
      b"a".to_vec(),
    )
    .unwrap();
  service
    .publish_with_partitioning(
      &topic,
      BrokerPublishPartitioning::RoundRobin,
      None,
      false,
      b"b".to_vec(),
    )
    .unwrap();
  service
    .publish_with_partitioning(
      &topic,
      BrokerPublishPartitioning::Explicit(1),
      None,
      false,
      b"c".to_vec(),
    )
    .unwrap();

  let mut app = AppConfig::default();
  app.admin.enabled = true;

  let wiring = wire_runtime(&app, service).unwrap();
  let admin = wiring.admin_config.expect("admin should be enabled");
  let topics = (admin.topic_snapshot)().unwrap();
  let orders = topics
    .into_iter()
    .find(|summary| summary.name == topic)
    .expect("topic summary should exist");
  assert_eq!(orders.produced_records_total, 3);
  assert_eq!(orders.hottest_partition, Some(1));
  assert_eq!(orders.hottest_partition_records, 2);
  assert_eq!(orders.last_partitioning.as_deref(), Some("explicit"));

  let _ = std::fs::remove_dir_all(root);
}

#[test]
fn admin_metrics_refresh_updates_stream_gauges() {
  let _guard = metrics_test_lock().lock().expect("metrics lock poisoned");
  crate::metrics::init_metrics().unwrap();
  let (service, root) = build_service();
  let topic = format!(
    "metrics-orders-{}",
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_nanos()
  );
  let group = format!("{topic}-cg");
  service.create_topic(&topic, 2).unwrap();
  service
    .publish_with_partitioning(
      &topic,
      BrokerPublishPartitioning::Explicit(0),
      None,
      false,
      b"a".to_vec(),
    )
    .unwrap();
  service
    .publish_with_partitioning(
      &topic,
      BrokerPublishPartitioning::Explicit(0),
      None,
      false,
      b"b".to_vec(),
    )
    .unwrap();
  service
    .publish_with_partitioning(
      &topic,
      BrokerPublishPartitioning::Explicit(1),
      None,
      false,
      b"c".to_vec(),
    )
    .unwrap();
  service
    .join_consumer_group(&group, "member-1", vec![topic.clone()], 30_000)
    .unwrap();
  let assignment = service
    .load_consumer_group_assignment(&group)
    .unwrap()
    .expect("assignment should exist after join");
  service
    .commit_consumer_group_offset(&group, "member-1", assignment.generation, &topic, 0, 1)
    .unwrap();
  service
    .commit_consumer_group_offset(&group, "member-1", assignment.generation, &topic, 1, 1)
    .unwrap();

  let mut app = AppConfig::default();
  app.admin.enabled = true;

  let wiring = wire_runtime(&app, service).unwrap();
  let admin = wiring.admin_config.expect("admin should be enabled");
  (admin.metrics_refresh)().unwrap();

  let snapshot = crate::metrics::snapshot();
  assert_eq!(snapshot.visible_topics_current, 1);
  assert_eq!(snapshot.topics_with_backlog_current, 1);
  assert_eq!(snapshot.total_topic_backlog_records_current, 3);
  assert_eq!(snapshot.max_topic_backlog_records_current, 3);
  assert_eq!(snapshot.max_partition_backlog_records_current, 2);
  assert_eq!(snapshot.consumer_groups_current, 1);
  assert_eq!(snapshot.consumer_groups_with_lag_current, 1);
  assert_eq!(snapshot.total_consumer_group_backlog_records_current, 3);
  assert_eq!(snapshot.total_consumer_group_lag_records_current, 1);
  assert_eq!(snapshot.max_consumer_group_lag_records_current, 1);

  let _ = std::fs::remove_dir_all(root);
}
