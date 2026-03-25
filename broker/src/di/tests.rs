use std::{
  sync::Arc,
  time::{SystemTime, UNIX_EPOCH},
};

use store::{RedbMetadataStore, SegmentLog, SegmentLogOptions};

use crate::{
  config::AppConfig,
  service::{BrokerIdentity, BrokerRuntimeMode, BrokerService},
};

use super::wire_runtime;

fn temp_path(name: &str) -> std::path::PathBuf {
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_nanos();
  std::env::temp_dir().join(format!("ech0-di-{name}-{nanos}"))
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
