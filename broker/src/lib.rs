pub mod config;
mod builder;

mod admin;
mod di;
mod logging;
mod metrics;
mod ops_state;
mod producer_state;
mod raft;
mod server;
mod service;

use std::future::Future;

use scheduler::process_due_once;
use tracing::{info, warn};

pub use config::{AppConfig, ConfigError};
pub use builder::BrokerBuilder;
use config::GroupAssignmentStrategyConfig;
use di::{BootstrapModule, LifecycleModule, wire_runtime};
use raft::OpenRaftRuntimeConfig;
use service::{
  BrokerIdentity, BrokerRuntimeMode, BrokerService, GroupAssignmentStrategy,
  GroupCoordinatorOptions,
};
use store::{
  BrokerState, BrokerStateStore, MessageLogStore, RedbMetadataStore, Result, SegmentLog,
  SegmentLogOptions, TopicCatalogStore, TopicConfig,
};

pub async fn run() -> Result<()> {
  let app = AppConfig::load()
    .map_err(|err| store::StoreError::Codec(format!("failed to load config: {err}")))?;
  run_with_config(app).await
}

pub async fn run_with_config(app: AppConfig) -> Result<()> {
  run_with_config_and_shutdown(app, async {
    let _ = tokio::signal::ctrl_c().await;
  })
  .await
}

pub async fn run_with_config_and_shutdown<F>(app: AppConfig, shutdown: F) -> Result<()>
where
  F: Future<Output = ()> + Send,
{
  let bootstrap = BootstrapModule::new(app.clone());
  bootstrap.init_observability()?;

  info!(node_id = app.broker.node_id, cluster = %app.broker.cluster_name, ?app, "loaded broker configuration");

  let mut log_options = SegmentLogOptions::new(app.segments_dir());
  log_options.compaction_sealed_segment_batch = app.storage.compaction_sealed_segment_batch.max(1);
  let log = std::sync::Arc::new(SegmentLog::open(log_options)?);
  let retention_log = std::sync::Arc::clone(&log);
  let meta = std::sync::Arc::new(RedbMetadataStore::create(app.metadata_path())?);

  if !app.raft.enabled {
    ensure_bootstrap_topic(&log, &meta)?;
  }
  validate_topics(&log, &meta)?;

  meta.save_broker_state(&BrokerState {
    node_id: format!("node-{}", app.broker.node_id),
    epoch: 1,
  })?;

  let raft_runtime =
    if app.raft.enabled {
      Some(OpenRaftRuntimeConfig::from_app_config(&app).map_err(|err| {
        store::StoreError::Codec(format!("failed to build openraft config: {err}"))
      })?)
    } else {
      None
    };

  info!(
    raft_enabled = app.raft.enabled,
    raft_runtime = ?raft_runtime.as_ref().map(|cfg| (&cfg.node_id, cfg.known_nodes.len())),
    runtime_mode = %BrokerRuntimeMode::from_raft_runtime(raft_runtime.clone()).label(),
    bind_addr = %app.broker.bind_addr,
    admin_enabled = app.admin.enabled,
    admin_bind_addr = %app.admin.bind_addr,
    "broker bootstrap completed"
  );

  bootstrap.configure_runtime_limits();

  let service = std::sync::Arc::new(BrokerService::new_with_mode_and_group_options(
    BrokerIdentity {
      node_id: app.broker.node_id,
      cluster_name: app.broker.cluster_name.clone(),
    },
    std::sync::Arc::clone(&log),
    std::sync::Arc::clone(&meta),
    BrokerRuntimeMode::from_raft_runtime(raft_runtime),
    GroupCoordinatorOptions {
      assignment_strategy: match app.broker.group_assignment_strategy {
        GroupAssignmentStrategyConfig::RoundRobin => GroupAssignmentStrategy::RoundRobin,
        GroupAssignmentStrategyConfig::Range => GroupAssignmentStrategy::Range,
      },
      sticky_assignments: app.broker.group_sticky_assignments,
    },
  )?);
  let wiring = wire_runtime(&app, std::sync::Arc::clone(&service))
    .map_err(|err| store::StoreError::Codec(format!("failed to wire runtime via fluxdi: {err}")))?;
  let lifecycle = LifecycleModule::new();

  bootstrap.spawn_retention_cleanup_task(retention_log);
  bootstrap.spawn_compaction_cleanup_task(std::sync::Arc::clone(&log));
  if app.broker.delay_scheduler_enabled {
    tokio::spawn(run_delay_scheduler_task(
      std::sync::Arc::clone(&log),
      std::sync::Arc::clone(&meta),
      app.broker.delay_scheduler_consumer_prefix.clone(),
      app.broker.delay_scheduler_interval_secs,
      app.broker.delay_scheduler_max_records,
    ));
  }
  if app.broker.retry_worker_enabled {
    tokio::spawn(run_retry_worker_task(
      std::sync::Arc::clone(&service),
      app.broker.retry_worker_consumer_prefix.clone(),
      app.broker.retry_worker_interval_secs,
      app.broker.retry_worker_max_records,
    ));
  }

  lifecycle.run_with_shutdown(wiring, shutdown).await
}

fn ensure_bootstrap_topic(log: &SegmentLog, meta: &RedbMetadataStore) -> Result<()> {
  if !log.topic_exists("demo")? {
    let mut topic = TopicConfig::new("demo");
    topic.partitions = 2;
    topic.segment_max_bytes = 1024 * 1024;
    topic.index_interval_bytes = 1024;
    log.create_topic(topic.clone())?;
    meta.save_topic_config(&topic)?;
    info!(topic = %topic.name, partitions = topic.partitions, "created demo topic");
  }

  Ok(())
}

async fn run_delay_scheduler_task(
  log: std::sync::Arc<SegmentLog>,
  meta: std::sync::Arc<RedbMetadataStore>,
  consumer_prefix: String,
  interval_secs: u64,
  max_records_per_partition: usize,
) -> std::io::Result<()> {
  let interval_secs = interval_secs.max(1);
  let max_records_per_partition = max_records_per_partition.max(1);
  let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
  interval.tick().await;
  loop {
    interval.tick().await;
    let now_ms = std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .unwrap_or_default()
      .as_millis() as u64;
    match process_due_once(
      log.as_ref(),
      meta.as_ref(),
      &consumer_prefix,
      max_records_per_partition,
      now_ms,
    ) {
      Ok(moved) if moved > 0 => {
        info!(
          moved_records = moved,
          interval_secs,
          max_records_per_partition,
          "delay scheduler forwarded due records"
        );
      }
      Ok(_) => {}
      Err(err) => {
        warn!(error = %err, "delay scheduler iteration failed");
      }
    }
  }
}

fn validate_topics(log: &SegmentLog, meta: &RedbMetadataStore) -> Result<()> {
  let unavailable_topics = log.validate_all_topics_against_catalog(meta)?;
  for issue in &unavailable_topics {
    warn!(topic = %issue.topic, reason = %issue.reason, "topic marked unavailable during startup validation");
  }
  if unavailable_topics.iter().any(|issue| issue.topic == "demo") {
    return Err(store::StoreError::TopicUnavailable {
      topic: "demo".to_owned(),
      reason: unavailable_topics
        .into_iter()
        .find(|issue| issue.topic == "demo")
        .map(|issue| issue.reason)
        .unwrap_or_else(|| "topic is unavailable".to_owned()),
    });
  }

  Ok(())
}

async fn run_retry_worker_task(
  service: std::sync::Arc<di::BrokerServiceHandle>,
  consumer_prefix: String,
  interval_secs: u64,
  max_records_per_partition: usize,
) -> std::io::Result<()> {
  let interval_secs = interval_secs.max(1);
  let max_records_per_partition = max_records_per_partition.max(1);
  let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
  interval.tick().await;
  loop {
    interval.tick().await;
    match service.process_retry_topics_once(&consumer_prefix, max_records_per_partition) {
      Ok(moved) if moved > 0 => {
        info!(
          moved_records = moved,
          interval_secs,
          max_records_per_partition,
          "retry worker moved records from retry topics"
        );
      }
      Ok(_) => {}
      Err(err) => {
        warn!(error = %err, "retry worker iteration failed");
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use std::time::{SystemTime, UNIX_EPOCH};

  use store::{StoreError, TopicConfig};

  use super::{ensure_bootstrap_topic, validate_topics};
  use store::{
    BrokerState, JsonCodec, MessageLogStore, RedbMetadataStore, SegmentLog, SegmentLogOptions,
    TopicCatalogStore,
  };

  fn temp_path(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_nanos();
    std::env::temp_dir().join(format!("ech0-broker-main-{name}-{nanos}"))
  }

  #[test]
  fn validate_topics_allows_unavailable_non_demo_topics() {
    let root = temp_path("validate-non-demo");
    let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
    let meta = RedbMetadataStore::create_with_codecs(
      root.join("meta.redb"),
      JsonCodec::<TopicConfig>::new(),
      JsonCodec::<BrokerState>::new(),
      JsonCodec::new(),
    )
    .unwrap();

    ensure_bootstrap_topic(&log, &meta).unwrap();

    let mut broken_manifest = TopicConfig::new("orders");
    broken_manifest.partitions = 2;
    log.create_topic(broken_manifest.clone()).unwrap();

    let mut broken_catalog = TopicConfig::new("orders");
    broken_catalog.partitions = 1;
    meta.save_topic_config(&broken_catalog).unwrap();

    validate_topics(&log, &meta).unwrap();

    let _ = std::fs::remove_dir_all(root);
  }

  #[test]
  fn validate_topics_rejects_unavailable_demo_topic() {
    let root = temp_path("validate-demo");
    let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
    let meta = RedbMetadataStore::create_with_codecs(
      root.join("meta.redb"),
      JsonCodec::<TopicConfig>::new(),
      JsonCodec::<BrokerState>::new(),
      JsonCodec::new(),
    )
    .unwrap();

    let mut demo_manifest = TopicConfig::new("demo");
    demo_manifest.partitions = 2;
    log.create_topic(demo_manifest).unwrap();

    let mut demo_catalog = TopicConfig::new("demo");
    demo_catalog.partitions = 1;
    meta.save_topic_config(&demo_catalog).unwrap();

    let err = validate_topics(&log, &meta).unwrap_err();
    match err {
      StoreError::TopicUnavailable { topic, reason } => {
        assert_eq!(topic, "demo");
        assert!(reason.contains("unavailable"));
      }
      other => panic!("expected TopicUnavailable error, got {other:?}"),
    }

    let _ = std::fs::remove_dir_all(root);
  }
}
