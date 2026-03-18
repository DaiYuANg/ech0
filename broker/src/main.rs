mod config;
mod logging;
mod raft;
mod server;
mod service;

use tracing::{info, warn};

use config::AppConfig;
use raft::OpenRaftRuntimeConfig;
use server::{TcpBrokerServer, TcpServerConfig};
use service::{BrokerIdentity, BrokerService};
use store::{
  BrokerState, BrokerStateStore, MessageLogStore, RedbMetadataStore, Result, SegmentLog,
  SegmentLogOptions, TopicCatalogStore, TopicConfig,
};

#[tokio::main]
async fn main() -> Result<()> {
  let app = AppConfig::load()
    .map_err(|err| store::StoreError::Codec(format!("failed to load config: {err}")))?;
  logging::init_logging(&app.logging)
    .map_err(|err| store::StoreError::Codec(format!("failed to init logging: {err}")))?;

  info!(node_id = app.broker.node_id, cluster = %app.broker.cluster_name, ?app, "loaded broker configuration");

  let log = SegmentLog::open(SegmentLogOptions::new(app.segments_dir()))?;
  let meta = RedbMetadataStore::create(app.metadata_path())?;

  ensure_bootstrap_topic(&log, &meta)?;
  validate_topics(&log, &meta)?;

  meta.save_broker_state(&BrokerState {
    node_id: format!("node-{}", app.broker.node_id),
    epoch: 1,
  })?;

  let raft_runtime = if app.raft.enabled {
    Some(OpenRaftRuntimeConfig::from_app_config(&app).map_err(|err| {
      store::StoreError::Codec(format!("failed to build openraft config: {err}"))
    })?)
  } else {
    None
  };

  info!(
    raft_enabled = app.raft.enabled,
    raft_runtime = ?raft_runtime.as_ref().map(|cfg| (&cfg.node_id, cfg.known_nodes.len())),
    bind_addr = %app.broker.bind_addr,
    "broker bootstrap completed"
  );

  let service = std::sync::Arc::new(BrokerService::new(
    BrokerIdentity {
      node_id: app.broker.node_id,
      cluster_name: app.broker.cluster_name.clone(),
    },
    log,
    meta,
  ));
  let server = TcpBrokerServer::new(
    TcpServerConfig {
      bind_addr: app.broker.bind_addr.clone(),
    },
    service,
  );

  server
    .run()
    .await
    .map_err(|err| store::StoreError::Io(std::io::Error::new(err.kind(), err.to_string())))
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
