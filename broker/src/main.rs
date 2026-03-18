mod config;
mod logging;
mod raft;

use tracing::{debug, info, warn};

use config::AppConfig;
use raft::{OpenRaftEntryPayload, OpenRaftPartitionStateMachineAdapter, OpenRaftRuntimeConfig};
use store::{
  BrokerState, BrokerStateStore, CommandSource, LocalPartitionCommand,
  LocalPartitionCommandExecutor, LocalPartitionStateMachine, LocalPartitionStateStore,
  MessageLogStore, MutablePartitionLogStore, OffsetStore, PartitionAvailability,
  PartitionCommandEnvelope, PartitionStateMachine, RedbMetadataStore, Result, SegmentLog,
  SegmentLogOptions, TopicCatalogStore, TopicConfig, TopicPartition,
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
  let demo_partition = TopicPartition::new("demo", 0);
  let state_machine =
    LocalPartitionStateMachine::new(LocalPartitionCommandExecutor::new(&log, &meta));
  let raft_state_machine = OpenRaftPartitionStateMachineAdapter::new(state_machine);

  if !log.topic_exists("demo")? {
    let mut topic = TopicConfig::new("demo");
    topic.partitions = 2;
    topic.segment_max_bytes = 1024 * 1024;
    topic.index_interval_bytes = 1024;
    log.create_topic(topic.clone())?;
    meta.save_topic_config(&topic)?;
    info!(topic = %topic.name, partitions = topic.partitions, "created demo topic");
  }

  let unavailable_topics = log.validate_all_topics_against_catalog(&meta)?;
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

  let append_envelope = PartitionCommandEnvelope::new(
    LocalPartitionCommand::Append {
      topic_partition: demo_partition.clone(),
      payload: b"hello, ech0".to_vec(),
    },
    CommandSource::Client,
  )
  .with_leader_epoch(1);

  let replicated = append_envelope.clone().replicated_command()?;
  let raft_payload = OpenRaftEntryPayload {
    envelope: replicated.clone(),
  };
  let wire_bytes = raft_payload.encode_json()?;
  let decoded = OpenRaftEntryPayload::decode_json(&wire_bytes)?;

  let append_applied = raft_state_machine.apply_replicated_entry(decoded.envelope)?;
  let next_offset = match append_applied.result {
    store::ApplyResult::Appended { next_offset } => next_offset,
    _ => unreachable!("append must return appended result"),
  };

  let poll_result = log.read_from(&demo_partition, 0, 16)?;
  meta.save_consumer_offset("consumer-a", &demo_partition, next_offset)?;
  let availability_applied = raft_state_machine.inner().apply_local_command(
    LocalPartitionCommand::UpdateAvailability {
      topic_partition: demo_partition.clone(),
      availability: PartitionAvailability::Online,
    },
    CommandSource::Metadata,
  )?;

  debug!(
    ?append_applied,
    ?availability_applied,
    "applied demo commands"
  );
  info!(
      partition = demo_partition.partition,
      offset = next_offset.saturating_sub(1),
      records_read = poll_result.len(),
      high_watermark = ?log.local_partition_state(&demo_partition)?.state.high_watermark,
      broker_state = ?meta.load_broker_state()?,
      local_partition_state = ?meta.load_local_partition_state(&demo_partition)?,
      raft_enabled = app.raft.enabled,
      raft_runtime = ?raft_runtime.as_ref().map(|cfg| (&cfg.node_id, cfg.known_nodes.len())),
      "broker bootstrap completed"
  );

  Ok(())
}
