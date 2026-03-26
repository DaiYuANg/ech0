use std::sync::OnceLock;

use prometheus::{Encoder, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder};

#[derive(Debug)]
struct BrokerMetrics {
  registry: Registry,
  tcp_connections_total: IntCounter,
  commands_total_all: IntCounter,
  commands_total: IntCounterVec,
  command_errors_total_all: IntCounter,
  command_errors_total: IntCounterVec,
  rebalances_total: IntCounter,
  rebalances_total_by_strategy: IntCounterVec,
  rebalance_assignments_total: IntCounter,
  rebalance_moved_partitions_total: IntCounter,
  raft_client_write_retries_total: IntCounter,
  raft_client_write_failures_total: IntCounter,
  retention_cleanup_runs_total: IntCounter,
  retention_cleanup_removed_segments_total: IntCounter,
  compaction_cleanup_runs_total: IntCounter,
  compaction_cleanup_compacted_partitions_total: IntCounter,
  compaction_cleanup_removed_records_total: IntCounter,
  produce_requests_total_all: IntCounter,
  produce_requests_total: IntCounterVec,
  produced_records_total_all: IntCounter,
  produced_records_total: IntCounterVec,
  visible_topics_current: IntGauge,
  topics_with_backlog_current: IntGauge,
  total_topic_backlog_records_current: IntGauge,
  max_topic_backlog_records_current: IntGauge,
  max_partition_backlog_records_current: IntGauge,
  consumer_groups_current: IntGauge,
  consumer_groups_with_lag_current: IntGauge,
  total_consumer_group_backlog_records_current: IntGauge,
  total_consumer_group_lag_records_current: IntGauge,
  max_consumer_group_lag_records_current: IntGauge,
}

static METRICS: OnceLock<BrokerMetrics> = OnceLock::new();

pub fn init_metrics() -> Result<(), prometheus::Error> {
  if METRICS.get().is_some() {
    return Ok(());
  }

  let registry = Registry::new();
  let tcp_connections_total = IntCounter::with_opts(Opts::new(
    "broker_tcp_connections_total",
    "Accepted TCP connections",
  ))?;
  let commands_total = IntCounterVec::new(
    Opts::new("broker_commands_total", "Handled broker commands"),
    &["command_id"],
  )?;
  let commands_total_all = IntCounter::with_opts(Opts::new(
    "broker_commands_total_all",
    "Handled broker commands across all command IDs",
  ))?;
  let command_errors_total = IntCounterVec::new(
    Opts::new(
      "broker_command_errors_total",
      "Broker command errors by code",
    ),
    &["code"],
  )?;
  let command_errors_total_all = IntCounter::with_opts(Opts::new(
    "broker_command_errors_total_all",
    "Broker command errors across all error codes",
  ))?;
  let rebalances_total = IntCounter::with_opts(Opts::new(
    "broker_rebalances_total",
    "Total consumer-group rebalance executions",
  ))?;
  let rebalances_total_by_strategy = IntCounterVec::new(
    Opts::new(
      "broker_rebalances_total_by_strategy",
      "Total consumer-group rebalance executions by assignment strategy",
    ),
    &["strategy"],
  )?;
  let rebalance_assignments_total = IntCounter::with_opts(Opts::new(
    "broker_rebalance_assignments_total",
    "Total partition assignments emitted by rebalances",
  ))?;
  let rebalance_moved_partitions_total = IntCounter::with_opts(Opts::new(
    "broker_rebalance_moved_partitions_total",
    "Total partition ownership moves caused by rebalances",
  ))?;
  let raft_client_write_retries_total = IntCounter::with_opts(Opts::new(
    "broker_raft_client_write_retries_total",
    "Total raft client write retries while waiting for leader resolution",
  ))?;
  let raft_client_write_failures_total = IntCounter::with_opts(Opts::new(
    "broker_raft_client_write_failures_total",
    "Total raft client write failures after retry handling",
  ))?;
  let retention_cleanup_runs_total = IntCounter::with_opts(Opts::new(
    "broker_retention_cleanup_runs_total",
    "Total background retention cleanup iterations that removed at least one segment",
  ))?;
  let retention_cleanup_removed_segments_total = IntCounter::with_opts(Opts::new(
    "broker_retention_cleanup_removed_segments_total",
    "Total log segments removed by the background retention worker",
  ))?;
  let compaction_cleanup_runs_total = IntCounter::with_opts(Opts::new(
    "broker_compaction_cleanup_runs_total",
    "Total background compaction cleanup iterations that compacted at least one partition",
  ))?;
  let compaction_cleanup_compacted_partitions_total = IntCounter::with_opts(Opts::new(
    "broker_compaction_cleanup_compacted_partitions_total",
    "Total partitions compacted by the background compaction worker",
  ))?;
  let compaction_cleanup_removed_records_total = IntCounter::with_opts(Opts::new(
    "broker_compaction_cleanup_removed_records_total",
    "Total records removed by the background compaction worker",
  ))?;
  let produce_requests_total = IntCounterVec::new(
    Opts::new(
      "broker_produce_requests_total",
      "Total broker produce requests by partitioning mode",
    ),
    &["partitioning"],
  )?;
  let produce_requests_total_all = IntCounter::with_opts(Opts::new(
    "broker_produce_requests_total_all",
    "Total broker produce requests across all partitioning modes",
  ))?;
  let produced_records_total = IntCounterVec::new(
    Opts::new(
      "broker_produced_records_total",
      "Total records appended by broker produce operations by partitioning mode",
    ),
    &["partitioning"],
  )?;
  let produced_records_total_all = IntCounter::with_opts(Opts::new(
    "broker_produced_records_total_all",
    "Total records appended by broker produce operations across all partitioning modes",
  ))?;
  let visible_topics_current = IntGauge::with_opts(Opts::new(
    "broker_visible_topics_current",
    "Current number of visible topics",
  ))?;
  let topics_with_backlog_current = IntGauge::with_opts(Opts::new(
    "broker_topics_with_backlog_current",
    "Current number of visible topics with backlog",
  ))?;
  let total_topic_backlog_records_current = IntGauge::with_opts(Opts::new(
    "broker_topic_backlog_records_current",
    "Current total backlog records across visible topics",
  ))?;
  let max_topic_backlog_records_current = IntGauge::with_opts(Opts::new(
    "broker_topic_backlog_records_max_current",
    "Current maximum backlog records for a single visible topic",
  ))?;
  let max_partition_backlog_records_current = IntGauge::with_opts(Opts::new(
    "broker_partition_backlog_records_max_current",
    "Current maximum backlog records for a single partition",
  ))?;
  let consumer_groups_current = IntGauge::with_opts(Opts::new(
    "broker_consumer_groups_current",
    "Current number of consumer groups with saved assignments",
  ))?;
  let consumer_groups_with_lag_current = IntGauge::with_opts(Opts::new(
    "broker_consumer_groups_with_lag_current",
    "Current number of consumer groups with non-zero lag",
  ))?;
  let total_consumer_group_backlog_records_current = IntGauge::with_opts(Opts::new(
    "broker_consumer_group_backlog_records_current",
    "Current total backlog records across consumer groups with saved assignments",
  ))?;
  let total_consumer_group_lag_records_current = IntGauge::with_opts(Opts::new(
    "broker_consumer_group_lag_records_current",
    "Current total lag records across consumer groups with saved assignments",
  ))?;
  let max_consumer_group_lag_records_current = IntGauge::with_opts(Opts::new(
    "broker_consumer_group_lag_records_max_current",
    "Current maximum lag records for a single consumer group",
  ))?;

  registry.register(Box::new(tcp_connections_total.clone()))?;
  registry.register(Box::new(commands_total_all.clone()))?;
  registry.register(Box::new(commands_total.clone()))?;
  registry.register(Box::new(command_errors_total_all.clone()))?;
  registry.register(Box::new(command_errors_total.clone()))?;
  registry.register(Box::new(rebalances_total.clone()))?;
  registry.register(Box::new(rebalances_total_by_strategy.clone()))?;
  registry.register(Box::new(rebalance_assignments_total.clone()))?;
  registry.register(Box::new(rebalance_moved_partitions_total.clone()))?;
  registry.register(Box::new(raft_client_write_retries_total.clone()))?;
  registry.register(Box::new(raft_client_write_failures_total.clone()))?;
  registry.register(Box::new(retention_cleanup_runs_total.clone()))?;
  registry.register(Box::new(retention_cleanup_removed_segments_total.clone()))?;
  registry.register(Box::new(compaction_cleanup_runs_total.clone()))?;
  registry.register(Box::new(compaction_cleanup_compacted_partitions_total.clone()))?;
  registry.register(Box::new(compaction_cleanup_removed_records_total.clone()))?;
  registry.register(Box::new(produce_requests_total_all.clone()))?;
  registry.register(Box::new(produce_requests_total.clone()))?;
  registry.register(Box::new(produced_records_total_all.clone()))?;
  registry.register(Box::new(produced_records_total.clone()))?;
  registry.register(Box::new(visible_topics_current.clone()))?;
  registry.register(Box::new(topics_with_backlog_current.clone()))?;
  registry.register(Box::new(total_topic_backlog_records_current.clone()))?;
  registry.register(Box::new(max_topic_backlog_records_current.clone()))?;
  registry.register(Box::new(max_partition_backlog_records_current.clone()))?;
  registry.register(Box::new(consumer_groups_current.clone()))?;
  registry.register(Box::new(consumer_groups_with_lag_current.clone()))?;
  registry.register(Box::new(total_consumer_group_backlog_records_current.clone()))?;
  registry.register(Box::new(total_consumer_group_lag_records_current.clone()))?;
  registry.register(Box::new(max_consumer_group_lag_records_current.clone()))?;

  let metrics = BrokerMetrics {
    registry,
    tcp_connections_total,
    commands_total_all,
    commands_total,
    command_errors_total_all,
    command_errors_total,
    rebalances_total,
    rebalances_total_by_strategy,
    rebalance_assignments_total,
    rebalance_moved_partitions_total,
    raft_client_write_retries_total,
    raft_client_write_failures_total,
    retention_cleanup_runs_total,
    retention_cleanup_removed_segments_total,
    compaction_cleanup_runs_total,
    compaction_cleanup_compacted_partitions_total,
    compaction_cleanup_removed_records_total,
    produce_requests_total_all,
    produce_requests_total,
    produced_records_total_all,
    produced_records_total,
    visible_topics_current,
    topics_with_backlog_current,
    total_topic_backlog_records_current,
    max_topic_backlog_records_current,
    max_partition_backlog_records_current,
    consumer_groups_current,
    consumer_groups_with_lag_current,
    total_consumer_group_backlog_records_current,
    total_consumer_group_lag_records_current,
    max_consumer_group_lag_records_current,
  };

  let _ = METRICS.set(metrics);
  Ok(())
}

pub fn record_tcp_connection() {
  if let Some(metrics) = METRICS.get() {
    metrics.tcp_connections_total.inc();
  }
}

pub fn record_command(command_id: u16) {
  if let Some(metrics) = METRICS.get() {
    metrics.commands_total_all.inc();
    let label = command_id.to_string();
    metrics.commands_total.with_label_values(&[&label]).inc();
  }
}

pub fn record_command_error(code: &str) {
  if let Some(metrics) = METRICS.get() {
    metrics.command_errors_total_all.inc();
    metrics
      .command_errors_total
      .with_label_values(&[code])
      .inc();
  }
}

pub fn record_rebalance(strategy: &str, assignments: u64, moved_partitions: u64) {
  if let Some(metrics) = METRICS.get() {
    metrics.rebalances_total.inc();
    metrics
      .rebalances_total_by_strategy
      .with_label_values(&[strategy])
      .inc();
    metrics.rebalance_assignments_total.inc_by(assignments);
    metrics
      .rebalance_moved_partitions_total
      .inc_by(moved_partitions);
  }
}

pub fn record_raft_client_write_retry() {
  if let Some(metrics) = METRICS.get() {
    metrics.raft_client_write_retries_total.inc();
  }
}

pub fn record_raft_client_write_failure() {
  if let Some(metrics) = METRICS.get() {
    metrics.raft_client_write_failures_total.inc();
  }
}

pub fn record_compaction_cleanup(compacted_partitions: u64, removed_records: u64) {
  if let Some(metrics) = METRICS.get() {
    metrics.compaction_cleanup_runs_total.inc();
    metrics
      .compaction_cleanup_compacted_partitions_total
      .inc_by(compacted_partitions);
    metrics
      .compaction_cleanup_removed_records_total
      .inc_by(removed_records);
  }
}

pub fn record_retention_cleanup(removed_segments: u64) {
  if let Some(metrics) = METRICS.get() {
    metrics.retention_cleanup_runs_total.inc();
    metrics
      .retention_cleanup_removed_segments_total
      .inc_by(removed_segments);
  }
}

pub fn record_produce(partitioning: &str, records: u64) {
  if let Some(metrics) = METRICS.get() {
    metrics.produce_requests_total_all.inc();
    metrics
      .produce_requests_total
      .with_label_values(&[partitioning])
      .inc();
    metrics.produced_records_total_all.inc_by(records);
    metrics
      .produced_records_total
      .with_label_values(&[partitioning])
      .inc_by(records);
  }
}

pub fn update_stream_metrics(snapshot: &crate::service::StreamMetricsSnapshot) {
  if let Some(metrics) = METRICS.get() {
    metrics.visible_topics_current.set(snapshot.topic_count as i64);
    metrics
      .topics_with_backlog_current
      .set(snapshot.topics_with_backlog as i64);
    metrics
      .total_topic_backlog_records_current
      .set(snapshot.total_topic_backlog_records as i64);
    metrics
      .max_topic_backlog_records_current
      .set(snapshot.max_topic_backlog_records as i64);
    metrics
      .max_partition_backlog_records_current
      .set(snapshot.max_partition_backlog_records as i64);
    metrics
      .consumer_groups_current
      .set(snapshot.consumer_group_count as i64);
    metrics
      .consumer_groups_with_lag_current
      .set(snapshot.consumer_groups_with_lag as i64);
    metrics
      .total_consumer_group_backlog_records_current
      .set(snapshot.total_consumer_group_backlog_records as i64);
    metrics
      .total_consumer_group_lag_records_current
      .set(snapshot.total_consumer_group_lag_records as i64);
    metrics
      .max_consumer_group_lag_records_current
      .set(snapshot.max_consumer_group_lag_records as i64);
  }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
  pub tcp_connections_total: u64,
  pub commands_total: u64,
  pub command_errors_total: u64,
  pub raft_client_write_retries_total: u64,
  pub raft_client_write_failures_total: u64,
  pub retention_cleanup_runs_total: u64,
  pub retention_cleanup_removed_segments_total: u64,
  pub compaction_cleanup_runs_total: u64,
  pub compaction_cleanup_compacted_partitions_total: u64,
  pub compaction_cleanup_removed_records_total: u64,
  pub produce_requests_total: u64,
  pub produced_records_total: u64,
  pub explicit_produce_requests_total: u64,
  pub round_robin_produce_requests_total: u64,
  pub key_hash_produce_requests_total: u64,
  pub explicit_produced_records_total: u64,
  pub round_robin_produced_records_total: u64,
  pub key_hash_produced_records_total: u64,
  pub visible_topics_current: u64,
  pub topics_with_backlog_current: u64,
  pub total_topic_backlog_records_current: u64,
  pub max_topic_backlog_records_current: u64,
  pub max_partition_backlog_records_current: u64,
  pub consumer_groups_current: u64,
  pub consumer_groups_with_lag_current: u64,
  pub total_consumer_group_backlog_records_current: u64,
  pub total_consumer_group_lag_records_current: u64,
  pub max_consumer_group_lag_records_current: u64,
}

pub fn snapshot() -> MetricsSnapshot {
  let Some(metrics) = METRICS.get() else {
    return MetricsSnapshot {
      tcp_connections_total: 0,
      commands_total: 0,
      command_errors_total: 0,
      raft_client_write_retries_total: 0,
      raft_client_write_failures_total: 0,
      retention_cleanup_runs_total: 0,
      retention_cleanup_removed_segments_total: 0,
      compaction_cleanup_runs_total: 0,
      compaction_cleanup_compacted_partitions_total: 0,
      compaction_cleanup_removed_records_total: 0,
      produce_requests_total: 0,
      produced_records_total: 0,
      explicit_produce_requests_total: 0,
      round_robin_produce_requests_total: 0,
      key_hash_produce_requests_total: 0,
      explicit_produced_records_total: 0,
      round_robin_produced_records_total: 0,
      key_hash_produced_records_total: 0,
      visible_topics_current: 0,
      topics_with_backlog_current: 0,
      total_topic_backlog_records_current: 0,
      max_topic_backlog_records_current: 0,
      max_partition_backlog_records_current: 0,
      consumer_groups_current: 0,
      consumer_groups_with_lag_current: 0,
      total_consumer_group_backlog_records_current: 0,
      total_consumer_group_lag_records_current: 0,
      max_consumer_group_lag_records_current: 0,
    };
  };

  MetricsSnapshot {
    tcp_connections_total: metrics.tcp_connections_total.get(),
    commands_total: metrics.commands_total_all.get(),
    command_errors_total: metrics.command_errors_total_all.get(),
    raft_client_write_retries_total: metrics.raft_client_write_retries_total.get(),
    raft_client_write_failures_total: metrics.raft_client_write_failures_total.get(),
    retention_cleanup_runs_total: metrics.retention_cleanup_runs_total.get(),
    retention_cleanup_removed_segments_total: metrics.retention_cleanup_removed_segments_total.get(),
    compaction_cleanup_runs_total: metrics.compaction_cleanup_runs_total.get(),
    compaction_cleanup_compacted_partitions_total: metrics
      .compaction_cleanup_compacted_partitions_total
      .get(),
    compaction_cleanup_removed_records_total: metrics.compaction_cleanup_removed_records_total.get(),
    produce_requests_total: metrics.produce_requests_total_all.get(),
    produced_records_total: metrics.produced_records_total_all.get(),
    explicit_produce_requests_total: metrics
      .produce_requests_total
      .with_label_values(&["explicit"])
      .get(),
    round_robin_produce_requests_total: metrics
      .produce_requests_total
      .with_label_values(&["round_robin"])
      .get(),
    key_hash_produce_requests_total: metrics
      .produce_requests_total
      .with_label_values(&["key_hash"])
      .get(),
    explicit_produced_records_total: metrics
      .produced_records_total
      .with_label_values(&["explicit"])
      .get(),
    round_robin_produced_records_total: metrics
      .produced_records_total
      .with_label_values(&["round_robin"])
      .get(),
    key_hash_produced_records_total: metrics
      .produced_records_total
      .with_label_values(&["key_hash"])
      .get(),
    visible_topics_current: metrics.visible_topics_current.get() as u64,
    topics_with_backlog_current: metrics.topics_with_backlog_current.get() as u64,
    total_topic_backlog_records_current: metrics.total_topic_backlog_records_current.get() as u64,
    max_topic_backlog_records_current: metrics.max_topic_backlog_records_current.get() as u64,
    max_partition_backlog_records_current: metrics.max_partition_backlog_records_current.get()
      as u64,
    consumer_groups_current: metrics.consumer_groups_current.get() as u64,
    consumer_groups_with_lag_current: metrics.consumer_groups_with_lag_current.get() as u64,
    total_consumer_group_backlog_records_current: metrics
      .total_consumer_group_backlog_records_current
      .get() as u64,
    total_consumer_group_lag_records_current: metrics
      .total_consumer_group_lag_records_current
      .get() as u64,
    max_consumer_group_lag_records_current: metrics.max_consumer_group_lag_records_current.get()
      as u64,
  }
}

pub fn render() -> Result<String, String> {
  let Some(metrics) = METRICS.get() else {
    return Ok(String::new());
  };

  let metric_families = metrics.registry.gather();
  let mut buf = Vec::new();
  let encoder = TextEncoder::new();
  encoder
    .encode(&metric_families, &mut buf)
    .map_err(|err| format!("prometheus encode failed: {err}"))?;
  String::from_utf8(buf).map_err(|err| format!("invalid utf8 metrics output: {err}"))
}
