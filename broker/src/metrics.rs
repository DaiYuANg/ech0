use std::sync::OnceLock;

use prometheus::{Encoder, IntCounter, IntCounterVec, Opts, Registry, TextEncoder};

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

  registry.register(Box::new(tcp_connections_total.clone()))?;
  registry.register(Box::new(commands_total_all.clone()))?;
  registry.register(Box::new(commands_total.clone()))?;
  registry.register(Box::new(command_errors_total_all.clone()))?;
  registry.register(Box::new(command_errors_total.clone()))?;
  registry.register(Box::new(rebalances_total.clone()))?;
  registry.register(Box::new(rebalances_total_by_strategy.clone()))?;
  registry.register(Box::new(rebalance_assignments_total.clone()))?;
  registry.register(Box::new(rebalance_moved_partitions_total.clone()))?;

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

#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
  pub tcp_connections_total: u64,
  pub commands_total: u64,
  pub command_errors_total: u64,
}

pub fn snapshot() -> MetricsSnapshot {
  let Some(metrics) = METRICS.get() else {
    return MetricsSnapshot {
      tcp_connections_total: 0,
      commands_total: 0,
      command_errors_total: 0,
    };
  };

  MetricsSnapshot {
    tcp_connections_total: metrics.tcp_connections_total.get(),
    commands_total: metrics.commands_total_all.get(),
    command_errors_total: metrics.command_errors_total_all.get(),
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
