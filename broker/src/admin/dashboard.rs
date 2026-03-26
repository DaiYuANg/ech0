use askama::Template;
use axum::{extract::State, response::IntoResponse};

use crate::{metrics, producer_state};

use super::{AdminState, HotPartitionSummary, error_health_response, render_html, uptime_seconds};

#[derive(Debug, Template)]
#[template(path = "dashboard.html")]
struct DashboardTemplate {
  uptime_seconds: u64,
  health_status: String,
  runtime_mode: String,
  raft_leader: String,
  tcp_connections_total: u64,
  commands_total: u64,
  command_errors_total: u64,
  command_error_rate_percent: String,
  produce_requests_total: u64,
  produced_records_total: u64,
  explicit_produce_requests_total: u64,
  round_robin_produce_requests_total: u64,
  key_hash_produce_requests_total: u64,
  explicit_produced_records_total: u64,
  round_robin_produced_records_total: u64,
  key_hash_produced_records_total: u64,
  hot_partitions: Vec<HotPartitionSummary>,
  raft_client_write_retries_total: u64,
  raft_client_write_failures_total: u64,
  retention_cleanup_runs_total: u64,
  retention_cleanup_removed_segments_total: u64,
  retention_cleanup_interval_secs: u64,
  retention_cleanup_ready: bool,
  retention_cleanup_readiness_reason: Option<String>,
  retention_cleanup_last_run_started_at_ms: Option<u64>,
  retention_cleanup_last_run_finished_at_ms: Option<u64>,
  retention_cleanup_last_success_at_ms: Option<u64>,
  retention_cleanup_last_removed_segments: u64,
  retention_cleanup_last_error: Option<String>,
  compaction_cleanup_runs_total: u64,
  compaction_cleanup_compacted_partitions_total: u64,
  compaction_cleanup_removed_records_total: u64,
  compaction_cleanup_interval_secs: u64,
  compaction_sealed_segment_batch: usize,
  compaction_cleanup_ready: bool,
  compaction_cleanup_readiness_reason: Option<String>,
  compaction_cleanup_last_run_started_at_ms: Option<u64>,
  compaction_cleanup_last_run_finished_at_ms: Option<u64>,
  compaction_cleanup_last_success_at_ms: Option<u64>,
  compaction_cleanup_last_error: Option<String>,
  topic_count: usize,
  topic_error: Option<String>,
}

pub(super) async fn ui_dashboard(State(state): State<AdminState>) -> impl IntoResponse {
  let snapshot = metrics::snapshot();
  let health = (state.health_snapshot)()
    .unwrap_or_else(|err| error_health_response(&state.ops_config, err));
  let command_error_rate_percent = if snapshot.commands_total == 0 {
    0.0
  } else {
    (snapshot.command_errors_total as f64 / snapshot.commands_total as f64) * 100.0
  };
  let producer_snapshot = producer_state::snapshot();
  let (topic_count, topic_error) = match (state.topic_snapshot)() {
    Ok(topics) => (topics.len(), None),
    Err(err) => (0, Some(err)),
  };

  let template = DashboardTemplate {
    uptime_seconds: uptime_seconds(),
    health_status: health.status,
    runtime_mode: health.runtime_mode,
    raft_leader: health
      .raft
      .as_ref()
      .and_then(|raft| raft.leader_id)
      .map(|leader_id| leader_id.to_string())
      .unwrap_or_else(|| "unknown".to_owned()),
    tcp_connections_total: snapshot.tcp_connections_total,
    commands_total: snapshot.commands_total,
    command_errors_total: snapshot.command_errors_total,
    command_error_rate_percent: format!("{command_error_rate_percent:.2}"),
    produce_requests_total: snapshot.produce_requests_total,
    produced_records_total: snapshot.produced_records_total,
    explicit_produce_requests_total: snapshot.explicit_produce_requests_total,
    round_robin_produce_requests_total: snapshot.round_robin_produce_requests_total,
    key_hash_produce_requests_total: snapshot.key_hash_produce_requests_total,
    explicit_produced_records_total: snapshot.explicit_produced_records_total,
    round_robin_produced_records_total: snapshot.round_robin_produced_records_total,
    key_hash_produced_records_total: snapshot.key_hash_produced_records_total,
    hot_partitions: producer_snapshot
      .hot_partitions
      .into_iter()
      .map(|item| HotPartitionSummary {
        topic: item.topic,
        partition: item.partition,
        produced_records_total: item.produced_records_total,
        last_partitioning: item.last_partitioning,
      })
      .collect(),
    raft_client_write_retries_total: snapshot.raft_client_write_retries_total,
    raft_client_write_failures_total: snapshot.raft_client_write_failures_total,
    retention_cleanup_runs_total: snapshot.retention_cleanup_runs_total,
    retention_cleanup_removed_segments_total: snapshot.retention_cleanup_removed_segments_total,
    retention_cleanup_interval_secs: state.ops_config.retention_cleanup_interval_secs,
    retention_cleanup_ready: health.background.retention_cleanup.ready,
    retention_cleanup_readiness_reason: health.background.retention_cleanup.readiness_reason.clone(),
    retention_cleanup_last_run_started_at_ms: health
      .background
      .retention_cleanup
      .last_run_started_at_ms,
    retention_cleanup_last_run_finished_at_ms: health
      .background
      .retention_cleanup
      .last_run_finished_at_ms,
    retention_cleanup_last_success_at_ms: health.background.retention_cleanup.last_success_at_ms,
    retention_cleanup_last_removed_segments: health
      .background
      .retention_cleanup
      .last_removed_segments,
    retention_cleanup_last_error: health.background.retention_cleanup.last_error.clone(),
    compaction_cleanup_runs_total: snapshot.compaction_cleanup_runs_total,
    compaction_cleanup_compacted_partitions_total: snapshot
      .compaction_cleanup_compacted_partitions_total,
    compaction_cleanup_removed_records_total: snapshot.compaction_cleanup_removed_records_total,
    compaction_cleanup_interval_secs: state.ops_config.compaction_cleanup_interval_secs,
    compaction_sealed_segment_batch: state.ops_config.compaction_sealed_segment_batch,
    compaction_cleanup_ready: health.background.compaction_cleanup.ready,
    compaction_cleanup_readiness_reason: health
      .background
      .compaction_cleanup
      .readiness_reason
      .clone(),
    compaction_cleanup_last_run_started_at_ms: health
      .background
      .compaction_cleanup
      .last_run_started_at_ms,
    compaction_cleanup_last_run_finished_at_ms: health
      .background
      .compaction_cleanup
      .last_run_finished_at_ms,
    compaction_cleanup_last_success_at_ms: health
      .background
      .compaction_cleanup
      .last_success_at_ms,
    compaction_cleanup_last_error: health.background.compaction_cleanup.last_error.clone(),
    topic_count,
    topic_error,
  };

  render_html(template.render(), "dashboard")
}
