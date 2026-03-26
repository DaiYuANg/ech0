use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use utoipa::ToSchema;

use crate::{
  ops_state::{CompactionCleanupState, OpsRuntimeState, RetentionCleanupState},
  service::BrokerRuntimeHealth,
};

const WORKER_STALE_INTERVAL_FACTOR: u64 = 3;

#[derive(Debug, Clone)]
pub struct OpsConfigSummary {
  pub retention_cleanup_enabled: bool,
  pub retention_cleanup_interval_secs: u64,
  pub compaction_cleanup_enabled: bool,
  pub compaction_cleanup_interval_secs: u64,
  pub compaction_sealed_segment_batch: usize,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RaftHealthResponse {
  pub node_id: u64,
  pub initialized: bool,
  pub known_nodes: usize,
  pub leader_id: Option<u64>,
  pub local_is_leader: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RetentionCleanupHealthResponse {
  pub enabled: bool,
  pub interval_secs: u64,
  pub ready: bool,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub readiness_reason: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub last_run_started_at_ms: Option<u64>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub last_run_finished_at_ms: Option<u64>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub last_success_at_ms: Option<u64>,
  pub last_removed_segments: u64,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CompactionCleanupHealthResponse {
  pub enabled: bool,
  pub interval_secs: u64,
  pub sealed_segment_batch: usize,
  pub ready: bool,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub readiness_reason: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub last_run_started_at_ms: Option<u64>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub last_run_finished_at_ms: Option<u64>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub last_success_at_ms: Option<u64>,
  pub last_compacted_partitions: u64,
  pub last_removed_records: u64,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct BackgroundWorkersHealthResponse {
  pub retention_cleanup: RetentionCleanupHealthResponse,
  pub compaction_cleanup: CompactionCleanupHealthResponse,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HealthResponse {
  pub status: String,
  pub runtime_mode: String,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub error: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub raft: Option<RaftHealthResponse>,
  pub background: BackgroundWorkersHealthResponse,
}

pub fn build_health_response(
  runtime: BrokerRuntimeHealth,
  ops_config: &OpsConfigSummary,
  ops: &OpsRuntimeState,
) -> HealthResponse {
  let background = build_background_workers_health(ops_config, ops, now_ms());
  let mut status = runtime.status.to_owned();
  let mut errors = Vec::new();

  if !background.retention_cleanup.ready {
    errors.push(format!(
      "retention cleanup not ready: {}",
      background
        .retention_cleanup
        .readiness_reason
        .clone()
        .unwrap_or_else(|| "unknown".to_owned())
    ));
  }
  if !background.compaction_cleanup.ready {
    errors.push(format!(
      "compaction cleanup not ready: {}",
      background
        .compaction_cleanup
        .readiness_reason
        .clone()
        .unwrap_or_else(|| "unknown".to_owned())
    ));
  }
  if !errors.is_empty() && status == "ok" {
    status = "degraded".to_owned();
  }

  HealthResponse {
    status,
    runtime_mode: runtime.runtime_mode.to_owned(),
    error: (!errors.is_empty()).then(|| errors.join("; ")),
    raft: runtime.raft.map(|raft| RaftHealthResponse {
      node_id: raft.node_id,
      initialized: raft.initialized,
      known_nodes: raft.known_nodes,
      leader_id: raft.leader_id,
      local_is_leader: raft.local_is_leader,
    }),
    background,
  }
}

pub fn error_health_response(
  ops_config: &OpsConfigSummary,
  error: impl Into<String>,
) -> HealthResponse {
  let error = error.into();
  HealthResponse {
    status: "error".to_owned(),
    runtime_mode: "unknown".to_owned(),
    error: Some(error.clone()),
    raft: None,
    background: BackgroundWorkersHealthResponse {
      retention_cleanup: RetentionCleanupHealthResponse {
        enabled: ops_config.retention_cleanup_enabled,
        interval_secs: ops_config.retention_cleanup_interval_secs,
        ready: !ops_config.retention_cleanup_enabled,
        readiness_reason: ops_config
          .retention_cleanup_enabled
          .then(|| error.clone()),
        last_run_started_at_ms: None,
        last_run_finished_at_ms: None,
        last_success_at_ms: None,
        last_removed_segments: 0,
        last_error: None,
      },
      compaction_cleanup: CompactionCleanupHealthResponse {
        enabled: ops_config.compaction_cleanup_enabled,
        interval_secs: ops_config.compaction_cleanup_interval_secs,
        sealed_segment_batch: ops_config.compaction_sealed_segment_batch,
        ready: !ops_config.compaction_cleanup_enabled,
        readiness_reason: ops_config
          .compaction_cleanup_enabled
          .then_some(error),
        last_run_started_at_ms: None,
        last_run_finished_at_ms: None,
        last_success_at_ms: None,
        last_compacted_partitions: 0,
        last_removed_records: 0,
        last_error: None,
      },
    },
  }
}

fn build_background_workers_health(
  ops_config: &OpsConfigSummary,
  ops: &OpsRuntimeState,
  now_ms: u64,
) -> BackgroundWorkersHealthResponse {
  BackgroundWorkersHealthResponse {
    retention_cleanup: build_retention_cleanup_health(
      ops_config.retention_cleanup_enabled,
      ops_config.retention_cleanup_interval_secs,
      &ops.retention_cleanup,
      now_ms,
    ),
    compaction_cleanup: build_compaction_cleanup_health(
      ops_config.compaction_cleanup_enabled,
      ops_config.compaction_cleanup_interval_secs,
      ops_config.compaction_sealed_segment_batch,
      &ops.compaction_cleanup,
      now_ms,
    ),
  }
}

fn build_retention_cleanup_health(
  enabled: bool,
  interval_secs: u64,
  state: &RetentionCleanupState,
  now_ms: u64,
) -> RetentionCleanupHealthResponse {
  let (ready, readiness_reason) = evaluate_worker_readiness(
    enabled,
    interval_secs,
    state.configured_at_ms,
    state.last_success_at_ms,
    state.last_error.as_deref(),
    now_ms,
  );
  RetentionCleanupHealthResponse {
    enabled,
    interval_secs,
    ready,
    readiness_reason,
    last_run_started_at_ms: state.last_run_started_at_ms,
    last_run_finished_at_ms: state.last_run_finished_at_ms,
    last_success_at_ms: state.last_success_at_ms,
    last_removed_segments: state.last_removed_segments,
    last_error: state.last_error.clone(),
  }
}

fn build_compaction_cleanup_health(
  enabled: bool,
  interval_secs: u64,
  sealed_segment_batch: usize,
  state: &CompactionCleanupState,
  now_ms: u64,
) -> CompactionCleanupHealthResponse {
  let (ready, readiness_reason) = evaluate_worker_readiness(
    enabled,
    interval_secs,
    state.configured_at_ms,
    state.last_success_at_ms,
    state.last_error.as_deref(),
    now_ms,
  );
  CompactionCleanupHealthResponse {
    enabled,
    interval_secs,
    sealed_segment_batch,
    ready,
    readiness_reason,
    last_run_started_at_ms: state.last_run_started_at_ms,
    last_run_finished_at_ms: state.last_run_finished_at_ms,
    last_success_at_ms: state.last_success_at_ms,
    last_compacted_partitions: state.last_compacted_partitions,
    last_removed_records: state.last_removed_records,
    last_error: state.last_error.clone(),
  }
}

fn evaluate_worker_readiness(
  enabled: bool,
  interval_secs: u64,
  configured_at_ms: Option<u64>,
  last_success_at_ms: Option<u64>,
  last_error: Option<&str>,
  now_ms: u64,
) -> (bool, Option<String>) {
  if !enabled {
    return (true, None);
  }

  let stale_after_ms = worker_stale_after_ms(interval_secs);
  if let Some(last_success_at_ms) = last_success_at_ms {
    let success_age_ms = now_ms.saturating_sub(last_success_at_ms);
    if success_age_ms > stale_after_ms {
      return (
        false,
        Some(stale_reason(
          "last successful run",
          success_age_ms,
          stale_after_ms,
          last_error,
        )),
      );
    }
    return (true, None);
  }

  let Some(configured_at_ms) = configured_at_ms else {
    return (true, None);
  };
  let configured_age_ms = now_ms.saturating_sub(configured_at_ms);
  if configured_age_ms > stale_after_ms {
    return (
      false,
      Some(stale_reason(
        "worker has not completed a successful run since configuration",
        configured_age_ms,
        stale_after_ms,
        last_error,
      )),
    );
  }

  (true, None)
}

fn worker_stale_after_ms(interval_secs: u64) -> u64 {
  interval_secs
    .max(1)
    .saturating_mul(WORKER_STALE_INTERVAL_FACTOR)
    .saturating_mul(1_000)
}

fn stale_reason(context: &str, age_ms: u64, stale_after_ms: u64, last_error: Option<&str>) -> String {
  match last_error {
    Some(last_error) => format!(
      "{context} is stale after {age_ms} ms (threshold {stale_after_ms} ms); last error: {last_error}"
    ),
    None => format!("{context} is stale after {age_ms} ms (threshold {stale_after_ms} ms)"),
  }
}

fn now_ms() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64
}

#[cfg(test)]
mod tests {
  use crate::{
    ops_state::{CompactionCleanupState, OpsRuntimeState, RetentionCleanupState},
    service::BrokerRuntimeHealth,
  };

  use super::{OpsConfigSummary, build_health_response};

  #[test]
  fn enabled_worker_is_ready_during_startup_grace() {
    let now_ms = 90_000;
    let ops = OpsRuntimeState {
      retention_cleanup: RetentionCleanupState {
        enabled: true,
        interval_secs: 30,
        configured_at_ms: Some(60_500),
        ..RetentionCleanupState::default()
      },
      compaction_cleanup: CompactionCleanupState::default(),
    };
    let health = super::build_background_workers_health(
      &OpsConfigSummary {
        retention_cleanup_enabled: true,
        retention_cleanup_interval_secs: 30,
        compaction_cleanup_enabled: false,
        compaction_cleanup_interval_secs: 60,
        compaction_sealed_segment_batch: 2,
      },
      &ops,
      now_ms,
    );

    assert!(health.retention_cleanup.ready);
    assert!(health.retention_cleanup.readiness_reason.is_none());
  }

  #[test]
  fn worker_without_success_becomes_not_ready_after_threshold() {
    let now_ms = 120_001;
    let ops = OpsRuntimeState {
      retention_cleanup: RetentionCleanupState {
        enabled: true,
        interval_secs: 30,
        configured_at_ms: Some(30_000),
        last_error: Some("disk busy".to_owned()),
        ..RetentionCleanupState::default()
      },
      compaction_cleanup: CompactionCleanupState::default(),
    };
    let health = super::build_background_workers_health(
      &OpsConfigSummary {
        retention_cleanup_enabled: true,
        retention_cleanup_interval_secs: 30,
        compaction_cleanup_enabled: false,
        compaction_cleanup_interval_secs: 60,
        compaction_sealed_segment_batch: 2,
      },
      &ops,
      now_ms,
    );

    assert!(!health.retention_cleanup.ready);
    assert!(
      health
        .retention_cleanup
        .readiness_reason
        .as_deref()
        .unwrap_or_default()
        .contains("disk busy")
    );
  }

  #[test]
  fn stale_worker_degrades_overall_health() {
    let ops = OpsRuntimeState {
      retention_cleanup: RetentionCleanupState {
        enabled: true,
        interval_secs: 30,
        configured_at_ms: Some(0),
        last_success_at_ms: Some(1_000),
        ..RetentionCleanupState::default()
      },
      compaction_cleanup: CompactionCleanupState::default(),
    };
    let health = build_health_response(
      BrokerRuntimeHealth {
        status: "ok",
        runtime_mode: "standalone",
        raft: None,
      },
      &OpsConfigSummary {
        retention_cleanup_enabled: true,
        retention_cleanup_interval_secs: 30,
        compaction_cleanup_enabled: false,
        compaction_cleanup_interval_secs: 60,
        compaction_sealed_segment_batch: 2,
      },
      &ops,
    );

    assert_eq!(health.status, "degraded");
    assert!(
      health
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("retention cleanup not ready")
    );
  }
}
