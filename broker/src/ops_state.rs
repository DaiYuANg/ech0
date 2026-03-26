use std::sync::{OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionCleanupState {
  pub enabled: bool,
  pub interval_secs: u64,
  pub sealed_segment_batch: usize,
  pub configured_at_ms: Option<u64>,
  pub last_run_started_at_ms: Option<u64>,
  pub last_run_finished_at_ms: Option<u64>,
  pub last_success_at_ms: Option<u64>,
  pub last_compacted_partitions: u64,
  pub last_removed_records: u64,
  pub last_error: Option<String>,
}

impl Default for CompactionCleanupState {
  fn default() -> Self {
    Self {
      enabled: false,
      interval_secs: 0,
      sealed_segment_batch: 0,
      configured_at_ms: None,
      last_run_started_at_ms: None,
      last_run_finished_at_ms: None,
      last_success_at_ms: None,
      last_compacted_partitions: 0,
      last_removed_records: 0,
      last_error: None,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionCleanupState {
  pub enabled: bool,
  pub interval_secs: u64,
  pub configured_at_ms: Option<u64>,
  pub last_run_started_at_ms: Option<u64>,
  pub last_run_finished_at_ms: Option<u64>,
  pub last_success_at_ms: Option<u64>,
  pub last_removed_segments: u64,
  pub last_error: Option<String>,
}

impl Default for RetentionCleanupState {
  fn default() -> Self {
    Self {
      enabled: false,
      interval_secs: 0,
      configured_at_ms: None,
      last_run_started_at_ms: None,
      last_run_finished_at_ms: None,
      last_success_at_ms: None,
      last_removed_segments: 0,
      last_error: None,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsRuntimeState {
  pub retention_cleanup: RetentionCleanupState,
  pub compaction_cleanup: CompactionCleanupState,
}

impl Default for OpsRuntimeState {
  fn default() -> Self {
    Self {
      retention_cleanup: RetentionCleanupState::default(),
      compaction_cleanup: CompactionCleanupState::default(),
    }
  }
}

static OPS_STATE: OnceLock<RwLock<OpsRuntimeState>> = OnceLock::new();

fn state_lock() -> &'static RwLock<OpsRuntimeState> {
  OPS_STATE.get_or_init(|| RwLock::new(OpsRuntimeState::default()))
}

fn now_ms() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64
}

pub fn configure_compaction_cleanup(enabled: bool, interval_secs: u64, sealed_segment_batch: usize) {
  if let Ok(mut state) = state_lock().write() {
    state.compaction_cleanup.enabled = enabled;
    state.compaction_cleanup.interval_secs = interval_secs;
    state.compaction_cleanup.sealed_segment_batch = sealed_segment_batch;
    state.compaction_cleanup.configured_at_ms = enabled.then(now_ms);
    if !enabled {
      state.compaction_cleanup.last_error = None;
    }
  }
}

pub fn configure_retention_cleanup(enabled: bool, interval_secs: u64) {
  if let Ok(mut state) = state_lock().write() {
    state.retention_cleanup.enabled = enabled;
    state.retention_cleanup.interval_secs = interval_secs;
    state.retention_cleanup.configured_at_ms = enabled.then(now_ms);
    if !enabled {
      state.retention_cleanup.last_error = None;
    }
  }
}

pub fn mark_retention_cleanup_started() {
  if let Ok(mut state) = state_lock().write() {
    state.retention_cleanup.last_run_started_at_ms = Some(now_ms());
  }
}

pub fn mark_retention_cleanup_success(removed_segments: u64) {
  let now = now_ms();
  if let Ok(mut state) = state_lock().write() {
    state.retention_cleanup.last_run_finished_at_ms = Some(now);
    state.retention_cleanup.last_success_at_ms = Some(now);
    state.retention_cleanup.last_removed_segments = removed_segments;
    state.retention_cleanup.last_error = None;
  }
}

pub fn mark_retention_cleanup_failure(error: impl Into<String>) {
  if let Ok(mut state) = state_lock().write() {
    state.retention_cleanup.last_run_finished_at_ms = Some(now_ms());
    state.retention_cleanup.last_error = Some(error.into());
  }
}

pub fn mark_compaction_cleanup_started() {
  if let Ok(mut state) = state_lock().write() {
    state.compaction_cleanup.last_run_started_at_ms = Some(now_ms());
  }
}

pub fn mark_compaction_cleanup_success(compacted_partitions: u64, removed_records: u64) {
  let now = now_ms();
  if let Ok(mut state) = state_lock().write() {
    state.compaction_cleanup.last_run_finished_at_ms = Some(now);
    state.compaction_cleanup.last_success_at_ms = Some(now);
    state.compaction_cleanup.last_compacted_partitions = compacted_partitions;
    state.compaction_cleanup.last_removed_records = removed_records;
    state.compaction_cleanup.last_error = None;
  }
}

pub fn mark_compaction_cleanup_failure(error: impl Into<String>) {
  if let Ok(mut state) = state_lock().write() {
    state.compaction_cleanup.last_run_finished_at_ms = Some(now_ms());
    state.compaction_cleanup.last_error = Some(error.into());
  }
}

pub fn snapshot() -> OpsRuntimeState {
  state_lock()
    .read()
    .map(|state| state.clone())
    .unwrap_or_default()
}
