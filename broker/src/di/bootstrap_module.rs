use std::sync::Arc;

use tracing::{info, warn};

use crate::{
  config::AppConfig,
  logging, metrics,
  ops_state,
  server::{HandlerLimits, configure_handler_limits},
};
use store::{Result, SegmentLog, StoreError};

pub struct BootstrapModule {
  app: AppConfig,
}

impl BootstrapModule {
  pub fn new(app: AppConfig) -> Self {
    Self { app }
  }

  pub fn init_observability(&self) -> Result<()> {
    logging::init_logging(&self.app.logging)
      .map_err(|err| StoreError::Codec(format!("failed to init logging: {err}")))?;
    metrics::init_metrics()
      .map_err(|err| StoreError::Codec(format!("failed to init metrics: {err}")))?;
    Ok(())
  }

  pub fn configure_runtime_limits(&self) {
    configure_handler_limits(HandlerLimits {
      max_payload_bytes: self.app.broker.max_payload_bytes,
      max_batch_payload_bytes: self.app.broker.max_batch_payload_bytes,
      max_fetch_records: self.app.broker.max_fetch_records,
      max_fetch_wait_ms: self.app.broker.max_fetch_wait_ms,
    });
    ops_state::configure_retention_cleanup(
      self.app.storage.retention_cleanup_enabled,
      self.app.storage.retention_cleanup_interval_secs,
    );
    ops_state::configure_compaction_cleanup(
      self.app.storage.compaction_cleanup_enabled,
      self.app.storage.compaction_cleanup_interval_secs,
      self.app.storage.compaction_sealed_segment_batch,
    );
  }

  pub fn spawn_retention_cleanup_task(&self, log: Arc<SegmentLog>) {
    if self.app.storage.retention_cleanup_enabled {
      tokio::spawn(run_retention_cleanup_task(
        log,
        self.app.storage.retention_cleanup_interval_secs,
      ));
    }
  }

  pub fn spawn_compaction_cleanup_task(&self, log: Arc<SegmentLog>) {
    if self.app.storage.compaction_cleanup_enabled {
      tokio::spawn(run_compaction_cleanup_task(
        log,
        self.app.storage.compaction_cleanup_interval_secs,
        self.app.storage.compaction_sealed_segment_batch,
      ));
    }
  }
}

async fn run_retention_cleanup_task(
  log: Arc<SegmentLog>,
  interval_secs: u64,
) -> std::io::Result<()> {
  let interval_secs = interval_secs.max(1);
  let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
  interval.tick().await;
  loop {
    interval.tick().await;
    ops_state::mark_retention_cleanup_started();
    match log.enforce_retention_once() {
      Ok(removed_segments) if removed_segments > 0 => {
        metrics::record_retention_cleanup(removed_segments as u64);
        ops_state::mark_retention_cleanup_success(removed_segments as u64);
        info!(
          removed_segments,
          interval_secs, "retention cleanup removed old log segments"
        );
      }
      Ok(_) => {
        ops_state::mark_retention_cleanup_success(0);
      }
      Err(err) => {
        ops_state::mark_retention_cleanup_failure(err.to_string());
        warn!(error = %err, "retention cleanup failed");
      }
    }
  }
}

async fn run_compaction_cleanup_task(
  log: Arc<SegmentLog>,
  interval_secs: u64,
  compaction_sealed_segment_batch: usize,
) -> std::io::Result<()> {
  let interval_secs = interval_secs.max(1);
  let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
  interval.tick().await;
  loop {
    interval.tick().await;
    ops_state::mark_compaction_cleanup_started();
    match log.compact_once() {
      Ok((compacted_partitions, removed_records)) if compacted_partitions > 0 => {
        metrics::record_compaction_cleanup(compacted_partitions as u64, removed_records as u64);
        ops_state::mark_compaction_cleanup_success(
          compacted_partitions as u64,
          removed_records as u64,
        );
        info!(
          compacted_partitions,
          removed_records,
          interval_secs,
          compaction_sealed_segment_batch,
          "compaction cleanup rebuilt compacted partitions"
        );
      }
      Ok(_) => {
        ops_state::mark_compaction_cleanup_success(0, 0);
      }
      Err(err) => {
        ops_state::mark_compaction_cleanup_failure(err.to_string());
        warn!(error = %err, "compaction cleanup failed");
      }
    }
  }
}
