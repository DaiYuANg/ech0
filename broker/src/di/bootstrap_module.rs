use std::sync::Arc;

use tracing::{info, warn};

use crate::{
  config::AppConfig,
  logging, metrics,
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
      max_fetch_records: self.app.broker.max_fetch_records,
    });
  }

  pub fn spawn_retention_cleanup_task(&self, log: Arc<SegmentLog>) {
    if self.app.storage.retention_cleanup_enabled {
      tokio::spawn(run_retention_cleanup_task(
        log,
        self.app.storage.retention_cleanup_interval_secs,
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
    match log.enforce_retention_once() {
      Ok(removed_segments) if removed_segments > 0 => {
        info!(
          removed_segments,
          interval_secs, "retention cleanup removed old log segments"
        );
      }
      Ok(_) => {}
      Err(err) => {
        warn!(error = %err, "retention cleanup failed");
      }
    }
  }
}
