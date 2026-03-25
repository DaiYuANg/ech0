use std::future::Future;

use crate::{AppConfig, ConfigError, run_with_config, run_with_config_and_shutdown};

pub struct BrokerBuilder {
  config: AppConfig,
}

impl BrokerBuilder {
  pub fn new() -> Self {
    Self {
      config: AppConfig::default(),
    }
  }

  pub fn from_config(config: AppConfig) -> Self {
    Self { config }
  }

  pub fn from_env() -> Result<Self, ConfigError> {
    Ok(Self::from_config(AppConfig::load()?))
  }

  pub fn with_config(mut self, update: impl FnOnce(&mut AppConfig)) -> Self {
    update(&mut self.config);
    self
  }

  pub fn data_dir(mut self, value: impl Into<String>) -> Self {
    self.config.broker.data_dir = value.into();
    self
  }

  pub fn broker_bind_addr(mut self, value: impl Into<String>) -> Self {
    self.config.broker.bind_addr = value.into();
    self
  }

  pub fn admin_enabled(mut self, enabled: bool) -> Self {
    self.config.admin.enabled = enabled;
    self
  }

  pub fn admin_bind_addr(mut self, value: impl Into<String>) -> Self {
    self.config.admin.bind_addr = value.into();
    self
  }

  pub fn raft_enabled(mut self, enabled: bool) -> Self {
    self.config.raft.enabled = enabled;
    self
  }

  pub fn max_payload_bytes(mut self, value: usize) -> Self {
    self.config.broker.max_payload_bytes = value;
    self
  }

  pub fn max_fetch_records(mut self, value: usize) -> Self {
    self.config.broker.max_fetch_records = value;
    self
  }

  pub fn config(&self) -> &AppConfig {
    &self.config
  }

  pub fn config_mut(&mut self) -> &mut AppConfig {
    &mut self.config
  }

  pub fn into_config(self) -> AppConfig {
    self.config
  }

  pub async fn run(self) -> store::Result<()> {
    run_with_config(self.config).await
  }

  pub async fn run_with_shutdown<F>(self, shutdown: F) -> store::Result<()>
  where
    F: Future<Output = ()> + Send,
  {
    run_with_config_and_shutdown(self.config, shutdown).await
  }
}

impl Default for BrokerBuilder {
  fn default() -> Self {
    Self::new()
  }
}
