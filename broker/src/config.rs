use std::path::{Path, PathBuf};

use figment2::{
  Figment,
  providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
  #[error("figment error: {0}")]
  Figment(#[from] figment2::Error),
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ConfigError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
  pub broker: BrokerConfig,
  pub admin: AdminConfig,
  pub storage: StorageConfig,
  pub logging: LoggingConfig,
  pub raft: RaftConfig,
}

impl Default for AppConfig {
  fn default() -> Self {
    Self {
      broker: BrokerConfig::default(),
      admin: AdminConfig::default(),
      storage: StorageConfig::default(),
      logging: LoggingConfig::default(),
      raft: RaftConfig::default(),
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminConfig {
  pub enabled: bool,
  pub bind_addr: String,
}

impl Default for AdminConfig {
  fn default() -> Self {
    Self {
      enabled: true,
      bind_addr: "127.0.0.1:9091".to_owned(),
    }
  }
}

impl AppConfig {
  pub fn load() -> Result<Self> {
    Self::load_from_paths(default_config_candidates())
  }

  pub fn load_from_paths(paths: impl IntoIterator<Item = PathBuf>) -> Result<Self> {
    let _ = dotenvy::from_filename(".env");

    let mut figment = Figment::from(Serialized::defaults(Self::default()));
    for path in paths {
      if path.exists() {
        figment = figment.merge(Toml::file(path));
      }
    }

    figment = figment.merge(Env::prefixed("ECH0_").split("__"));

    Ok(figment.extract()?)
  }

  pub fn data_dir(&self) -> &Path {
    Path::new(&self.broker.data_dir)
  }

  pub fn segments_dir(&self) -> PathBuf {
    resolve_under_data_dir(self.data_dir(), &self.storage.segments_dir)
  }

  pub fn metadata_path(&self) -> PathBuf {
    resolve_under_data_dir(self.data_dir(), &self.storage.metadata_path)
  }

  pub fn log_dir(&self) -> PathBuf {
    resolve_under_data_dir(self.data_dir(), &self.logging.directory)
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
  pub node_id: u64,
  pub cluster_name: String,
  pub data_dir: String,
  pub bind_addr: String,
  pub max_frame_body_bytes: usize,
  pub max_payload_bytes: usize,
  pub max_batch_payload_bytes: usize,
  pub max_fetch_records: usize,
  pub group_assignment_strategy: GroupAssignmentStrategyConfig,
  pub group_sticky_assignments: bool,
  pub retry_worker_enabled: bool,
  pub retry_worker_interval_secs: u64,
  pub retry_worker_max_records: usize,
  pub retry_worker_consumer_prefix: String,
  pub delay_scheduler_enabled: bool,
  pub delay_scheduler_interval_secs: u64,
  pub delay_scheduler_max_records: usize,
  pub delay_scheduler_consumer_prefix: String,
}

impl Default for BrokerConfig {
  fn default() -> Self {
    Self {
      node_id: 1,
      cluster_name: "ech0-dev".to_owned(),
      data_dir: "./data".to_owned(),
      bind_addr: "127.0.0.1:9090".to_owned(),
      max_frame_body_bytes: 4 * 1024 * 1024,
      max_payload_bytes: 1024 * 1024,
      max_batch_payload_bytes: 8 * 1024 * 1024,
      max_fetch_records: 1_000,
      group_assignment_strategy: GroupAssignmentStrategyConfig::RoundRobin,
      group_sticky_assignments: true,
      retry_worker_enabled: true,
      retry_worker_interval_secs: 5,
      retry_worker_max_records: 256,
      retry_worker_consumer_prefix: "__retry_worker".to_owned(),
      delay_scheduler_enabled: true,
      delay_scheduler_interval_secs: 1,
      delay_scheduler_max_records: 256,
      delay_scheduler_consumer_prefix: "__delay_scheduler".to_owned(),
    }
  }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GroupAssignmentStrategyConfig {
  RoundRobin,
  Range,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
  pub segments_dir: String,
  pub metadata_path: String,
  pub retention_cleanup_enabled: bool,
  pub retention_cleanup_interval_secs: u64,
}

impl Default for StorageConfig {
  fn default() -> Self {
    Self {
      segments_dir: "segments".to_owned(),
      metadata_path: "meta/metadata.redb".to_owned(),
      retention_cleanup_enabled: true,
      retention_cleanup_interval_secs: 30,
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
  pub level: String,
  pub format: LogFormat,
  pub enable_stdout: bool,
  pub enable_file: bool,
  pub directory: String,
  pub file_prefix: String,
  pub rotation: LogRotation,
  pub max_files: usize,
  pub ansi: bool,
}

impl Default for LoggingConfig {
  fn default() -> Self {
    Self {
      level: "info,broker=debug,store=debug".to_owned(),
      format: LogFormat::Text,
      enable_stdout: true,
      enable_file: true,
      directory: "logs".to_owned(),
      file_prefix: "ech0".to_owned(),
      rotation: LogRotation::Daily,
      max_files: 14,
      ansi: true,
    }
  }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
  Text,
  Compact,
  Json,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogRotation {
  Minutely,
  Hourly,
  Daily,
  Weekly,
  Never,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
  pub enabled: bool,
  pub bind_addr: String,
  pub read_policy: RaftReadPolicy,
  pub metrics_interval_ms: u64,
  pub heartbeat_interval_ms: u64,
  pub election_timeout_min_ms: u64,
  pub election_timeout_max_ms: u64,
  pub snapshot_max_chunk_size: u64,
  pub cluster: Vec<RaftPeerConfig>,
}

impl Default for RaftConfig {
  fn default() -> Self {
    Self {
      enabled: true,
      bind_addr: "127.0.0.1:3210".to_owned(),
      read_policy: RaftReadPolicy::Local,
      metrics_interval_ms: 1_000,
      heartbeat_interval_ms: 150,
      election_timeout_min_ms: 300,
      election_timeout_max_ms: 600,
      snapshot_max_chunk_size: 3 * 1024 * 1024,
      cluster: vec![RaftPeerConfig::default()],
    }
  }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RaftReadPolicy {
  Local,
  Leader,
  Linearizable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftPeerConfig {
  pub node_id: u64,
  pub addr: String,
}

impl Default for RaftPeerConfig {
  fn default() -> Self {
    Self {
      node_id: 1,
      addr: "127.0.0.1:3210".to_owned(),
    }
  }
}

fn resolve_under_data_dir(data_dir: &Path, value: &str) -> PathBuf {
  let path = PathBuf::from(value);
  if path.is_absolute() {
    path
  } else {
    data_dir.join(path)
  }
}

fn default_config_candidates() -> Vec<PathBuf> {
  vec![
    PathBuf::from("./ech0.toml"),
    PathBuf::from("./config/ech0.toml"),
    PathBuf::from("./config/ech0.local.toml"),
  ]
}
