use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum GroupAssignmentStrategyArg {
  #[value(name = "round_robin")]
  RoundRobin,
  #[value(name = "range")]
  Range,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogFormatArg {
  #[value(name = "text")]
  Text,
  #[value(name = "compact")]
  Compact,
  #[value(name = "json")]
  Json,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogRotationArg {
  #[value(name = "minutely")]
  Minutely,
  #[value(name = "hourly")]
  Hourly,
  #[value(name = "daily")]
  Daily,
  #[value(name = "weekly")]
  Weekly,
  #[value(name = "never")]
  Never,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum RaftReadPolicyArg {
  #[value(name = "local")]
  Local,
  #[value(name = "leader")]
  Leader,
  #[value(name = "linearizable")]
  Linearizable,
}

#[derive(Debug, Parser)]
#[command(name = "broker")]
#[command(about = "ech0 broker binary entrypoint")]
pub struct Args {
  /// Override config file path(s). Can be provided multiple times.
  #[arg(long = "config", value_name = "PATH")]
  pub config: Vec<PathBuf>,

  /// Broker TCP bind address, e.g. 127.0.0.1:9090.
  #[arg(long)]
  pub bind_addr: Option<String>,

  /// Broker data directory root path.
  #[arg(long)]
  pub data_dir: Option<String>,

  /// Max protocol frame body bytes.
  #[arg(long)]
  pub max_frame_body_bytes: Option<usize>,

  /// Max single payload bytes for produce/send_direct.
  #[arg(long)]
  pub max_payload_bytes: Option<usize>,

  /// Max total payload bytes for one produce_batch request.
  #[arg(long)]
  pub max_batch_payload_bytes: Option<usize>,

  /// Max records allowed per fetch/fetch_inbox/fetch_batch item.
  #[arg(long)]
  pub max_fetch_records: Option<usize>,

  /// Max broker-side wait time for fetch/fetch_batch long polling.
  #[arg(long)]
  pub max_fetch_wait_ms: Option<u64>,

  /// Consumer group assignment strategy.
  #[arg(long, value_enum)]
  pub group_assignment_strategy: Option<GroupAssignmentStrategyArg>,

  /// Enable/disable sticky assignments, pass true/false.
  #[arg(long, value_parser = clap::value_parser!(bool))]
  pub group_sticky_assignments: Option<bool>,

  /// Enable/disable background retry worker, pass true/false.
  #[arg(long, value_parser = clap::value_parser!(bool))]
  pub retry_worker_enabled: Option<bool>,

  /// Retry worker polling interval in seconds.
  #[arg(long)]
  pub retry_worker_interval_secs: Option<u64>,

  /// Max records processed per retry partition in one pass.
  #[arg(long)]
  pub retry_worker_max_records: Option<usize>,

  /// Consumer prefix used by retry worker offset commits.
  #[arg(long)]
  pub retry_worker_consumer_prefix: Option<String>,

  /// Enable/disable delay scheduler background worker, pass true/false.
  #[arg(long, value_parser = clap::value_parser!(bool))]
  pub delay_scheduler_enabled: Option<bool>,

  /// Delay scheduler polling interval in seconds.
  #[arg(long)]
  pub delay_scheduler_interval_secs: Option<u64>,

  /// Max delay records processed per partition in one pass.
  #[arg(long)]
  pub delay_scheduler_max_records: Option<usize>,

  /// Consumer prefix used by delay scheduler offset commits.
  #[arg(long)]
  pub delay_scheduler_consumer_prefix: Option<String>,

  /// Admin HTTP bind address, e.g. 127.0.0.1:9091.
  #[arg(long)]
  pub admin_bind_addr: Option<String>,

  /// Enable/disable admin HTTP server, pass true/false.
  #[arg(long, value_parser = clap::value_parser!(bool))]
  pub admin_enabled: Option<bool>,

  /// Enable/disable raft mode, pass true/false.
  #[arg(long, value_parser = clap::value_parser!(bool))]
  pub raft_enabled: Option<bool>,

  /// Raft bind address, e.g. 127.0.0.1:3210.
  #[arg(long)]
  pub raft_bind_addr: Option<String>,

  /// Raft read policy.
  #[arg(long, value_enum)]
  pub raft_read_policy: Option<RaftReadPolicyArg>,

  /// Logging level directive.
  #[arg(long)]
  pub logging_level: Option<String>,

  /// Logging format.
  #[arg(long, value_enum)]
  pub logging_format: Option<LogFormatArg>,

  /// Enable/disable stdout logging, pass true/false.
  #[arg(long, value_parser = clap::value_parser!(bool))]
  pub logging_enable_stdout: Option<bool>,

  /// Enable/disable file logging, pass true/false.
  #[arg(long, value_parser = clap::value_parser!(bool))]
  pub logging_enable_file: Option<bool>,

  /// Logging output directory under data dir unless absolute.
  #[arg(long)]
  pub logging_directory: Option<String>,

  /// Logging file prefix.
  #[arg(long)]
  pub logging_file_prefix: Option<String>,

  /// Logging rotation policy.
  #[arg(long, value_enum)]
  pub logging_rotation: Option<LogRotationArg>,

  /// Maximum rotated log files to keep.
  #[arg(long)]
  pub logging_max_files: Option<usize>,

  /// Enable/disable ANSI output, pass true/false.
  #[arg(long, value_parser = clap::value_parser!(bool))]
  pub logging_ansi: Option<bool>,
}
