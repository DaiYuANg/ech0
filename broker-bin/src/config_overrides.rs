use broker::AppConfig;

use crate::cli::{Args, GroupAssignmentStrategyArg, LogFormatArg, LogRotationArg, RaftReadPolicyArg};

pub fn load_app_config(args: &Args) -> Result<AppConfig, broker::ConfigError> {
  if args.config.is_empty() {
    AppConfig::load()
  } else {
    AppConfig::load_from_paths(args.config.clone())
  }
}

pub fn apply_cli_overrides(app: &mut AppConfig, args: &Args) {
  if let Some(bind_addr) = args.bind_addr.clone() {
    app.broker.bind_addr = bind_addr;
  }
  if let Some(data_dir) = args.data_dir.clone() {
    app.broker.data_dir = data_dir;
  }
  if let Some(max_frame_body_bytes) = args.max_frame_body_bytes {
    app.broker.max_frame_body_bytes = max_frame_body_bytes;
  }
  if let Some(max_payload_bytes) = args.max_payload_bytes {
    app.broker.max_payload_bytes = max_payload_bytes;
  }
  if let Some(max_batch_payload_bytes) = args.max_batch_payload_bytes {
    app.broker.max_batch_payload_bytes = max_batch_payload_bytes;
  }
  if let Some(max_fetch_records) = args.max_fetch_records {
    app.broker.max_fetch_records = max_fetch_records;
  }
  if let Some(strategy) = args.group_assignment_strategy {
    app.broker.group_assignment_strategy = match strategy {
      GroupAssignmentStrategyArg::RoundRobin => broker::config::GroupAssignmentStrategyConfig::RoundRobin,
      GroupAssignmentStrategyArg::Range => broker::config::GroupAssignmentStrategyConfig::Range,
    };
  }
  if let Some(sticky) = args.group_sticky_assignments {
    app.broker.group_sticky_assignments = sticky;
  }

  if let Some(admin_bind_addr) = args.admin_bind_addr.clone() {
    app.admin.bind_addr = admin_bind_addr;
  }
  if let Some(admin_enabled) = args.admin_enabled {
    app.admin.enabled = admin_enabled;
  }

  if let Some(raft_enabled) = args.raft_enabled {
    app.raft.enabled = raft_enabled;
  }
  if let Some(raft_bind_addr) = args.raft_bind_addr.clone() {
    app.raft.bind_addr = raft_bind_addr;
  }
  if let Some(read_policy) = args.raft_read_policy {
    app.raft.read_policy = match read_policy {
      RaftReadPolicyArg::Local => broker::config::RaftReadPolicy::Local,
      RaftReadPolicyArg::Leader => broker::config::RaftReadPolicy::Leader,
      RaftReadPolicyArg::Linearizable => broker::config::RaftReadPolicy::Linearizable,
    };
  }

  if let Some(logging_level) = args.logging_level.clone() {
    app.logging.level = logging_level;
  }
  if let Some(logging_format) = args.logging_format {
    app.logging.format = match logging_format {
      LogFormatArg::Text => broker::config::LogFormat::Text,
      LogFormatArg::Compact => broker::config::LogFormat::Compact,
      LogFormatArg::Json => broker::config::LogFormat::Json,
    };
  }
  if let Some(logging_enable_stdout) = args.logging_enable_stdout {
    app.logging.enable_stdout = logging_enable_stdout;
  }
  if let Some(logging_enable_file) = args.logging_enable_file {
    app.logging.enable_file = logging_enable_file;
  }
  if let Some(logging_directory) = args.logging_directory.clone() {
    app.logging.directory = logging_directory;
  }
  if let Some(logging_file_prefix) = args.logging_file_prefix.clone() {
    app.logging.file_prefix = logging_file_prefix;
  }
  if let Some(logging_rotation) = args.logging_rotation {
    app.logging.rotation = match logging_rotation {
      LogRotationArg::Minutely => broker::config::LogRotation::Minutely,
      LogRotationArg::Hourly => broker::config::LogRotation::Hourly,
      LogRotationArg::Daily => broker::config::LogRotation::Daily,
      LogRotationArg::Weekly => broker::config::LogRotation::Weekly,
      LogRotationArg::Never => broker::config::LogRotation::Never,
    };
  }
  if let Some(logging_max_files) = args.logging_max_files {
    app.logging.max_files = logging_max_files;
  }
  if let Some(logging_ansi) = args.logging_ansi {
    app.logging.ansi = logging_ansi;
  }
}

#[cfg(test)]
mod tests {
  use clap::Parser;

  use super::apply_cli_overrides;
  use crate::cli::Args;

  #[test]
  fn apply_cli_overrides_updates_all_supported_fields() {
    let args = Args::parse_from([
      "broker",
      "--bind-addr",
      "127.0.0.1:19090",
      "--data-dir",
      "/tmp/ech0-data",
      "--max-frame-body-bytes",
      "8388608",
      "--max-payload-bytes",
      "2097152",
      "--max-batch-payload-bytes",
      "16777216",
      "--max-fetch-records",
      "2000",
      "--group-assignment-strategy",
      "range",
      "--group-sticky-assignments",
      "false",
      "--admin-bind-addr",
      "127.0.0.1:19091",
      "--admin-enabled",
      "false",
      "--raft-enabled",
      "false",
      "--raft-bind-addr",
      "127.0.0.1:33210",
      "--raft-read-policy",
      "linearizable",
      "--logging-level",
      "info,broker=debug",
      "--logging-format",
      "json",
      "--logging-enable-stdout",
      "false",
      "--logging-enable-file",
      "false",
      "--logging-directory",
      "runtime-logs",
      "--logging-file-prefix",
      "broker-test",
      "--logging-rotation",
      "hourly",
      "--logging-max-files",
      "7",
      "--logging-ansi",
      "false",
    ]);

    let mut app = broker::AppConfig::default();
    apply_cli_overrides(&mut app, &args);

    assert_eq!(app.broker.bind_addr, "127.0.0.1:19090");
    assert_eq!(app.broker.data_dir, "/tmp/ech0-data");
    assert_eq!(app.broker.max_frame_body_bytes, 8_388_608);
    assert_eq!(app.broker.max_payload_bytes, 2_097_152);
    assert_eq!(app.broker.max_batch_payload_bytes, 16_777_216);
    assert_eq!(app.broker.max_fetch_records, 2_000);
    assert_eq!(
      app.broker.group_assignment_strategy,
      broker::config::GroupAssignmentStrategyConfig::Range
    );
    assert!(!app.broker.group_sticky_assignments);

    assert_eq!(app.admin.bind_addr, "127.0.0.1:19091");
    assert!(!app.admin.enabled);

    assert!(!app.raft.enabled);
    assert_eq!(app.raft.bind_addr, "127.0.0.1:33210");
    assert_eq!(app.raft.read_policy, broker::config::RaftReadPolicy::Linearizable);

    assert_eq!(app.logging.level, "info,broker=debug");
    assert!(matches!(app.logging.format, broker::config::LogFormat::Json));
    assert!(!app.logging.enable_stdout);
    assert!(!app.logging.enable_file);
    assert_eq!(app.logging.directory, "runtime-logs");
    assert_eq!(app.logging.file_prefix, "broker-test");
    assert!(matches!(
      app.logging.rotation,
      broker::config::LogRotation::Hourly
    ));
    assert_eq!(app.logging.max_files, 7);
    assert!(!app.logging.ansi);
  }
}
