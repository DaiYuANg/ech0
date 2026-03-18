use std::{fs, path::Path};
use tracing_appender::non_blocking;

use tracing_appender::{
  non_blocking::WorkerGuard,
  rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{
  EnvFilter, Registry, fmt,
  layer::{Layer, SubscriberExt},
  util::SubscriberInitExt,
};

use crate::config::{LogFormat, LogRotation, LoggingConfig};

type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

pub fn init_logging(
  config: &LoggingConfig,
) -> Result<Vec<WorkerGuard>, Box<dyn std::error::Error>> {
  let mut guards = Vec::new();
  let mut layers: Vec<BoxedLayer> = Vec::new();

  let filter = EnvFilter::try_new(config.level.clone()).or_else(|_| EnvFilter::try_new("info"))?;
  layers.push(Box::new(filter));

  if config.enable_stdout {
    let stdout_layer = match config.format {
      LogFormat::Json => fmt::layer().with_ansi(config.ansi).json().boxed(),
      LogFormat::Compact => fmt::layer().with_ansi(config.ansi).compact().boxed(),
      LogFormat::Text => fmt::layer().with_ansi(config.ansi).boxed(),
    };
    layers.push(stdout_layer);
  }

  if config.enable_file {
    fs::create_dir_all(&config.directory)?;

    let rotation = match config.rotation {
      LogRotation::Minutely => Rotation::MINUTELY,
      LogRotation::Hourly => Rotation::HOURLY,
      LogRotation::Daily => Rotation::DAILY,
      LogRotation::Never => Rotation::NEVER,
      LogRotation::Weekly => Rotation::DAILY,
    };

    let appender: RollingFileAppender = tracing_appender::rolling::Builder::new()
      .rotation(rotation)
      .filename_prefix(&config.file_prefix)
      .build(Path::new(&config.directory))?;

    let (non_blocking_writer, guard) = non_blocking(appender);
    guards.push(guard);

    let file_layer = match config.format {
      LogFormat::Json => fmt::layer()
        .with_ansi(false)
        .with_writer(non_blocking_writer)
        .json()
        .boxed(),
      LogFormat::Compact => fmt::layer()
        .with_ansi(false)
        .with_writer(non_blocking_writer)
        .compact()
        .boxed(),
      LogFormat::Text => fmt::layer()
        .with_ansi(false)
        .with_writer(non_blocking_writer)
        .boxed(),
    };
    layers.push(file_layer);
  }

  tracing_subscriber::registry().with(layers).try_init()?;

  Ok(guards)
}
