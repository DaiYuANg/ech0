use clap::Parser;

mod cli;
mod config_overrides;

use cli::Args;
use config_overrides::{apply_cli_overrides, load_app_config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
  let args = Args::parse();
  let mut app = load_app_config(&args)?;
  apply_cli_overrides(&mut app, &args);

  broker::run_with_config(app).await?;
  Ok(())
}
