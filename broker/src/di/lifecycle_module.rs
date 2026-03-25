use std::sync::Arc;
use std::future::Future;

use tracing::info;

use crate::{admin::run as run_admin_server, server::TcpBrokerServer};
use store::{Result, StoreError};

use super::RuntimeWiring;

pub struct LifecycleModule;

impl LifecycleModule {
  pub fn new() -> Self {
    Self
  }

  pub async fn run_with_shutdown<F>(&self, wiring: RuntimeWiring, shutdown: F) -> Result<()>
  where
    F: Future<Output = ()> + Send,
  {
    let tcp_server = TcpBrokerServer::new(wiring.tcp_config, Arc::clone(&wiring.service));
    let tcp_task = tokio::spawn(async move { tcp_server.run().await });

    let admin_task = wiring
      .admin_config
      .map(|config| tokio::spawn(async move { run_admin_server(config).await }));
    tokio::pin!(shutdown);

    if let Some(admin_task) = admin_task {
      tokio::select! {
        result = tcp_task => {
          join_result_to_store_result("tcp broker server", result)
        }
        result = admin_task => {
          join_result_to_store_result("admin http server", result)
        }
        _ = &mut shutdown => {
          info!("shutdown signal received");
          Ok(())
        }
      }
    } else {
      tokio::select! {
        result = tcp_task => {
          join_result_to_store_result("tcp broker server", result)
        }
        _ = &mut shutdown => {
          info!("shutdown signal received");
          Ok(())
        }
      }
    }
  }
}

fn join_result_to_store_result(
  component: &str,
  result: std::result::Result<std::io::Result<()>, tokio::task::JoinError>,
) -> Result<()> {
  match result {
    Ok(Ok(())) => Ok(()),
    Ok(Err(err)) => Err(StoreError::Io(std::io::Error::new(
      err.kind(),
      format!("{component} exited with io error: {err}"),
    ))),
    Err(err) => Err(StoreError::Codec(format!(
      "{component} task join error: {err}"
    ))),
  }
}
