use std::{io, sync::Arc};

use tokio::{
  net::{TcpListener, TcpStream},
  task::JoinHandle,
};
use tracing::{debug, error, info, warn};
use transport::{read_frame, write_frame};

mod handler;
#[cfg(test)]
mod tests;

pub use handler::BrokerCommandHandler;

#[derive(Debug, Clone)]
pub struct TcpServerConfig {
  pub bind_addr: String,
}

#[derive(Clone)]
pub struct TcpBrokerServer<S> {
  config: TcpServerConfig,
  service: Arc<S>,
}

impl<S> TcpBrokerServer<S> {
  pub fn new(config: TcpServerConfig, service: Arc<S>) -> Self {
    Self { config, service }
  }
}

impl<S> TcpBrokerServer<S>
where
  S: BrokerCommandHandler + Send + Sync + 'static,
{
  pub async fn run(&self) -> io::Result<()> {
    let listener = TcpListener::bind(&self.config.bind_addr).await?;
    info!(bind_addr = %self.config.bind_addr, "tcp broker server listening");

    loop {
      let (stream, remote_addr) = listener.accept().await?;
      let peer = remote_addr.to_string();
      let service = Arc::clone(&self.service);
      info!(peer = %peer, "accepted broker tcp connection");
      spawn_connection(peer, stream, service);
    }
  }
}

fn spawn_connection<S>(peer: String, stream: TcpStream, service: Arc<S>) -> JoinHandle<()>
where
  S: BrokerCommandHandler + Send + Sync + 'static,
{
  tokio::spawn(async move {
    if let Err(err) = handle_connection(peer.clone(), stream, service).await {
      warn!(peer = %peer, error = %err, "broker connection closed with error");
    }
  })
}

async fn handle_connection<S>(
  peer: String,
  mut stream: TcpStream,
  service: Arc<S>,
) -> io::Result<()>
where
  S: BrokerCommandHandler + Send + Sync + 'static,
{
  loop {
    let request = match read_frame(&mut stream).await {
      Ok(frame) => frame,
      Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
        debug!(peer = %peer, "client disconnected");
        return Ok(());
      }
      Err(err) => return Err(err),
    };

    let response = match service.handle_frame(request).await {
      Ok(frame) => frame,
      Err(err) => {
        error!(peer = %peer, error = %err, "failed to handle broker frame");
        handler::error_response("request_error", err.to_string())?
      }
    };

    write_frame(&mut stream, &response).await?;
  }
}
