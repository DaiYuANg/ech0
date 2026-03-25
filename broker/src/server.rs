use std::{io, sync::Arc};

use tokio::{
  net::{TcpListener, TcpStream},
  task::JoinHandle,
};
use tracing::{debug, error, info, warn};
use transport::{read_frame, write_frame};

use crate::metrics;

mod handler;
#[cfg(test)]
mod tests;

pub use handler::{BrokerCommandHandler, HandlerLimits};

pub fn configure_handler_limits(limits: HandlerLimits) {
  handler::set_handler_limits(limits);
}

#[derive(Debug, Clone)]
pub struct TcpServerConfig {
  pub bind_addr: String,
  pub max_frame_body_bytes: usize,
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
      metrics::record_tcp_connection();
      spawn_connection(peer, stream, service, self.config.max_frame_body_bytes);
    }
  }
}

fn spawn_connection<S>(
  peer: String,
  stream: TcpStream,
  service: Arc<S>,
  max_frame_body_bytes: usize,
) -> JoinHandle<()>
where
  S: BrokerCommandHandler + Send + Sync + 'static,
{
  tokio::spawn(async move {
    if let Err(err) = handle_connection(peer.clone(), stream, service, max_frame_body_bytes).await {
      warn!(peer = %peer, error = %err, "broker connection closed with error");
    }
  })
}

async fn handle_connection<S>(
  peer: String,
  mut stream: TcpStream,
  service: Arc<S>,
  max_frame_body_bytes: usize,
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

    if request.body.len() > max_frame_body_bytes {
      let response = handler::error_response(
        "frame_too_large",
        format!(
          "frame body size {} exceeds limit {}",
          request.body.len(),
          max_frame_body_bytes
        ),
      )?;
      write_frame(&mut stream, &response).await?;
      continue;
    }

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
