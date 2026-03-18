use std::{io, sync::Arc};

use protocol::{
  CMD_COMMIT_OFFSET_REQUEST, CMD_COMMIT_OFFSET_RESPONSE, CMD_CREATE_TOPIC_REQUEST,
  CMD_CREATE_TOPIC_RESPONSE, CMD_ERROR_RESPONSE, CMD_FETCH_REQUEST, CMD_FETCH_RESPONSE,
  CMD_HANDSHAKE_REQUEST, CMD_HANDSHAKE_RESPONSE, CMD_LIST_TOPICS_REQUEST,
  CMD_LIST_TOPICS_RESPONSE, CMD_PING_REQUEST, CMD_PING_RESPONSE, CMD_PRODUCE_REQUEST,
  CMD_PRODUCE_RESPONSE, CommitOffsetRequest, CommitOffsetResponse, CreateTopicRequest,
  CreateTopicResponse, ErrorResponse, FetchRequest, FetchResponse, FetchRecord,
  HandshakeRequest, HandshakeResponse, ListTopicsResponse, PingRequest, PingResponse,
  ProduceRequest, ProduceResponse, TopicMetadata, VERSION_1, decode_json, encode_json,
};
use tokio::{
  net::{TcpListener, TcpStream},
  task::JoinHandle,
};
use tracing::{debug, error, info, warn};
use transport::{Frame, read_frame, write_frame};

use crate::service::BrokerService;

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

async fn handle_connection<S>(peer: String, mut stream: TcpStream, service: Arc<S>) -> io::Result<()>
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
        error_response("request_error", err.to_string())?
      }
    };

    write_frame(&mut stream, &response).await?;
  }
}

pub trait BrokerCommandHandler {
  fn handle_frame(&self, frame: Frame) -> impl std::future::Future<Output = io::Result<Frame>> + Send;
}

impl<L, M> BrokerCommandHandler for BrokerService<L, M>
where
  L: store::MessageLogStore + Send + Sync,
  M: store::OffsetStore + store::TopicCatalogStore + Send + Sync,
{
  async fn handle_frame(&self, frame: Frame) -> io::Result<Frame> {
    if frame.header.version != VERSION_1 {
      return error_response(
        "unsupported_version",
        format!(
          "unsupported protocol version {}, expected {}",
          frame.header.version, VERSION_1
        ),
      );
    }

    match frame.header.command {
      CMD_HANDSHAKE_REQUEST => {
        let request: HandshakeRequest = decode_request(&frame)?;
        let response = HandshakeResponse {
          server_id: format!("{}-node-{}", self.identity().cluster_name, self.identity().node_id),
          protocol_version: VERSION_1,
        };
        debug!(client_id = %request.client_id, "handled handshake request");
        ok_response(CMD_HANDSHAKE_RESPONSE, &response)
      }
      CMD_PING_REQUEST => {
        let request: PingRequest = decode_request(&frame)?;
        ok_response(CMD_PING_RESPONSE, &PingResponse { nonce: request.nonce })
      }
      CMD_CREATE_TOPIC_REQUEST => {
        let request: CreateTopicRequest = decode_request(&frame)?;
        let topic = self
          .create_topic(request.topic, request.partitions)
          .map_err(to_io_error)?;
        ok_response(
          CMD_CREATE_TOPIC_RESPONSE,
          &CreateTopicResponse {
            topic: topic.name,
            partitions: topic.partitions,
          },
        )
      }
      CMD_PRODUCE_REQUEST => {
        let request: ProduceRequest = decode_request(&frame)?;
        let (offset, next_offset) = self
          .publish(request.topic, request.partition, request.payload)
          .map_err(to_io_error)?;
        ok_response(
          CMD_PRODUCE_RESPONSE,
          &ProduceResponse {
            offset,
            next_offset,
          },
        )
      }
      CMD_FETCH_REQUEST => {
        let request: FetchRequest = decode_request(&frame)?;
        let fetched = self
          .fetch(
            &request.consumer,
            request.topic,
            request.partition,
            request.offset,
            request.max_records,
          )
          .map_err(to_io_error)?;
        ok_response(
          CMD_FETCH_RESPONSE,
          &FetchResponse {
            topic: fetched.topic,
            partition: fetched.partition,
            records: fetched
              .records
              .into_iter()
              .map(|record| FetchRecord {
                offset: record.offset,
                timestamp_ms: record.timestamp_ms,
                payload: record.payload,
              })
              .collect(),
            next_offset: fetched.next_offset,
            high_watermark: fetched.high_watermark,
          },
        )
      }
      CMD_COMMIT_OFFSET_REQUEST => {
        let request: CommitOffsetRequest = decode_request(&frame)?;
        self
          .commit_offset(
            &request.consumer,
            request.topic.clone(),
            request.partition,
            request.next_offset,
          )
          .map_err(to_io_error)?;
        ok_response(
          CMD_COMMIT_OFFSET_RESPONSE,
          &CommitOffsetResponse {
            consumer: request.consumer,
            topic: request.topic,
            partition: request.partition,
            next_offset: request.next_offset,
          },
        )
      }
      CMD_LIST_TOPICS_REQUEST => {
        let topics = self.list_topics().map_err(to_io_error)?;
        ok_response(
          CMD_LIST_TOPICS_RESPONSE,
          &ListTopicsResponse {
            topics: topics
              .into_iter()
              .map(|topic| TopicMetadata {
                topic: topic.name,
                partitions: topic.partitions,
              })
              .collect(),
          },
        )
      }
      command => error_response("unknown_command", format!("unknown command id {command}")),
    }
  }
}

fn decode_request<T>(frame: &Frame) -> io::Result<T>
where
  T: for<'de> serde::Deserialize<'de>,
{
  decode_json(&frame.body)
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("invalid json body: {err}")))
}

fn ok_response<T>(command: u16, response: &T) -> io::Result<Frame>
where
  T: serde::Serialize,
{
  let body = encode_json(response)
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("json encode failed: {err}")))?;
  Frame::new(VERSION_1, command, body)
}

fn error_response(code: impl Into<String>, message: impl Into<String>) -> io::Result<Frame> {
  ok_response(
    CMD_ERROR_RESPONSE,
    &ErrorResponse {
      code: code.into(),
      message: message.into(),
    },
  )
}

fn to_io_error(err: store::StoreError) -> io::Error {
  io::Error::other(err.to_string())
}

#[cfg(test)]
mod tests {
  use protocol::{CMD_FETCH_REQUEST, CMD_FETCH_RESPONSE, FetchRequest, FetchResponse};
  use store::InMemoryStore;
  use transport::Frame;

  use crate::service::{BrokerIdentity, BrokerService};

  use super::BrokerCommandHandler;

  #[tokio::test]
  async fn fetch_frame_roundtrip_works() {
    let log = InMemoryStore::new();
    let meta = InMemoryStore::new();
    let service = BrokerService::new(
      BrokerIdentity {
        node_id: 1,
        cluster_name: "test".to_owned(),
      },
      log,
      meta,
    );
    service.create_topic("orders", 1).unwrap();
    service.publish("orders", 0, b"hello".to_vec()).unwrap();

    let request = Frame::new(
      protocol::VERSION_1,
      CMD_FETCH_REQUEST,
      protocol::encode_json(&FetchRequest {
        consumer: "c1".to_owned(),
        topic: "orders".to_owned(),
        partition: 0,
        offset: Some(0),
        max_records: 10,
      })
      .unwrap(),
    )
    .unwrap();

    let response = service.handle_frame(request).await.unwrap();
    assert_eq!(response.header.command, CMD_FETCH_RESPONSE);

    let response: FetchResponse = protocol::decode_json(&response.body).unwrap();
    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].payload, b"hello".to_vec());
  }
}
