use std::io;

use protocol::{
  AckDirectRequest, AckDirectResponse, CMD_ACK_DIRECT_REQUEST, CMD_ACK_DIRECT_RESPONSE,
  CMD_COMMIT_OFFSET_REQUEST, CMD_COMMIT_OFFSET_RESPONSE, CMD_CREATE_TOPIC_REQUEST,
  CMD_CREATE_TOPIC_RESPONSE, CMD_ERROR_RESPONSE, CMD_FETCH_INBOX_REQUEST, CMD_FETCH_INBOX_RESPONSE,
  CMD_FETCH_REQUEST, CMD_FETCH_RESPONSE, CMD_HANDSHAKE_REQUEST, CMD_HANDSHAKE_RESPONSE,
  CMD_LIST_TOPICS_REQUEST, CMD_LIST_TOPICS_RESPONSE, CMD_PING_REQUEST, CMD_PING_RESPONSE,
  CMD_PRODUCE_REQUEST, CMD_PRODUCE_RESPONSE, CMD_SEND_DIRECT_REQUEST, CMD_SEND_DIRECT_RESPONSE,
  CommitOffsetRequest, CommitOffsetResponse, CreateTopicRequest, CreateTopicResponse,
  DirectMessageRecord, ErrorResponse, FetchInboxRequest, FetchInboxResponse, FetchRecord,
  FetchRequest, FetchResponse, HandshakeRequest, HandshakeResponse, ListTopicsResponse,
  PingRequest, PingResponse, ProduceRequest, ProduceResponse, SendDirectRequest,
  SendDirectResponse, TopicMetadata, VERSION_1, decode_json, encode_json,
};
use transport::Frame;

use crate::service::BrokerService;

pub trait BrokerCommandHandler {
  fn handle_frame(
    &self,
    frame: Frame,
  ) -> impl std::future::Future<Output = io::Result<Frame>> + Send;
}

impl<L, M> BrokerCommandHandler for BrokerService<L, M>
where
  L: store::MessageLogStore + store::MutablePartitionLogStore + Send + Sync + 'static,
  M: store::OffsetStore
    + store::TopicCatalogStore
    + store::LocalPartitionStateStore
    + store::ConsensusLogStore
    + store::ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
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
          server_id: format!(
            "{}-node-{}",
            self.identity().cluster_name,
            self.identity().node_id
          ),
          protocol_version: VERSION_1,
        };
        tracing::debug!(client_id = %request.client_id, "handled handshake request");
        ok_response(CMD_HANDSHAKE_RESPONSE, &response)
      }
      CMD_PING_REQUEST => {
        let request: PingRequest = decode_request(&frame)?;
        ok_response(
          CMD_PING_RESPONSE,
          &PingResponse {
            nonce: request.nonce,
          },
        )
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
      CMD_SEND_DIRECT_REQUEST => {
        let request: SendDirectRequest = decode_request(&frame)?;
        let sent = self
          .send_direct(
            &request.sender,
            &request.recipient,
            request.conversation_id,
            request.payload,
          )
          .map_err(to_io_error)?;
        ok_response(
          CMD_SEND_DIRECT_RESPONSE,
          &SendDirectResponse {
            message_id: sent.message_id,
            conversation_id: sent.conversation_id,
            offset: sent.offset,
            next_offset: sent.next_offset,
          },
        )
      }
      CMD_FETCH_INBOX_REQUEST => {
        let request: FetchInboxRequest = decode_request(&frame)?;
        let fetched = self
          .fetch_inbox(&request.recipient, request.max_records)
          .map_err(to_io_error)?;
        ok_response(
          CMD_FETCH_INBOX_RESPONSE,
          &FetchInboxResponse {
            recipient: fetched.recipient,
            records: fetched
              .records
              .into_iter()
              .map(|record| DirectMessageRecord {
                offset: record.offset,
                message_id: record.message.message_id,
                conversation_id: record.message.conversation_id,
                sender: record.message.sender,
                recipient: record.message.recipient,
                timestamp_ms: record.message.timestamp_ms,
                payload: record.message.payload,
              })
              .collect(),
            next_offset: fetched.next_offset,
            high_watermark: fetched.high_watermark,
          },
        )
      }
      CMD_ACK_DIRECT_REQUEST => {
        let request: AckDirectRequest = decode_request(&frame)?;
        self
          .ack_direct(&request.recipient, request.next_offset)
          .map_err(to_io_error)?;
        ok_response(
          CMD_ACK_DIRECT_RESPONSE,
          &AckDirectResponse {
            recipient: request.recipient,
            next_offset: request.next_offset,
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
  decode_json(&frame.body).map_err(|err| {
    io::Error::new(
      io::ErrorKind::InvalidData,
      format!("invalid json body: {err}"),
    )
  })
}

fn ok_response<T>(command: u16, response: &T) -> io::Result<Frame>
where
  T: serde::Serialize,
{
  let body = encode_json(response).map_err(|err| {
    io::Error::new(
      io::ErrorKind::InvalidData,
      format!("json encode failed: {err}"),
    )
  })?;
  Frame::new(VERSION_1, command, body)
}

pub(super) fn error_response(
  code: impl Into<String>,
  message: impl Into<String>,
) -> io::Result<Frame> {
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
