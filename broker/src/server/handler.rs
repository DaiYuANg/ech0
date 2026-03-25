use std::io;
use std::sync::{OnceLock, RwLock};

use protocol::{
  AckDirectRequest, AckDirectResponse, CMD_ACK_DIRECT_REQUEST, CMD_ACK_DIRECT_RESPONSE,
  CMD_COMMIT_OFFSET_REQUEST, CMD_COMMIT_OFFSET_RESPONSE, CMD_CREATE_TOPIC_REQUEST,
  CMD_CREATE_TOPIC_RESPONSE, CMD_ERROR_RESPONSE, CMD_FETCH_INBOX_REQUEST, CMD_FETCH_INBOX_RESPONSE,
  CMD_FETCH_BATCH_REQUEST, CMD_FETCH_BATCH_RESPONSE, CMD_FETCH_REQUEST, CMD_FETCH_RESPONSE,
  CMD_GET_CONSUMER_GROUP_ASSIGNMENT_REQUEST, CMD_GET_CONSUMER_GROUP_ASSIGNMENT_RESPONSE,
  CMD_HANDSHAKE_REQUEST, CMD_HANDSHAKE_RESPONSE, CMD_HEARTBEAT_CONSUMER_GROUP_REQUEST,
  CMD_HEARTBEAT_CONSUMER_GROUP_RESPONSE, CMD_JOIN_CONSUMER_GROUP_REQUEST,
  CMD_JOIN_CONSUMER_GROUP_RESPONSE, CMD_LIST_TOPICS_REQUEST, CMD_LIST_TOPICS_RESPONSE,
  CMD_PING_REQUEST, CMD_PING_RESPONSE, CMD_PRODUCE_BATCH_REQUEST, CMD_PRODUCE_BATCH_RESPONSE,
  CMD_PRODUCE_REQUEST, CMD_PRODUCE_RESPONSE, CMD_REBALANCE_CONSUMER_GROUP_REQUEST,
  CMD_REBALANCE_CONSUMER_GROUP_RESPONSE, CMD_SEND_DIRECT_REQUEST, CMD_SEND_DIRECT_RESPONSE,
  CommitOffsetRequest, CommitOffsetResponse, ConsumerGroupAssignment, ConsumerGroupMemberLease,
  CreateTopicRequest, CreateTopicResponse, DirectMessageRecord, ErrorResponse, FetchBatchRequest,
  FetchBatchResponse, FetchInboxRequest, FetchInboxResponse, FetchRecord, FetchRequest,
  FetchResponse, GetConsumerGroupAssignmentRequest, GetConsumerGroupAssignmentResponse,
  GroupPartitionAssignment, HandshakeRequest, HandshakeResponse, HeartbeatConsumerGroupRequest,
  HeartbeatConsumerGroupResponse, JoinConsumerGroupRequest, JoinConsumerGroupResponse,
  ListTopicsResponse, PingRequest, PingResponse, ProduceBatchRequest, ProduceBatchResponse,
  ProduceRequest, ProduceResponse, RebalanceConsumerGroupRequest, RebalanceConsumerGroupResponse,
  SendDirectRequest, SendDirectResponse, TopicMetadata, VERSION_1, decode_json, encode_json,
};
use transport::Frame;

use crate::{metrics, service::BrokerService};

#[derive(Debug, Clone, Copy)]
pub struct HandlerLimits {
  pub max_payload_bytes: usize,
  pub max_batch_payload_bytes: usize,
  pub max_fetch_records: usize,
}

impl Default for HandlerLimits {
  fn default() -> Self {
    Self {
      max_payload_bytes: 1024 * 1024,
      max_batch_payload_bytes: 8 * 1024 * 1024,
      max_fetch_records: 1_000,
    }
  }
}

static HANDLER_LIMITS: OnceLock<RwLock<HandlerLimits>> = OnceLock::new();

pub(super) fn set_handler_limits(limits: HandlerLimits) {
  let lock = HANDLER_LIMITS.get_or_init(|| RwLock::new(HandlerLimits::default()));
  if let Ok(mut guard) = lock.write() {
    *guard = limits;
  }
}

fn handler_limits() -> HandlerLimits {
  HANDLER_LIMITS
    .get_or_init(|| RwLock::new(HandlerLimits::default()))
    .read()
    .map(|guard| *guard)
    .unwrap_or_default()
}

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
    + store::ConsumerGroupStore
    + store::LocalPartitionStateStore
    + store::ConsensusLogStore
    + store::ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  async fn handle_frame(&self, frame: Frame) -> io::Result<Frame> {
    metrics::record_command(frame.header.command);
    let result = (|| -> io::Result<Frame> {
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
            .create_topic_with_retention(
              request.topic,
              request.partitions,
              request.retention_max_bytes,
            )
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
          let limits = handler_limits();
          if request.payload.len() > limits.max_payload_bytes {
            return error_response(
              "payload_too_large",
              format!(
                "produce payload size {} exceeds max_payload_bytes {}",
                request.payload.len(),
                limits.max_payload_bytes
              ),
            );
          }
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
        CMD_PRODUCE_BATCH_REQUEST => {
          let request: ProduceBatchRequest = decode_request(&frame)?;
          if request.payloads.is_empty() {
            return error_response(
              "invalid_request",
              "produce_batch payloads must not be empty",
            );
          }
          let limits = handler_limits();
          let total_payload_bytes = request
            .payloads
            .iter()
            .try_fold(0usize, |acc, payload| acc.checked_add(payload.len()))
            .unwrap_or(usize::MAX);
          if total_payload_bytes > limits.max_batch_payload_bytes {
            return error_response(
              "payload_too_large",
              format!(
                "produce_batch total payload bytes {} exceeds max_batch_payload_bytes {}",
                total_payload_bytes,
                limits.max_batch_payload_bytes
              ),
            );
          }
          if let Some((idx, payload)) = request
            .payloads
            .iter()
            .enumerate()
            .find(|(_, payload)| payload.len() > limits.max_payload_bytes)
          {
            return error_response(
              "payload_too_large",
              format!(
                "produce_batch payload at index {} size {} exceeds max_payload_bytes {}",
                idx,
                payload.len(),
                limits.max_payload_bytes
              ),
            );
          }
          let appended = request.payloads.len();
          let (base_offset, last_offset, next_offset) = self
            .publish_batch(request.topic, request.partition, request.payloads)
            .map_err(to_io_error)?;
          ok_response(
            CMD_PRODUCE_BATCH_RESPONSE,
            &ProduceBatchResponse {
              base_offset,
              last_offset,
              next_offset,
              appended,
            },
          )
        }
        CMD_FETCH_REQUEST => {
          let request: FetchRequest = decode_request(&frame)?;
          let limits = handler_limits();
          if request.max_records > limits.max_fetch_records {
            return error_response(
              "fetch_limit_exceeded",
              format!(
                "fetch max_records {} exceeds limit {}",
                request.max_records, limits.max_fetch_records
              ),
            );
          }
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
        CMD_FETCH_BATCH_REQUEST => {
          let request: FetchBatchRequest = decode_request(&frame)?;
          if request.items.is_empty() {
            return error_response("invalid_request", "fetch_batch items must not be empty");
          }
          let limits = handler_limits();
          if let Some((idx, item)) = request
            .items
            .iter()
            .enumerate()
            .find(|(_, item)| item.max_records > limits.max_fetch_records)
          {
            return error_response(
              "fetch_limit_exceeded",
              format!(
                "fetch_batch item {} max_records {} exceeds limit {}",
                idx,
                item.max_records,
                limits.max_fetch_records
              ),
            );
          }
          let mut items = Vec::with_capacity(request.items.len());
          for item in request.items {
            let fetched = self
              .fetch(
                &request.consumer,
                item.topic,
                item.partition,
                item.offset,
                item.max_records,
              )
              .map_err(to_io_error)?;
            items.push(protocol::FetchBatchItemResponse {
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
            });
          }
          ok_response(CMD_FETCH_BATCH_RESPONSE, &FetchBatchResponse { items })
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
          let limits = handler_limits();
          if request.payload.len() > limits.max_payload_bytes {
            return error_response(
              "payload_too_large",
              format!(
                "direct payload size {} exceeds max_payload_bytes {}",
                request.payload.len(),
                limits.max_payload_bytes
              ),
            );
          }
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
          let limits = handler_limits();
          if request.max_records > limits.max_fetch_records {
            return error_response(
              "fetch_limit_exceeded",
              format!(
                "fetch_inbox max_records {} exceeds limit {}",
                request.max_records, limits.max_fetch_records
              ),
            );
          }
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
        CMD_JOIN_CONSUMER_GROUP_REQUEST => {
          let request: JoinConsumerGroupRequest = decode_request(&frame)?;
          let lease = self
            .join_consumer_group(
              &request.group,
              &request.member_id,
              request.topics,
              request.session_timeout_ms,
            )
            .map_err(to_io_error)?;
          ok_response(
            CMD_JOIN_CONSUMER_GROUP_RESPONSE,
            &JoinConsumerGroupResponse {
              lease: lease_to_wire(lease),
            },
          )
        }
        CMD_HEARTBEAT_CONSUMER_GROUP_REQUEST => {
          let request: HeartbeatConsumerGroupRequest = decode_request(&frame)?;
          let lease = self
            .heartbeat_consumer_group(
              &request.group,
              &request.member_id,
              request.session_timeout_ms,
            )
            .map_err(to_io_error)?;
          ok_response(
            CMD_HEARTBEAT_CONSUMER_GROUP_RESPONSE,
            &HeartbeatConsumerGroupResponse {
              lease: lease_to_wire(lease),
            },
          )
        }
        CMD_REBALANCE_CONSUMER_GROUP_REQUEST => {
          let request: RebalanceConsumerGroupRequest = decode_request(&frame)?;
          let assignment = self
            .rebalance_consumer_group(&request.group)
            .map_err(to_io_error)?;
          ok_response(
            CMD_REBALANCE_CONSUMER_GROUP_RESPONSE,
            &RebalanceConsumerGroupResponse {
              assignment: assignment_to_wire(assignment),
            },
          )
        }
        CMD_GET_CONSUMER_GROUP_ASSIGNMENT_REQUEST => {
          let request: GetConsumerGroupAssignmentRequest = decode_request(&frame)?;
          let assignment = self
            .load_consumer_group_assignment(&request.group)
            .map_err(to_io_error)?;
          ok_response(
            CMD_GET_CONSUMER_GROUP_ASSIGNMENT_RESPONSE,
            &GetConsumerGroupAssignmentResponse {
              assignment: assignment.map(assignment_to_wire),
            },
          )
        }
        command => error_response("unknown_command", format!("unknown command id {command}")),
      }
    })();

    match result {
      Ok(frame) => Ok(frame),
      Err(err) if err.kind() == io::ErrorKind::InvalidData => {
        error_response("invalid_request", err.to_string())
      }
      Err(err) => {
        if let Some(wire_error) = err
          .get_ref()
          .and_then(|inner| inner.downcast_ref::<WireError>())
        {
          return error_response(wire_error.code, wire_error.message.clone());
        }
        Err(err)
      }
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
  let code = code.into();
  metrics::record_command_error(&code);
  ok_response(
    CMD_ERROR_RESPONSE,
    &ErrorResponse {
      code,
      message: message.into(),
    },
  )
}

fn to_io_error(err: store::StoreError) -> io::Error {
  io::Error::new(
    io::ErrorKind::Other,
    WireError {
      code: store_error_code(&err),
      message: err.to_string(),
    },
  )
}

fn store_error_code(err: &store::StoreError) -> &'static str {
  match err {
    store::StoreError::TopicAlreadyExists(_) => "topic_already_exists",
    store::StoreError::TopicNotFound(_) => "topic_not_found",
    store::StoreError::PartitionNotFound { .. } => "partition_not_found",
    store::StoreError::InvalidOffset { .. } => "invalid_offset",
    store::StoreError::TopicUnavailable { .. } => "topic_unavailable",
    store::StoreError::NotLeader { .. } => "not_leader",
    store::StoreError::Unsupported(_) => "unsupported_operation",
    store::StoreError::Codec(_) => "codec_error",
    store::StoreError::Corruption(_) => "storage_corruption",
    store::StoreError::Io(_)
    | store::StoreError::Redb(_)
    | store::StoreError::RedbTransaction(_)
    | store::StoreError::RedbDatabase(_)
    | store::StoreError::RedbTable(_)
    | store::StoreError::RedbStorage(_)
    | store::StoreError::RedbCommit(_) => "storage_error",
  }
}

fn lease_to_wire(lease: crate::service::GroupMemberLease) -> ConsumerGroupMemberLease {
  ConsumerGroupMemberLease {
    group: lease.group,
    member_id: lease.member_id,
    topics: lease.topics,
    session_timeout_ms: lease.session_timeout_ms,
    joined_at_ms: lease.joined_at_ms,
    last_heartbeat_ms: lease.last_heartbeat_ms,
    expires_at_ms: lease.expires_at_ms,
  }
}

fn assignment_to_wire(
  assignment: crate::service::GroupAssignmentSnapshot,
) -> ConsumerGroupAssignment {
  ConsumerGroupAssignment {
    group: assignment.group,
    generation: assignment.generation,
    assignments: assignment
      .assignments
      .into_iter()
      .map(|item| GroupPartitionAssignment {
        member_id: item.member_id,
        topic: item.topic,
        partition: item.partition,
      })
      .collect(),
    updated_at_ms: assignment.updated_at_ms,
  }
}

#[derive(Debug)]
struct WireError {
  code: &'static str,
  message: String,
}

impl std::fmt::Display for WireError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.message)
  }
}

impl std::error::Error for WireError {}
