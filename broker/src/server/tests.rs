use std::{sync::Arc, time::Duration};

use protocol::{
  CMD_COMMIT_CONSUMER_GROUP_OFFSET_REQUEST, CMD_COMMIT_CONSUMER_GROUP_OFFSET_RESPONSE,
  CMD_CREATE_TOPIC_REQUEST, CMD_CREATE_TOPIC_RESPONSE, CMD_ERROR_RESPONSE, CMD_FETCH_INBOX_REQUEST,
  CMD_FETCH_BATCH_REQUEST, CMD_FETCH_BATCH_RESPONSE, CMD_FETCH_CONSUMER_GROUP_REQUEST,
  CMD_FETCH_CONSUMER_GROUP_BATCH_REQUEST, CMD_FETCH_CONSUMER_GROUP_BATCH_RESPONSE,
  CMD_FETCH_CONSUMER_GROUP_RESPONSE, CMD_FETCH_INBOX_RESPONSE, CMD_FETCH_REQUEST,
  CMD_FETCH_RESPONSE, CMD_NACK_REQUEST, CMD_NACK_RESPONSE, CMD_PROCESS_RETRY_REQUEST,
  CMD_PROCESS_RETRY_RESPONSE, CMD_SCHEDULE_DELAY_REQUEST, CMD_SCHEDULE_DELAY_RESPONSE,
  CMD_GET_CONSUMER_GROUP_ASSIGNMENT_REQUEST, CMD_GET_CONSUMER_GROUP_ASSIGNMENT_RESPONSE,
  CMD_HANDSHAKE_REQUEST, CMD_HANDSHAKE_RESPONSE, CMD_HEARTBEAT_CONSUMER_GROUP_REQUEST,
  CMD_HEARTBEAT_CONSUMER_GROUP_RESPONSE, CMD_JOIN_CONSUMER_GROUP_REQUEST,
  CMD_JOIN_CONSUMER_GROUP_RESPONSE, CMD_LIST_TOPICS_REQUEST, CMD_LIST_TOPICS_RESPONSE,
  CMD_PING_REQUEST, CMD_PING_RESPONSE, CMD_PRODUCE_BATCH_REQUEST, CMD_PRODUCE_BATCH_RESPONSE,
  CMD_PRODUCE_REQUEST, CMD_PRODUCE_RESPONSE, CMD_REBALANCE_CONSUMER_GROUP_REQUEST,
  CMD_REBALANCE_CONSUMER_GROUP_RESPONSE, CommitConsumerGroupOffsetRequest,
  CommitConsumerGroupOffsetResponse, CreateTopicRequest, ErrorResponse, FetchBatchRequest,
  FetchBatchResponse, FetchConsumerGroupBatchRequest, FetchConsumerGroupBatchResponse,
  FetchConsumerGroupRequest, FetchConsumerGroupResponse, FetchInboxRequest, FetchInboxResponse,
  FetchRequest, FetchResponse, GetConsumerGroupAssignmentRequest, GetConsumerGroupAssignmentResponse,
  HandshakeRequest, HandshakeResponse,
  HeartbeatConsumerGroupRequest, HeartbeatConsumerGroupResponse, JoinConsumerGroupRequest,
  JoinConsumerGroupResponse, ListTopicsResponse, PingRequest, PingResponse, ProduceBatchRequest,
  ProducePartitioning, ProduceRequest, ProduceResponse, RebalanceConsumerGroupRequest,
  RebalanceConsumerGroupResponse, NackRequest, NackResponse, ProcessRetryRequest,
  ProcessRetryResponse, ScheduleDelayRequest, ScheduleDelayResponse,
};
use store::InMemoryStore;
use tokio::time::Instant;
use transport::Frame;

use crate::service::{BrokerIdentity, BrokerService};

use super::BrokerCommandHandler;

fn build_service() -> BrokerService<InMemoryStore, InMemoryStore> {
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  BrokerService::new(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
  )
  .unwrap()
}

#[tokio::test]
async fn fetch_frame_roundtrip_works() {
  let service = build_service();
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
      min_records: None,
      max_wait_ms: None,
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

#[tokio::test]
async fn fetch_inbox_frame_roundtrip_works() {
  let service = build_service();
  service
    .send_direct("alice", "bob", None, b"hello".to_vec())
    .unwrap();

  let request = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_INBOX_REQUEST,
    protocol::encode_json(&FetchInboxRequest {
      recipient: "bob".to_owned(),
      max_records: 10,
    })
    .unwrap(),
  )
  .unwrap();

  let response = service.handle_frame(request).await.unwrap();
  assert_eq!(response.header.command, CMD_FETCH_INBOX_RESPONSE);

  let response: FetchInboxResponse = protocol::decode_json(&response.body).unwrap();
  assert_eq!(response.records.len(), 1);
  assert_eq!(response.records[0].sender, "alice");
  assert_eq!(response.records[0].payload, b"hello".to_vec());
}

#[tokio::test]
async fn handshake_and_ping_roundtrip_works() {
  let service = build_service();

  let handshake_request = Frame::new(
    protocol::VERSION_1,
    CMD_HANDSHAKE_REQUEST,
    protocol::encode_json(&HandshakeRequest {
      client_id: "client-1".to_owned(),
    })
    .unwrap(),
  )
  .unwrap();
  let handshake_response = service.handle_frame(handshake_request).await.unwrap();
  assert_eq!(handshake_response.header.command, CMD_HANDSHAKE_RESPONSE);
  let handshake: HandshakeResponse = protocol::decode_json(&handshake_response.body).unwrap();
  assert_eq!(handshake.protocol_version, protocol::VERSION_1);
  assert!(handshake.server_id.starts_with("test-node-"));

  let ping_request = Frame::new(
    protocol::VERSION_1,
    CMD_PING_REQUEST,
    protocol::encode_json(&PingRequest { nonce: 99 }).unwrap(),
  )
  .unwrap();
  let ping_response = service.handle_frame(ping_request).await.unwrap();
  assert_eq!(ping_response.header.command, CMD_PING_RESPONSE);
  let ping: PingResponse = protocol::decode_json(&ping_response.body).unwrap();
  assert_eq!(ping.nonce, 99);
}

#[tokio::test]
async fn create_topic_produce_and_list_topics_roundtrip_works() {
  let service = build_service();

  let create_topic = Frame::new(
    protocol::VERSION_1,
    CMD_CREATE_TOPIC_REQUEST,
    protocol::encode_json(&CreateTopicRequest {
      topic: "orders".to_owned(),
      partitions: 1,
      retention_max_bytes: None,
      cleanup_policy: None,
      max_message_bytes: None,
      max_batch_bytes: None,
      retention_ms: None,
      retry_policy: None,
      dead_letter_topic: None,
      delay_enabled: None,
      compaction_enabled: None,
      compaction_tombstone_retention_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let create_response = service.handle_frame(create_topic).await.unwrap();
  assert_eq!(create_response.header.command, CMD_CREATE_TOPIC_RESPONSE);

  let produce = Frame::new(
    protocol::VERSION_1,
    CMD_PRODUCE_REQUEST,
    protocol::encode_json(&ProduceRequest {
      topic: "orders".to_owned(),
      partition: Some(0),
      partitioning: ProducePartitioning::Explicit,
      key: None,
      tombstone: false,
      payload: b"msg".to_vec(),
    })
    .unwrap(),
  )
  .unwrap();
  let produce_response = service.handle_frame(produce).await.unwrap();
  assert_eq!(produce_response.header.command, CMD_PRODUCE_RESPONSE);
  let produce: ProduceResponse = protocol::decode_json(&produce_response.body).unwrap();
  assert_eq!(produce.partition, 0);
  assert_eq!(produce.offset, 0);

  let list_topics = Frame::new(protocol::VERSION_1, CMD_LIST_TOPICS_REQUEST, Vec::new()).unwrap();
  let list_response = service.handle_frame(list_topics).await.unwrap();
  assert_eq!(list_response.header.command, CMD_LIST_TOPICS_RESPONSE);
  let list: ListTopicsResponse = protocol::decode_json(&list_response.body).unwrap();
  assert!(list.topics.iter().any(|topic| topic.topic == "orders"));
}

#[tokio::test]
async fn produce_and_fetch_roundtrip_preserves_record_key() {
  let service = build_service();

  let create_topic = Frame::new(
    protocol::VERSION_1,
    CMD_CREATE_TOPIC_REQUEST,
    protocol::encode_json(&CreateTopicRequest {
      topic: "orders".to_owned(),
      partitions: 1,
      retention_max_bytes: None,
      cleanup_policy: None,
      max_message_bytes: None,
      max_batch_bytes: None,
      retention_ms: None,
      retry_policy: None,
      dead_letter_topic: None,
      delay_enabled: None,
      compaction_enabled: None,
      compaction_tombstone_retention_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(create_topic).await.unwrap();

  let produce = Frame::new(
    protocol::VERSION_1,
    CMD_PRODUCE_REQUEST,
    protocol::encode_json(&ProduceRequest {
      topic: "orders".to_owned(),
      partition: Some(0),
      partitioning: ProducePartitioning::Explicit,
      key: Some(b"customer-1".to_vec()),
      tombstone: false,
      payload: b"msg".to_vec(),
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(produce).await.unwrap();

  let fetch = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_REQUEST,
    protocol::encode_json(&FetchRequest {
      consumer: "c1".to_owned(),
      topic: "orders".to_owned(),
      partition: 0,
      offset: Some(0),
      max_records: 10,
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let fetch_response = service.handle_frame(fetch).await.unwrap();
  let fetched: FetchResponse = protocol::decode_json(&fetch_response.body).unwrap();
  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].key.as_deref(), Some(&b"customer-1"[..]));
  assert_eq!(fetched.records[0].payload, b"msg".to_vec());
}

#[tokio::test]
async fn produce_batch_and_fetch_batch_roundtrip_works() {
  let service = build_service();

  let create_topic = Frame::new(
    protocol::VERSION_1,
    CMD_CREATE_TOPIC_REQUEST,
    protocol::encode_json(&CreateTopicRequest {
      topic: "orders".to_owned(),
      partitions: 2,
      retention_max_bytes: None,
      cleanup_policy: None,
      max_message_bytes: None,
      max_batch_bytes: None,
      retention_ms: None,
      retry_policy: None,
      dead_letter_topic: None,
      delay_enabled: None,
      compaction_enabled: None,
      compaction_tombstone_retention_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let create_response = service.handle_frame(create_topic).await.unwrap();
  assert_eq!(create_response.header.command, CMD_CREATE_TOPIC_RESPONSE);

  let produce_batch = Frame::new(
    protocol::VERSION_1,
    CMD_PRODUCE_BATCH_REQUEST,
    protocol::encode_json(&ProduceBatchRequest {
      topic: "orders".to_owned(),
      partition: Some(0),
      partitioning: ProducePartitioning::Explicit,
      payloads: vec![b"m1".to_vec(), b"m2".to_vec(), b"m3".to_vec()],
      records: Vec::new(),
    })
    .unwrap(),
  )
  .unwrap();
  let produce_response = service.handle_frame(produce_batch).await.unwrap();
  assert_eq!(produce_response.header.command, CMD_PRODUCE_BATCH_RESPONSE);
  let produce: protocol::ProduceBatchResponse = protocol::decode_json(&produce_response.body).unwrap();
  assert_eq!(produce.partition, 0);
  assert_eq!(produce.base_offset, 0);
  assert_eq!(produce.last_offset, 2);
  assert_eq!(produce.next_offset, 3);
  assert_eq!(produce.appended, 3);

  let fetch_batch = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_BATCH_REQUEST,
    protocol::encode_json(&FetchBatchRequest {
      consumer: "c1".to_owned(),
      items: vec![
        protocol::FetchBatchItemRequest {
          topic: "orders".to_owned(),
          partition: 0,
          offset: Some(0),
          max_records: 10,
        },
        protocol::FetchBatchItemRequest {
          topic: "orders".to_owned(),
          partition: 1,
          offset: Some(0),
          max_records: 10,
        },
      ],
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let fetch_response = service.handle_frame(fetch_batch).await.unwrap();
  assert_eq!(fetch_response.header.command, CMD_FETCH_BATCH_RESPONSE);
  let fetched: FetchBatchResponse = protocol::decode_json(&fetch_response.body).unwrap();
  assert_eq!(fetched.items.len(), 2);
  assert_eq!(fetched.items[0].records.len(), 3);
  assert_eq!(fetched.items[0].records[0].payload, b"m1".to_vec());
  assert_eq!(fetched.items[1].records.len(), 0);
}

#[tokio::test]
async fn produce_batch_records_roundtrip_preserves_keys_and_tombstones() {
  let service = build_service();

  let create_topic = Frame::new(
    protocol::VERSION_1,
    CMD_CREATE_TOPIC_REQUEST,
    protocol::encode_json(&CreateTopicRequest {
      topic: "orders".to_owned(),
      partitions: 1,
      retention_max_bytes: None,
      cleanup_policy: None,
      max_message_bytes: None,
      max_batch_bytes: None,
      retention_ms: None,
      retry_policy: None,
      dead_letter_topic: None,
      delay_enabled: None,
      compaction_enabled: None,
      compaction_tombstone_retention_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(create_topic).await.unwrap();

  let produce_batch = Frame::new(
    protocol::VERSION_1,
    CMD_PRODUCE_BATCH_REQUEST,
    protocol::encode_json(&ProduceBatchRequest {
      topic: "orders".to_owned(),
      partition: Some(0),
      partitioning: ProducePartitioning::Explicit,
      payloads: Vec::new(),
      records: vec![
        protocol::ProduceBatchRecord {
          key: Some(b"k1".to_vec()),
          tombstone: false,
          payload: b"m1".to_vec(),
        },
        protocol::ProduceBatchRecord {
          key: Some(b"k2".to_vec()),
          tombstone: true,
          payload: Vec::new(),
        },
      ],
    })
    .unwrap(),
  )
  .unwrap();
  let produce_response = service.handle_frame(produce_batch).await.unwrap();
  assert_eq!(produce_response.header.command, CMD_PRODUCE_BATCH_RESPONSE);

  let fetch = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_REQUEST,
    protocol::encode_json(&FetchRequest {
      consumer: "c1".to_owned(),
      topic: "orders".to_owned(),
      partition: 0,
      offset: Some(0),
      max_records: 10,
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let fetch_response = service.handle_frame(fetch).await.unwrap();
  let fetched: FetchResponse = protocol::decode_json(&fetch_response.body).unwrap();
  assert_eq!(fetched.records.len(), 2);
  assert_eq!(fetched.records[0].key.as_deref(), Some(&b"k1"[..]));
  assert!(!fetched.records[0].tombstone);
  assert_eq!(fetched.records[1].key.as_deref(), Some(&b"k2"[..]));
  assert!(fetched.records[1].tombstone);
  assert!(fetched.records[1].payload.is_empty());
}

#[tokio::test]
async fn nack_moves_failed_record_into_retry_topic() {
  let service = build_service();

  let create_topic = Frame::new(
    protocol::VERSION_1,
    CMD_CREATE_TOPIC_REQUEST,
    protocol::encode_json(&CreateTopicRequest {
      topic: "orders".to_owned(),
      partitions: 1,
      retention_max_bytes: None,
      cleanup_policy: None,
      max_message_bytes: None,
      max_batch_bytes: None,
      retention_ms: None,
      retry_policy: None,
      dead_letter_topic: None,
      delay_enabled: None,
      compaction_enabled: None,
      compaction_tombstone_retention_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(create_topic).await.unwrap();

  let produce = Frame::new(
    protocol::VERSION_1,
    CMD_PRODUCE_REQUEST,
    protocol::encode_json(&ProduceRequest {
      topic: "orders".to_owned(),
      partition: Some(0),
      partitioning: ProducePartitioning::Explicit,
      key: None,
      tombstone: false,
      payload: b"m1".to_vec(),
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(produce).await.unwrap();

  let nack = Frame::new(
    protocol::VERSION_1,
    CMD_NACK_REQUEST,
    protocol::encode_json(&NackRequest {
      consumer: "c1".to_owned(),
      topic: "orders".to_owned(),
      partition: 0,
      offset: 0,
      last_error: Some("timeout".to_owned()),
    })
    .unwrap(),
  )
  .unwrap();
  let nack_response = service.handle_frame(nack).await.unwrap();
  assert_eq!(nack_response.header.command, CMD_NACK_RESPONSE);
  let nack: NackResponse = protocol::decode_json(&nack_response.body).unwrap();
  assert_eq!(nack.retry_topic, "__retry.orders");
  assert_eq!(nack.retry_partition, 0);
  assert_eq!(nack.retry_offset, 0);
  assert_eq!(nack.retry_count, 1);

  let fetch_retry = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_REQUEST,
    protocol::encode_json(&FetchRequest {
      consumer: "c1-retry".to_owned(),
      topic: "__retry.orders".to_owned(),
      partition: 0,
      offset: Some(0),
      max_records: 10,
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let fetch_retry_response = service.handle_frame(fetch_retry).await.unwrap();
  assert_eq!(fetch_retry_response.header.command, CMD_FETCH_RESPONSE);
  let fetched: FetchResponse = protocol::decode_json(&fetch_retry_response.body).unwrap();
  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].payload, b"m1".to_vec());
}

#[tokio::test]
async fn process_retry_moves_record_back_to_origin_topic() {
  let service = build_service();

  let create_topic = Frame::new(
    protocol::VERSION_1,
    CMD_CREATE_TOPIC_REQUEST,
    protocol::encode_json(&CreateTopicRequest {
      topic: "orders".to_owned(),
      partitions: 1,
      retention_max_bytes: None,
      cleanup_policy: None,
      max_message_bytes: None,
      max_batch_bytes: None,
      retention_ms: None,
      retry_policy: None,
      dead_letter_topic: None,
      delay_enabled: None,
      compaction_enabled: None,
      compaction_tombstone_retention_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(create_topic).await.unwrap();

  let produce = Frame::new(
    protocol::VERSION_1,
    CMD_PRODUCE_REQUEST,
    protocol::encode_json(&ProduceRequest {
      topic: "orders".to_owned(),
      partition: Some(0),
      partitioning: ProducePartitioning::Explicit,
      key: None,
      tombstone: false,
      payload: b"m1".to_vec(),
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(produce).await.unwrap();

  let nack = Frame::new(
    protocol::VERSION_1,
    CMD_NACK_REQUEST,
    protocol::encode_json(&NackRequest {
      consumer: "c1".to_owned(),
      topic: "orders".to_owned(),
      partition: 0,
      offset: 0,
      last_error: Some("timeout".to_owned()),
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(nack).await.unwrap();

  let process_retry = Frame::new(
    protocol::VERSION_1,
    CMD_PROCESS_RETRY_REQUEST,
    protocol::encode_json(&ProcessRetryRequest {
      consumer: "retry-worker".to_owned(),
      source_topic: "orders".to_owned(),
      partition: 0,
      max_records: 10,
    })
    .unwrap(),
  )
  .unwrap();
  let process_retry_response = service.handle_frame(process_retry).await.unwrap();
  assert_eq!(
    process_retry_response.header.command,
    CMD_PROCESS_RETRY_RESPONSE
  );
  let processed: ProcessRetryResponse = protocol::decode_json(&process_retry_response.body).unwrap();
  assert_eq!(processed.retry_topic, "__retry.orders");
  assert_eq!(processed.partition, 0);
  assert_eq!(processed.moved_to_origin, 1);
  assert_eq!(processed.moved_to_dead_letter, 0);
  assert_eq!(processed.committed_next_offset, Some(1));

  let fetch_origin = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_REQUEST,
    protocol::encode_json(&FetchRequest {
      consumer: "origin-reader".to_owned(),
      topic: "orders".to_owned(),
      partition: 0,
      offset: Some(1),
      max_records: 10,
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let fetch_origin_response = service.handle_frame(fetch_origin).await.unwrap();
  let fetched: FetchResponse = protocol::decode_json(&fetch_origin_response.body).unwrap();
  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].payload, b"m1".to_vec());
}

#[tokio::test]
async fn schedule_delay_writes_into_delay_topic() {
  let service = build_service();

  let create_topic = Frame::new(
    protocol::VERSION_1,
    CMD_CREATE_TOPIC_REQUEST,
    protocol::encode_json(&CreateTopicRequest {
      topic: "orders".to_owned(),
      partitions: 1,
      retention_max_bytes: None,
      cleanup_policy: None,
      max_message_bytes: None,
      max_batch_bytes: None,
      retention_ms: None,
      retry_policy: None,
      dead_letter_topic: None,
      delay_enabled: None,
      compaction_enabled: None,
      compaction_tombstone_retention_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(create_topic).await.unwrap();

  let schedule = Frame::new(
    protocol::VERSION_1,
    CMD_SCHEDULE_DELAY_REQUEST,
    protocol::encode_json(&ScheduleDelayRequest {
      topic: "orders".to_owned(),
      partition: 0,
      payload: b"m1".to_vec(),
      deliver_at_ms: 999_999,
    })
    .unwrap(),
  )
  .unwrap();
  let schedule_response = service.handle_frame(schedule).await.unwrap();
  assert_eq!(schedule_response.header.command, CMD_SCHEDULE_DELAY_RESPONSE);
  let scheduled: ScheduleDelayResponse = protocol::decode_json(&schedule_response.body).unwrap();
  assert_eq!(scheduled.delay_topic, "__delay.orders");
  assert_eq!(scheduled.partition, 0);
  assert_eq!(scheduled.offset, 0);

  let fetch_delay = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_REQUEST,
    protocol::encode_json(&FetchRequest {
      consumer: "delay-reader".to_owned(),
      topic: "__delay.orders".to_owned(),
      partition: 0,
      offset: Some(0),
      max_records: 10,
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let fetch_delay_response = service.handle_frame(fetch_delay).await.unwrap();
  let fetched: FetchResponse = protocol::decode_json(&fetch_delay_response.body).unwrap();
  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].payload, b"m1".to_vec());
}

#[tokio::test]
async fn unknown_command_returns_error_response() {
  let service = build_service();

  let request = Frame::new(protocol::VERSION_1, 65535, Vec::new()).unwrap();
  let response = service.handle_frame(request).await.unwrap();
  assert_eq!(response.header.command, CMD_ERROR_RESPONSE);

  let error: ErrorResponse = protocol::decode_json(&response.body).unwrap();
  assert_eq!(error.code, "unknown_command");
}

#[tokio::test]
async fn unsupported_version_returns_error_response() {
  let service = build_service();

  let request = Frame::new(
    9,
    CMD_PING_REQUEST,
    protocol::encode_json(&PingRequest { nonce: 1 }).unwrap(),
  )
  .unwrap();
  let response = service.handle_frame(request).await.unwrap();
  assert_eq!(response.header.command, CMD_ERROR_RESPONSE);

  let error: ErrorResponse = protocol::decode_json(&response.body).unwrap();
  assert_eq!(error.code, "unsupported_version");
}

#[tokio::test]
async fn invalid_json_returns_invalid_request_error_response() {
  let service = build_service();

  let request = Frame::new(protocol::VERSION_1, CMD_PING_REQUEST, b"{bad-json".to_vec()).unwrap();
  let response = service.handle_frame(request).await.unwrap();
  assert_eq!(response.header.command, CMD_ERROR_RESPONSE);

  let error: ErrorResponse = protocol::decode_json(&response.body).unwrap();
  assert_eq!(error.code, "invalid_request");
}

#[tokio::test]
async fn produce_missing_topic_returns_topic_not_found_error_response() {
  let service = build_service();

  let request = Frame::new(
    protocol::VERSION_1,
    CMD_PRODUCE_REQUEST,
    protocol::encode_json(&ProduceRequest {
      topic: "missing".to_owned(),
      partition: Some(0),
      partitioning: ProducePartitioning::Explicit,
      key: None,
      tombstone: false,
      payload: b"hello".to_vec(),
    })
    .unwrap(),
  )
  .unwrap();
  let response = service.handle_frame(request).await.unwrap();
  assert_eq!(response.header.command, CMD_ERROR_RESPONSE);

  let error: ErrorResponse = protocol::decode_json(&response.body).unwrap();
  assert_eq!(error.code, "topic_not_found");
}

#[tokio::test]
async fn round_robin_produce_request_routes_across_partitions() {
  let service = build_service();
  service.create_topic("orders", 2).unwrap();

  for payload in [b"a".to_vec(), b"b".to_vec()] {
    let request = Frame::new(
      protocol::VERSION_1,
      CMD_PRODUCE_REQUEST,
      protocol::encode_json(&ProduceRequest {
        topic: "orders".to_owned(),
        partition: None,
        partitioning: ProducePartitioning::RoundRobin,
        key: None,
        tombstone: false,
        payload,
      })
      .unwrap(),
    )
    .unwrap();
    let response = service.handle_frame(request).await.unwrap();
    assert_eq!(response.header.command, CMD_PRODUCE_RESPONSE);
  }

  let fetch_p0 = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_REQUEST,
    protocol::encode_json(&FetchRequest {
      consumer: "c1".to_owned(),
      topic: "orders".to_owned(),
      partition: 0,
      offset: Some(0),
      max_records: 10,
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let response_p0 = service.handle_frame(fetch_p0).await.unwrap();
  let fetched_p0: FetchResponse = protocol::decode_json(&response_p0.body).unwrap();
  assert_eq!(fetched_p0.records.len(), 1);

  let fetch_p1 = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_REQUEST,
    protocol::encode_json(&FetchRequest {
      consumer: "c1".to_owned(),
      topic: "orders".to_owned(),
      partition: 1,
      offset: Some(0),
      max_records: 10,
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let response_p1 = service.handle_frame(fetch_p1).await.unwrap();
  let fetched_p1: FetchResponse = protocol::decode_json(&response_p1.body).unwrap();
  assert_eq!(fetched_p1.records.len(), 1);
}

#[tokio::test]
async fn key_hash_produce_request_rejects_partition_hint() {
  let service = build_service();
  service.create_topic("orders", 2).unwrap();

  let request = Frame::new(
    protocol::VERSION_1,
    CMD_PRODUCE_REQUEST,
    protocol::encode_json(&ProduceRequest {
      topic: "orders".to_owned(),
      partition: Some(0),
      partitioning: ProducePartitioning::KeyHash,
      key: Some(b"k1".to_vec()),
      tombstone: false,
      payload: b"a".to_vec(),
    })
    .unwrap(),
  )
  .unwrap();
  let response = service.handle_frame(request).await.unwrap();
  assert_eq!(response.header.command, CMD_ERROR_RESPONSE);

  let error: ErrorResponse = protocol::decode_json(&response.body).unwrap();
  assert_eq!(error.code, "invalid_request");
}

#[tokio::test]
async fn consumer_group_commands_roundtrip_work() {
  let service = build_service();
  service.create_topic("orders", 2).unwrap();
  service.create_topic("payments", 1).unwrap();

  let join_request = Frame::new(
    protocol::VERSION_1,
    CMD_JOIN_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&JoinConsumerGroupRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      topics: vec!["orders".to_owned(), "payments".to_owned()],
      session_timeout_ms: 30_000,
    })
    .unwrap(),
  )
  .unwrap();
  let join_response = service.handle_frame(join_request).await.unwrap();
  assert_eq!(
    join_response.header.command,
    CMD_JOIN_CONSUMER_GROUP_RESPONSE
  );
  let join: JoinConsumerGroupResponse = protocol::decode_json(&join_response.body).unwrap();
  assert_eq!(join.lease.group, "orders-cg");
  assert_eq!(join.lease.member_id, "member-1");

  let heartbeat_request = Frame::new(
    protocol::VERSION_1,
    CMD_HEARTBEAT_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&HeartbeatConsumerGroupRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      session_timeout_ms: Some(45_000),
    })
    .unwrap(),
  )
  .unwrap();
  let heartbeat_response = service.handle_frame(heartbeat_request).await.unwrap();
  assert_eq!(
    heartbeat_response.header.command,
    CMD_HEARTBEAT_CONSUMER_GROUP_RESPONSE
  );
  let heartbeat: HeartbeatConsumerGroupResponse =
    protocol::decode_json(&heartbeat_response.body).unwrap();
  assert_eq!(heartbeat.lease.session_timeout_ms, 45_000);

  let rebalance_request = Frame::new(
    protocol::VERSION_1,
    CMD_REBALANCE_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&RebalanceConsumerGroupRequest {
      group: "orders-cg".to_owned(),
    })
    .unwrap(),
  )
  .unwrap();
  let rebalance_response = service.handle_frame(rebalance_request).await.unwrap();
  assert_eq!(
    rebalance_response.header.command,
    CMD_REBALANCE_CONSUMER_GROUP_RESPONSE
  );
  let rebalance: RebalanceConsumerGroupResponse =
    protocol::decode_json(&rebalance_response.body).unwrap();
  assert_eq!(rebalance.assignment.group, "orders-cg");
  assert!(rebalance.assignment.generation >= 1);
  assert_eq!(rebalance.assignment.assignments.len(), 3);

  let get_assignment_request = Frame::new(
    protocol::VERSION_1,
    CMD_GET_CONSUMER_GROUP_ASSIGNMENT_REQUEST,
    protocol::encode_json(&GetConsumerGroupAssignmentRequest {
      group: "orders-cg".to_owned(),
    })
    .unwrap(),
  )
  .unwrap();
  let get_assignment_response = service.handle_frame(get_assignment_request).await.unwrap();
  assert_eq!(
    get_assignment_response.header.command,
    CMD_GET_CONSUMER_GROUP_ASSIGNMENT_RESPONSE
  );
  let assignment: GetConsumerGroupAssignmentResponse =
    protocol::decode_json(&get_assignment_response.body).unwrap();
  let assignment = assignment.assignment.expect("assignment should exist");
  assert_eq!(assignment.group, "orders-cg");
  assert_eq!(assignment.generation, rebalance.assignment.generation);
  assert_eq!(assignment.assignments.len(), 3);
}

#[tokio::test]
async fn consumer_group_fetch_and_commit_roundtrip_work() {
  let service = build_service();
  service.create_topic("orders", 1).unwrap();
  service.publish("orders", 0, b"a".to_vec()).unwrap();
  service.publish("orders", 0, b"b".to_vec()).unwrap();

  let join_request = Frame::new(
    protocol::VERSION_1,
    CMD_JOIN_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&JoinConsumerGroupRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      topics: vec!["orders".to_owned()],
      session_timeout_ms: 30_000,
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(join_request).await.unwrap();
  let assignment = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should exist after join");

  let fetch_request = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&FetchConsumerGroupRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      generation: assignment.generation,
      topic: "orders".to_owned(),
      partition: 0,
      offset: None,
      max_records: 10,
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let fetch_response = service.handle_frame(fetch_request).await.unwrap();
  assert_eq!(fetch_response.header.command, CMD_FETCH_CONSUMER_GROUP_RESPONSE);
  let fetched: FetchConsumerGroupResponse = protocol::decode_json(&fetch_response.body).unwrap();
  assert_eq!(fetched.records.len(), 2);
  assert_eq!(fetched.records[0].payload, b"a".to_vec());
  assert_eq!(fetched.records[1].payload, b"b".to_vec());

  let commit_request = Frame::new(
    protocol::VERSION_1,
    CMD_COMMIT_CONSUMER_GROUP_OFFSET_REQUEST,
    protocol::encode_json(&CommitConsumerGroupOffsetRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      generation: assignment.generation,
      topic: "orders".to_owned(),
      partition: 0,
      next_offset: 1,
    })
    .unwrap(),
  )
  .unwrap();
  let commit_response = service.handle_frame(commit_request).await.unwrap();
  assert_eq!(
    commit_response.header.command,
    CMD_COMMIT_CONSUMER_GROUP_OFFSET_RESPONSE
  );
  let committed: CommitConsumerGroupOffsetResponse =
    protocol::decode_json(&commit_response.body).unwrap();
  assert_eq!(committed.next_offset, 1);

  let resume_request = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&FetchConsumerGroupRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      generation: assignment.generation,
      topic: "orders".to_owned(),
      partition: 0,
      offset: None,
      max_records: 10,
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let resume_response = service.handle_frame(resume_request).await.unwrap();
  let resumed: FetchConsumerGroupResponse = protocol::decode_json(&resume_response.body).unwrap();
  assert_eq!(resumed.records.len(), 1);
  assert_eq!(resumed.records[0].payload, b"b".to_vec());
}

#[tokio::test]
async fn stale_consumer_group_generation_returns_error_response() {
  let service = build_service();
  service.create_topic("orders", 1).unwrap();
  service.publish("orders", 0, b"a".to_vec()).unwrap();

  let join_member_1 = Frame::new(
    protocol::VERSION_1,
    CMD_JOIN_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&JoinConsumerGroupRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      topics: vec!["orders".to_owned()],
      session_timeout_ms: 30_000,
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(join_member_1).await.unwrap();
  let stale_generation = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should exist after first join")
    .generation;

  let join_member_2 = Frame::new(
    protocol::VERSION_1,
    CMD_JOIN_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&JoinConsumerGroupRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-2".to_owned(),
      topics: vec!["orders".to_owned()],
      session_timeout_ms: 30_000,
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(join_member_2).await.unwrap();

  let fetch_request = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&FetchConsumerGroupRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      generation: stale_generation,
      topic: "orders".to_owned(),
      partition: 0,
      offset: None,
      max_records: 10,
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let response = service.handle_frame(fetch_request).await.unwrap();
  assert_eq!(response.header.command, CMD_ERROR_RESPONSE);

  let error: ErrorResponse = protocol::decode_json(&response.body).unwrap();
  assert_eq!(error.code, "consumer_group_generation_mismatch");
}

#[tokio::test]
async fn consumer_group_fetch_batch_roundtrip_work() {
  let service = build_service();
  service.create_topic("orders", 2).unwrap();
  service.publish("orders", 0, b"a0".to_vec()).unwrap();
  service.publish("orders", 1, b"b0".to_vec()).unwrap();

  let join_request = Frame::new(
    protocol::VERSION_1,
    CMD_JOIN_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&JoinConsumerGroupRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      topics: vec!["orders".to_owned()],
      session_timeout_ms: 30_000,
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(join_request).await.unwrap();
  let assignment = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should exist after join");

  let fetch_request = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_CONSUMER_GROUP_BATCH_REQUEST,
    protocol::encode_json(&FetchConsumerGroupBatchRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      generation: assignment.generation,
      items: vec![
        protocol::FetchBatchItemRequest {
          topic: "orders".to_owned(),
          partition: 0,
          offset: None,
          max_records: 10,
        },
        protocol::FetchBatchItemRequest {
          topic: "orders".to_owned(),
          partition: 1,
          offset: None,
          max_records: 10,
        },
      ],
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let fetch_response = service.handle_frame(fetch_request).await.unwrap();
  assert_eq!(
    fetch_response.header.command,
    CMD_FETCH_CONSUMER_GROUP_BATCH_RESPONSE
  );
  let fetched: FetchConsumerGroupBatchResponse =
    protocol::decode_json(&fetch_response.body).unwrap();
  assert_eq!(fetched.items.len(), 2);
  assert_eq!(fetched.items[0].records.len(), 1);
  assert_eq!(fetched.items[0].records[0].payload, b"a0".to_vec());
  assert_eq!(fetched.items[1].records.len(), 1);
  assert_eq!(fetched.items[1].records[0].payload, b"b0".to_vec());
}

#[tokio::test]
async fn consumer_group_fetch_batch_unassigned_partition_returns_error_response() {
  let service = build_service();
  service.create_topic("orders", 2).unwrap();

  let join_member_1 = Frame::new(
    protocol::VERSION_1,
    CMD_JOIN_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&JoinConsumerGroupRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      topics: vec!["orders".to_owned()],
      session_timeout_ms: 30_000,
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(join_member_1).await.unwrap();

  let join_member_2 = Frame::new(
    protocol::VERSION_1,
    CMD_JOIN_CONSUMER_GROUP_REQUEST,
    protocol::encode_json(&JoinConsumerGroupRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-2".to_owned(),
      topics: vec!["orders".to_owned()],
      session_timeout_ms: 30_000,
    })
    .unwrap(),
  )
  .unwrap();
  let _ = service.handle_frame(join_member_2).await.unwrap();
  let assignment = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should exist after joins");

  let owned_partition = assignment
    .assignments
    .iter()
    .find(|item| item.member_id == "member-1")
    .expect("member-1 should own a partition")
    .partition;
  let unassigned_partition = if owned_partition == 0 { 1 } else { 0 };

  let fetch_request = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_CONSUMER_GROUP_BATCH_REQUEST,
    protocol::encode_json(&FetchConsumerGroupBatchRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      generation: assignment.generation,
      items: vec![
        protocol::FetchBatchItemRequest {
          topic: "orders".to_owned(),
          partition: owned_partition,
          offset: None,
          max_records: 10,
        },
        protocol::FetchBatchItemRequest {
          topic: "orders".to_owned(),
          partition: unassigned_partition,
          offset: None,
          max_records: 10,
        },
      ],
      min_records: None,
      max_wait_ms: None,
    })
    .unwrap(),
  )
  .unwrap();
  let response = service.handle_frame(fetch_request).await.unwrap();
  assert_eq!(response.header.command, CMD_ERROR_RESPONSE);

  let error: ErrorResponse = protocol::decode_json(&response.body).unwrap();
  assert_eq!(error.code, "consumer_group_not_assigned");
}

#[tokio::test]
async fn fetch_request_long_polls_until_record_arrives() {
  let service = Arc::new(build_service());
  service.create_topic("orders", 1).unwrap();

  let publisher = Arc::clone(&service);
  tokio::spawn(async move {
    tokio::time::sleep(Duration::from_millis(50)).await;
    publisher.publish("orders", 0, b"delayed".to_vec()).unwrap();
  });

  let request = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_REQUEST,
    protocol::encode_json(&FetchRequest {
      consumer: "c1".to_owned(),
      topic: "orders".to_owned(),
      partition: 0,
      offset: Some(0),
      max_records: 10,
      min_records: None,
      max_wait_ms: Some(200),
    })
    .unwrap(),
  )
  .unwrap();

  let started_at = Instant::now();
  let response = service.handle_frame(request).await.unwrap();
  assert!(started_at.elapsed() >= Duration::from_millis(40));
  assert_eq!(response.header.command, CMD_FETCH_RESPONSE);

  let fetched: FetchResponse = protocol::decode_json(&response.body).unwrap();
  assert_eq!(fetched.records.len(), 1);
  assert_eq!(fetched.records[0].payload, b"delayed".to_vec());
}

#[tokio::test]
async fn consumer_group_fetch_batch_long_polls_until_any_partition_has_data() {
  let service = Arc::new(build_service());
  service.create_topic("orders", 2).unwrap();
  service
    .join_consumer_group(
      "orders-cg",
      "member-1",
      vec!["orders".to_owned()],
      30_000,
    )
    .unwrap();
  let assignment = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should exist after join");

  let publisher = Arc::clone(&service);
  tokio::spawn(async move {
    tokio::time::sleep(Duration::from_millis(50)).await;
    publisher.publish("orders", 1, b"late-b1".to_vec()).unwrap();
  });

  let request = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_CONSUMER_GROUP_BATCH_REQUEST,
    protocol::encode_json(&FetchConsumerGroupBatchRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      generation: assignment.generation,
      items: vec![
        protocol::FetchBatchItemRequest {
          topic: "orders".to_owned(),
          partition: 0,
          offset: None,
          max_records: 10,
        },
        protocol::FetchBatchItemRequest {
          topic: "orders".to_owned(),
          partition: 1,
          offset: None,
          max_records: 10,
        },
      ],
      min_records: None,
      max_wait_ms: Some(200),
    })
    .unwrap(),
  )
  .unwrap();

  let started_at = Instant::now();
  let response = service.handle_frame(request).await.unwrap();
  assert!(started_at.elapsed() >= Duration::from_millis(40));
  assert_eq!(response.header.command, CMD_FETCH_CONSUMER_GROUP_BATCH_RESPONSE);

  let fetched: FetchConsumerGroupBatchResponse =
    protocol::decode_json(&response.body).unwrap();
  assert_eq!(fetched.items.len(), 2);
  assert_eq!(fetched.items[0].records.len(), 0);
  assert_eq!(fetched.items[1].records.len(), 1);
  assert_eq!(fetched.items[1].records[0].payload, b"late-b1".to_vec());
}

#[tokio::test]
async fn fetch_request_long_polls_until_min_records_threshold_is_met() {
  let service = Arc::new(build_service());
  service.create_topic("orders", 1).unwrap();

  let publisher = Arc::clone(&service);
  tokio::spawn(async move {
    tokio::time::sleep(Duration::from_millis(30)).await;
    publisher.publish("orders", 0, b"first".to_vec()).unwrap();
    tokio::time::sleep(Duration::from_millis(35)).await;
    publisher.publish("orders", 0, b"second".to_vec()).unwrap();
  });

  let request = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_REQUEST,
    protocol::encode_json(&FetchRequest {
      consumer: "c1".to_owned(),
      topic: "orders".to_owned(),
      partition: 0,
      offset: Some(0),
      max_records: 10,
      min_records: Some(2),
      max_wait_ms: Some(250),
    })
    .unwrap(),
  )
  .unwrap();

  let started_at = Instant::now();
  let response = service.handle_frame(request).await.unwrap();
  assert!(started_at.elapsed() >= Duration::from_millis(55));
  assert_eq!(response.header.command, CMD_FETCH_RESPONSE);

  let fetched: FetchResponse = protocol::decode_json(&response.body).unwrap();
  assert_eq!(fetched.records.len(), 2);
  assert_eq!(fetched.records[0].payload, b"first".to_vec());
  assert_eq!(fetched.records[1].payload, b"second".to_vec());
}

#[tokio::test]
async fn consumer_group_fetch_batch_long_polls_until_min_records_threshold_is_met() {
  let service = Arc::new(build_service());
  service.create_topic("orders", 2).unwrap();
  service
    .join_consumer_group(
      "orders-cg",
      "member-1",
      vec!["orders".to_owned()],
      30_000,
    )
    .unwrap();
  let assignment = service
    .load_consumer_group_assignment("orders-cg")
    .unwrap()
    .expect("assignment should exist after join");

  let publisher = Arc::clone(&service);
  tokio::spawn(async move {
    tokio::time::sleep(Duration::from_millis(30)).await;
    publisher.publish("orders", 0, b"p0-1".to_vec()).unwrap();
    tokio::time::sleep(Duration::from_millis(35)).await;
    publisher.publish("orders", 1, b"p1-1".to_vec()).unwrap();
  });

  let request = Frame::new(
    protocol::VERSION_1,
    CMD_FETCH_CONSUMER_GROUP_BATCH_REQUEST,
    protocol::encode_json(&FetchConsumerGroupBatchRequest {
      group: "orders-cg".to_owned(),
      member_id: "member-1".to_owned(),
      generation: assignment.generation,
      items: vec![
        protocol::FetchBatchItemRequest {
          topic: "orders".to_owned(),
          partition: 0,
          offset: None,
          max_records: 10,
        },
        protocol::FetchBatchItemRequest {
          topic: "orders".to_owned(),
          partition: 1,
          offset: None,
          max_records: 10,
        },
      ],
      min_records: Some(2),
      max_wait_ms: Some(250),
    })
    .unwrap(),
  )
  .unwrap();

  let started_at = Instant::now();
  let response = service.handle_frame(request).await.unwrap();
  assert!(started_at.elapsed() >= Duration::from_millis(55));
  assert_eq!(response.header.command, CMD_FETCH_CONSUMER_GROUP_BATCH_RESPONSE);

  let fetched: FetchConsumerGroupBatchResponse =
    protocol::decode_json(&response.body).unwrap();
  assert_eq!(fetched.items.len(), 2);
  assert_eq!(fetched.items[0].records.len(), 1);
  assert_eq!(fetched.items[0].records[0].payload, b"p0-1".to_vec());
  assert_eq!(fetched.items[1].records.len(), 1);
  assert_eq!(fetched.items[1].records[0].payload, b"p1-1".to_vec());
}
