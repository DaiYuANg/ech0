use protocol::{
  CMD_CREATE_TOPIC_REQUEST, CMD_CREATE_TOPIC_RESPONSE, CMD_ERROR_RESPONSE, CMD_FETCH_INBOX_REQUEST,
  CMD_FETCH_INBOX_RESPONSE, CMD_FETCH_REQUEST, CMD_FETCH_RESPONSE,
  CMD_GET_CONSUMER_GROUP_ASSIGNMENT_REQUEST, CMD_GET_CONSUMER_GROUP_ASSIGNMENT_RESPONSE,
  CMD_HANDSHAKE_REQUEST, CMD_HANDSHAKE_RESPONSE, CMD_HEARTBEAT_CONSUMER_GROUP_REQUEST,
  CMD_HEARTBEAT_CONSUMER_GROUP_RESPONSE, CMD_JOIN_CONSUMER_GROUP_REQUEST,
  CMD_JOIN_CONSUMER_GROUP_RESPONSE, CMD_LIST_TOPICS_REQUEST, CMD_LIST_TOPICS_RESPONSE,
  CMD_PING_REQUEST, CMD_PING_RESPONSE, CMD_PRODUCE_REQUEST, CMD_PRODUCE_RESPONSE,
  CMD_REBALANCE_CONSUMER_GROUP_REQUEST, CMD_REBALANCE_CONSUMER_GROUP_RESPONSE, CreateTopicRequest,
  ErrorResponse, FetchInboxRequest, FetchInboxResponse, FetchRequest, FetchResponse,
  GetConsumerGroupAssignmentRequest, GetConsumerGroupAssignmentResponse, HandshakeRequest,
  HandshakeResponse, HeartbeatConsumerGroupRequest, HeartbeatConsumerGroupResponse,
  JoinConsumerGroupRequest, JoinConsumerGroupResponse, ListTopicsResponse, PingRequest,
  PingResponse, ProduceRequest, ProduceResponse, RebalanceConsumerGroupRequest,
  RebalanceConsumerGroupResponse,
};
use store::InMemoryStore;
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
      partition: 0,
      payload: b"msg".to_vec(),
    })
    .unwrap(),
  )
  .unwrap();
  let produce_response = service.handle_frame(produce).await.unwrap();
  assert_eq!(produce_response.header.command, CMD_PRODUCE_RESPONSE);
  let produce: ProduceResponse = protocol::decode_json(&produce_response.body).unwrap();
  assert_eq!(produce.offset, 0);

  let list_topics = Frame::new(protocol::VERSION_1, CMD_LIST_TOPICS_REQUEST, Vec::new()).unwrap();
  let list_response = service.handle_frame(list_topics).await.unwrap();
  assert_eq!(list_response.header.command, CMD_LIST_TOPICS_RESPONSE);
  let list: ListTopicsResponse = protocol::decode_json(&list_response.body).unwrap();
  assert!(list.topics.iter().any(|topic| topic.topic == "orders"));
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
      partition: 0,
      payload: b"hello".to_vec(),
    })
    .unwrap(),
  )
  .unwrap();
  let response = service.handle_frame(request).await.unwrap();
  assert_eq!(response.header.command, CMD_ERROR_RESPONSE);

  let error: ErrorResponse = protocol::decode_json(&response.body).unwrap();
  assert_eq!(error.code, "partition_not_found");
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
