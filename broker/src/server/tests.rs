use protocol::{
  CMD_FETCH_INBOX_REQUEST, CMD_FETCH_INBOX_RESPONSE, CMD_FETCH_REQUEST, CMD_FETCH_RESPONSE,
  FetchInboxRequest, FetchInboxResponse, FetchRequest, FetchResponse,
};
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
  )
  .unwrap();
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
  let log = InMemoryStore::new();
  let meta = InMemoryStore::new();
  let service = BrokerService::new(
    BrokerIdentity {
      node_id: 1,
      cluster_name: "test".to_owned(),
    },
    log,
    meta,
  )
  .unwrap();
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
