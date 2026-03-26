use std::{env, io};

use protocol::{
  CMD_COMMIT_OFFSET_REQUEST, CMD_COMMIT_OFFSET_RESPONSE, CMD_CREATE_TOPIC_REQUEST,
  CMD_CREATE_TOPIC_RESPONSE, CMD_FETCH_REQUEST, CMD_FETCH_RESPONSE, CMD_HANDSHAKE_REQUEST,
  CMD_HANDSHAKE_RESPONSE, CMD_LIST_TOPICS_REQUEST, CMD_LIST_TOPICS_RESPONSE, CMD_PING_REQUEST,
  CMD_PING_RESPONSE, CMD_PRODUCE_REQUEST, CMD_PRODUCE_RESPONSE, CommitOffsetRequest,
  CommitOffsetResponse, CreateTopicRequest, CreateTopicResponse, FetchRequest, FetchResponse,
  HandshakeRequest, HandshakeResponse, ListTopicsResponse, PingRequest, PingResponse,
  ProduceRequest, ProduceResponse,
};
use tokio::net::TcpStream;

#[path = "shared/mod.rs"]
mod common;
use common::{request_json, unix_timestamp_ms};

#[tokio::main]
async fn main() -> io::Result<()> {
  let addr = env::var("BROKER_ADDR").unwrap_or_else(|_| "127.0.0.1:9090".to_owned());
  let mut stream = TcpStream::connect(&addr).await?;
  println!("connected to broker at {addr}");

  let handshake: HandshakeResponse = request_json(
    &mut stream,
    CMD_HANDSHAKE_REQUEST,
    CMD_HANDSHAKE_RESPONSE,
    &HandshakeRequest {
      client_id: "smoke-e2e".to_owned(),
    },
  )
  .await?;
  println!(
    "handshake ok: server_id={} protocol_version={}",
    handshake.server_id, handshake.protocol_version
  );

  let ping_nonce = 42_u64;
  let ping: PingResponse = request_json(
    &mut stream,
    CMD_PING_REQUEST,
    CMD_PING_RESPONSE,
    &PingRequest { nonce: ping_nonce },
  )
  .await?;
  if ping.nonce != ping_nonce {
    return Err(io::Error::other("ping nonce mismatch"));
  }
  println!("ping ok");

  let topic = format!("smoke-{}", unix_timestamp_ms());
  let created: CreateTopicResponse = request_json(
    &mut stream,
    CMD_CREATE_TOPIC_REQUEST,
    CMD_CREATE_TOPIC_RESPONSE,
    &CreateTopicRequest {
      topic: topic.clone(),
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
    },
  )
  .await?;
  println!(
    "topic created: {} (partitions={})",
    created.topic, created.partitions
  );

  let topics: ListTopicsResponse = request_json(
    &mut stream,
    CMD_LIST_TOPICS_REQUEST,
    CMD_LIST_TOPICS_RESPONSE,
    &serde_json::json!({}),
  )
  .await?;
  if !topics.topics.iter().any(|metadata| metadata.topic == topic) {
    return Err(io::Error::other(
      "list_topics does not include created topic",
    ));
  }
  println!("list_topics ok");

  let payload = b"smoke-payload".to_vec();
  let produce: ProduceResponse = request_json(
    &mut stream,
    CMD_PRODUCE_REQUEST,
    CMD_PRODUCE_RESPONSE,
    &ProduceRequest {
      topic: topic.clone(),
      partition: 0,
      payload: payload.clone(),
    },
  )
  .await?;
  println!("produce ok: offset={}", produce.offset);

  let fetch: FetchResponse = request_json(
    &mut stream,
    CMD_FETCH_REQUEST,
    CMD_FETCH_RESPONSE,
    &FetchRequest {
      consumer: "smoke-consumer".to_owned(),
      topic: topic.clone(),
      partition: 0,
      offset: Some(0),
      max_records: 10,
    },
  )
  .await?;
  if fetch.records.is_empty() {
    return Err(io::Error::other("fetch returned zero records"));
  }
  if fetch.records[0].payload != payload {
    return Err(io::Error::other("payload mismatch after fetch"));
  }
  println!("fetch ok: records={}", fetch.records.len());

  let commit: CommitOffsetResponse = request_json(
    &mut stream,
    CMD_COMMIT_OFFSET_REQUEST,
    CMD_COMMIT_OFFSET_RESPONSE,
    &CommitOffsetRequest {
      consumer: "smoke-consumer".to_owned(),
      topic: topic.clone(),
      partition: 0,
      next_offset: fetch.next_offset,
    },
  )
  .await?;
  println!(
    "commit ok: topic={} partition={} next_offset={}",
    commit.topic, commit.partition, commit.next_offset
  );

  println!("smoke test passed for topic {topic}");
  Ok(())
}
