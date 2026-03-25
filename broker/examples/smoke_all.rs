use std::{
  env, fs, io,
  net::TcpStream as StdTcpStream,
  path::Path,
  process::{Child, Command, Stdio},
  thread,
  time::{Duration, Instant},
};

use protocol::{
  AckDirectRequest, AckDirectResponse, CMD_ACK_DIRECT_REQUEST, CMD_ACK_DIRECT_RESPONSE,
  CMD_COMMIT_OFFSET_REQUEST, CMD_COMMIT_OFFSET_RESPONSE, CMD_CREATE_TOPIC_REQUEST,
  CMD_CREATE_TOPIC_RESPONSE, CMD_FETCH_INBOX_REQUEST, CMD_FETCH_INBOX_RESPONSE, CMD_FETCH_REQUEST,
  CMD_FETCH_RESPONSE, CMD_HANDSHAKE_REQUEST, CMD_HANDSHAKE_RESPONSE, CMD_LIST_TOPICS_REQUEST,
  CMD_LIST_TOPICS_RESPONSE, CMD_PING_REQUEST, CMD_PING_RESPONSE, CMD_PRODUCE_REQUEST,
  CMD_PRODUCE_RESPONSE, CMD_SEND_DIRECT_REQUEST, CMD_SEND_DIRECT_RESPONSE, CommitOffsetRequest,
  CommitOffsetResponse, CreateTopicRequest, CreateTopicResponse, FetchInboxRequest,
  FetchInboxResponse, FetchRequest, FetchResponse, HandshakeRequest, HandshakeResponse,
  ListTopicsResponse, PingRequest, PingResponse, ProduceRequest, ProduceResponse,
  SendDirectRequest, SendDirectResponse,
};
use tokio::net::TcpStream;

#[path = "shared/mod.rs"]
mod common;
use common::{request_json, unix_timestamp_ms};

#[tokio::main]
async fn main() -> io::Result<()> {
  ensure_local_config()?;

  let broker_addr = env::var("BROKER_ADDR").unwrap_or_else(|_| "127.0.0.1:9090".to_owned());
  let timeout_secs = env::var("SMOKE_STARTUP_TIMEOUT_SECS")
    .ok()
    .and_then(|value| value.parse::<u64>().ok())
    .unwrap_or(25);

  let mut broker = start_broker_process()?;
  let result = run_all_checks(&broker_addr, timeout_secs).await;

  stop_broker_process(&mut broker)?;
  result
}

fn ensure_local_config() -> io::Result<()> {
  if !Path::new("config/ech0.toml").exists() {
    fs::copy("config/ech0.toml.example", "config/ech0.toml")?;
    println!("created config/ech0.toml from example");
  }

  if !Path::new("config/ech0.local.toml").exists() {
    fs::write("config/ech0.local.toml", "[raft]\nenabled = false\n")?;
    println!("created config/ech0.local.toml with raft disabled");
  }

  Ok(())
}

fn start_broker_process() -> io::Result<Child> {
  fs::create_dir_all("logs")?;
  let log_file = fs::File::create("logs/smoke-broker.log")?;
  let log_file_err = log_file.try_clone()?;

  Command::new("cargo")
    .args(["run", "-p", "broker-bin"])
    .stdout(Stdio::from(log_file))
    .stderr(Stdio::from(log_file_err))
    .spawn()
}

fn stop_broker_process(child: &mut Child) -> io::Result<()> {
  if child.try_wait()?.is_none() {
    child.kill()?;
    let _ = child.wait()?;
  }
  Ok(())
}

async fn run_all_checks(broker_addr: &str, timeout_secs: u64) -> io::Result<()> {
  wait_broker_ready(broker_addr, timeout_secs)?;
  println!("broker is ready at {broker_addr}");

  run_queue_flow(broker_addr).await?;
  run_direct_flow(broker_addr).await?;

  println!("all smoke checks passed");
  Ok(())
}

fn wait_broker_ready(addr: &str, timeout_secs: u64) -> io::Result<()> {
  let deadline = Instant::now() + Duration::from_secs(timeout_secs);
  while Instant::now() < deadline {
    if StdTcpStream::connect(addr).is_ok() {
      return Ok(());
    }
    thread::sleep(Duration::from_millis(300));
  }

  Err(io::Error::new(
    io::ErrorKind::TimedOut,
    format!("broker did not become ready at {addr} within {timeout_secs}s"),
  ))
}

async fn run_queue_flow(addr: &str) -> io::Result<()> {
  let mut stream = TcpStream::connect(addr).await?;
  println!("queue flow connected");

  let _: HandshakeResponse = request_json(
    &mut stream,
    CMD_HANDSHAKE_REQUEST,
    CMD_HANDSHAKE_RESPONSE,
    &HandshakeRequest {
      client_id: "smoke-all-queue".to_owned(),
    },
  )
  .await?;

  let ping_nonce = 42_u64;
  let ping: PingResponse = request_json(
    &mut stream,
    CMD_PING_REQUEST,
    CMD_PING_RESPONSE,
    &PingRequest { nonce: ping_nonce },
  )
  .await?;
  if ping.nonce != ping_nonce {
    return Err(io::Error::other("queue flow ping nonce mismatch"));
  }

  let topic = format!("smoke-all-{}", unix_timestamp_ms());
  let _created: CreateTopicResponse = request_json(
    &mut stream,
    CMD_CREATE_TOPIC_REQUEST,
    CMD_CREATE_TOPIC_RESPONSE,
    &CreateTopicRequest {
      topic: topic.clone(),
      partitions: 1,
      retention_max_bytes: None,
    },
  )
  .await?;

  let listed_topics: ListTopicsResponse = request_json(
    &mut stream,
    CMD_LIST_TOPICS_REQUEST,
    CMD_LIST_TOPICS_RESPONSE,
    &serde_json::json!({}),
  )
  .await?;
  if !listed_topics
    .topics
    .iter()
    .any(|metadata| metadata.topic == topic)
  {
    return Err(io::Error::other("queue flow list_topics mismatch"));
  }

  let payload = b"smoke-all-queue-payload".to_vec();
  let _produce: ProduceResponse = request_json(
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

  let fetch: FetchResponse = request_json(
    &mut stream,
    CMD_FETCH_REQUEST,
    CMD_FETCH_RESPONSE,
    &FetchRequest {
      consumer: "smoke-all-consumer".to_owned(),
      topic: topic.clone(),
      partition: 0,
      offset: Some(0),
      max_records: 10,
    },
  )
  .await?;
  if fetch.records.is_empty() || fetch.records[0].payload != payload {
    return Err(io::Error::other("queue flow fetch payload mismatch"));
  }

  let _commit: CommitOffsetResponse = request_json(
    &mut stream,
    CMD_COMMIT_OFFSET_REQUEST,
    CMD_COMMIT_OFFSET_RESPONSE,
    &CommitOffsetRequest {
      consumer: "smoke-all-consumer".to_owned(),
      topic,
      partition: 0,
      next_offset: fetch.next_offset,
    },
  )
  .await?;

  println!("queue flow passed");
  Ok(())
}

async fn run_direct_flow(addr: &str) -> io::Result<()> {
  let mut stream = TcpStream::connect(addr).await?;
  println!("direct flow connected");

  let _: HandshakeResponse = request_json(
    &mut stream,
    CMD_HANDSHAKE_REQUEST,
    CMD_HANDSHAKE_RESPONSE,
    &HandshakeRequest {
      client_id: "smoke-all-direct".to_owned(),
    },
  )
  .await?;

  let sender = "smoke-all-alice";
  let recipient = format!("smoke-all-bob-{}", unix_timestamp_ms());
  let payload = b"smoke-all-direct-payload".to_vec();

  let _sent: SendDirectResponse = request_json(
    &mut stream,
    CMD_SEND_DIRECT_REQUEST,
    CMD_SEND_DIRECT_RESPONSE,
    &SendDirectRequest {
      sender: sender.to_owned(),
      recipient: recipient.clone(),
      conversation_id: None,
      payload: payload.clone(),
    },
  )
  .await?;

  let inbox: FetchInboxResponse = request_json(
    &mut stream,
    CMD_FETCH_INBOX_REQUEST,
    CMD_FETCH_INBOX_RESPONSE,
    &FetchInboxRequest {
      recipient: recipient.clone(),
      max_records: 10,
    },
  )
  .await?;
  if inbox.records.is_empty() {
    return Err(io::Error::other("direct flow returned zero inbox records"));
  }
  if inbox.records[0].sender != sender || inbox.records[0].payload != payload {
    return Err(io::Error::other("direct flow inbox payload mismatch"));
  }

  let ack: AckDirectResponse = request_json(
    &mut stream,
    CMD_ACK_DIRECT_REQUEST,
    CMD_ACK_DIRECT_RESPONSE,
    &AckDirectRequest {
      recipient,
      next_offset: inbox.next_offset,
    },
  )
  .await?;
  if ack.next_offset != inbox.next_offset {
    return Err(io::Error::other("direct flow ack next_offset mismatch"));
  }

  println!("direct flow passed");
  Ok(())
}
