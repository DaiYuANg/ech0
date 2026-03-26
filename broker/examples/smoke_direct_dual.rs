use std::{env, io};

use protocol::{
  AckDirectRequest, AckDirectResponse, CMD_ACK_DIRECT_REQUEST, CMD_ACK_DIRECT_RESPONSE,
  CMD_FETCH_INBOX_REQUEST, CMD_FETCH_INBOX_RESPONSE, CMD_HANDSHAKE_REQUEST,
  CMD_HANDSHAKE_RESPONSE, CMD_SEND_DIRECT_REQUEST, CMD_SEND_DIRECT_RESPONSE, FetchInboxRequest,
  FetchInboxResponse, HandshakeRequest, HandshakeResponse, SendDirectRequest, SendDirectResponse,
};
use tokio::net::TcpStream;

#[path = "shared/mod.rs"]
mod common;
use common::{request_json, unix_timestamp_ms};

async fn connect_client(addr: &str, client_id: &str) -> io::Result<TcpStream> {
  let mut stream = TcpStream::connect(addr).await?;
  let handshake: HandshakeResponse = request_json(
    &mut stream,
    CMD_HANDSHAKE_REQUEST,
    CMD_HANDSHAKE_RESPONSE,
    &HandshakeRequest {
      client_id: client_id.to_owned(),
    },
  )
  .await?;
  println!(
    "handshake ok: client_id={} server_id={} protocol_version={}",
    client_id, handshake.server_id, handshake.protocol_version
  );
  Ok(stream)
}

async fn send_direct(
  stream: &mut TcpStream,
  sender: &str,
  recipient: &str,
  payload: Vec<u8>,
) -> io::Result<SendDirectResponse> {
  request_json(
    stream,
    CMD_SEND_DIRECT_REQUEST,
    CMD_SEND_DIRECT_RESPONSE,
    &SendDirectRequest {
      sender: sender.to_owned(),
      recipient: recipient.to_owned(),
      conversation_id: None,
      payload,
    },
  )
  .await
}

async fn fetch_and_ack(
  stream: &mut TcpStream,
  recipient: &str,
  expected_sender: &str,
  expected_payload: &[u8],
) -> io::Result<()> {
  let inbox: FetchInboxResponse = request_json(
    stream,
    CMD_FETCH_INBOX_REQUEST,
    CMD_FETCH_INBOX_RESPONSE,
    &FetchInboxRequest {
      recipient: recipient.to_owned(),
      max_records: 10,
    },
  )
  .await?;
  if inbox.records.is_empty() {
    return Err(io::Error::other(format!(
      "fetch_inbox returned zero records for recipient {recipient}"
    )));
  }

  let first = &inbox.records[0];
  if first.sender != expected_sender {
    return Err(io::Error::other(format!(
      "sender mismatch: expected {expected_sender}, got {}",
      first.sender
    )));
  }
  if first.payload != expected_payload {
    return Err(io::Error::other(format!(
      "payload mismatch for recipient {recipient}"
    )));
  }

  let ack: AckDirectResponse = request_json(
    stream,
    CMD_ACK_DIRECT_REQUEST,
    CMD_ACK_DIRECT_RESPONSE,
    &AckDirectRequest {
      recipient: recipient.to_owned(),
      next_offset: inbox.next_offset,
    },
  )
  .await?;
  if ack.next_offset != inbox.next_offset {
    return Err(io::Error::other(format!(
      "ack next_offset mismatch for recipient {recipient}"
    )));
  }

  let after_ack: FetchInboxResponse = request_json(
    stream,
    CMD_FETCH_INBOX_REQUEST,
    CMD_FETCH_INBOX_RESPONSE,
    &FetchInboxRequest {
      recipient: recipient.to_owned(),
      max_records: 10,
    },
  )
  .await?;
  if !after_ack.records.is_empty() {
    return Err(io::Error::other(format!(
      "recipient {recipient} still has unread records after ack"
    )));
  }

  println!(
    "fetch_inbox + ack_direct ok: recipient={} message_id={}",
    recipient, first.message_id
  );
  Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
  let addr = env::var("BROKER_ADDR").unwrap_or_else(|_| "127.0.0.1:9090".to_owned());
  let suffix = unix_timestamp_ms();
  let alice = format!("smoke-alice-{suffix}");
  let bob = format!("smoke-bob-{suffix}");

  let mut alice_stream = connect_client(&addr, "smoke-direct-dual-alice").await?;
  let mut bob_stream = connect_client(&addr, "smoke-direct-dual-bob").await?;
  println!("connected two direct clients to broker at {addr}");

  let alice_payload = b"hello-from-alice".to_vec();
  let alice_sent = send_direct(&mut alice_stream, &alice, &bob, alice_payload.clone()).await?;
  println!(
    "alice -> bob send_direct ok: message_id={} conversation_id={}",
    alice_sent.message_id, alice_sent.conversation_id
  );
  fetch_and_ack(&mut bob_stream, &bob, &alice, &alice_payload).await?;

  let bob_payload = b"hello-from-bob".to_vec();
  let bob_sent = send_direct(&mut bob_stream, &bob, &alice, bob_payload.clone()).await?;
  println!(
    "bob -> alice send_direct ok: message_id={} conversation_id={}",
    bob_sent.message_id, bob_sent.conversation_id
  );
  fetch_and_ack(&mut alice_stream, &alice, &bob, &bob_payload).await?;

  println!("dual direct smoke test passed: alice={} bob={}", alice, bob);
  Ok(())
}
