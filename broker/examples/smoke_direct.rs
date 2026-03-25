use std::{env, io};

use protocol::{
  AckDirectRequest, AckDirectResponse, CMD_ACK_DIRECT_REQUEST, CMD_ACK_DIRECT_RESPONSE,
  CMD_FETCH_INBOX_REQUEST, CMD_FETCH_INBOX_RESPONSE, CMD_HANDSHAKE_REQUEST, CMD_HANDSHAKE_RESPONSE,
  CMD_SEND_DIRECT_REQUEST, CMD_SEND_DIRECT_RESPONSE, FetchInboxRequest, FetchInboxResponse,
  HandshakeRequest, HandshakeResponse, SendDirectRequest, SendDirectResponse,
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
      client_id: "smoke-direct".to_owned(),
    },
  )
  .await?;
  println!(
    "handshake ok: server_id={} protocol_version={}",
    handshake.server_id, handshake.protocol_version
  );

  let sender = "smoke-alice";
  let recipient = format!("smoke-bob-{}", unix_timestamp_ms());
  let payload = b"direct-smoke-payload".to_vec();

  let sent: SendDirectResponse = request_json(
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
  println!(
    "send_direct ok: message_id={} conversation_id={}",
    sent.message_id, sent.conversation_id
  );

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
    return Err(io::Error::other("fetch_inbox returned zero records"));
  }

  let first = &inbox.records[0];
  if first.sender != sender {
    return Err(io::Error::other("sender mismatch in inbox record"));
  }
  if first.payload != payload {
    return Err(io::Error::other("payload mismatch in inbox record"));
  }
  println!(
    "fetch_inbox ok: records={} next_offset={}",
    inbox.records.len(),
    inbox.next_offset
  );

  let ack: AckDirectResponse = request_json(
    &mut stream,
    CMD_ACK_DIRECT_REQUEST,
    CMD_ACK_DIRECT_RESPONSE,
    &AckDirectRequest {
      recipient: recipient.clone(),
      next_offset: inbox.next_offset,
    },
  )
  .await?;
  if ack.next_offset != inbox.next_offset {
    return Err(io::Error::other("ack next_offset mismatch"));
  }
  println!(
    "ack_direct ok: recipient={} next_offset={}",
    ack.recipient, ack.next_offset
  );

  println!("direct smoke test passed for recipient {recipient}");
  Ok(())
}
