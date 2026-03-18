use std::{
  fmt::Write,
  sync::atomic::{AtomicU64, Ordering},
  time::{SystemTime, UNIX_EPOCH},
};

use queue::QueueRuntime;
use serde::{Deserialize, Serialize};
use store::{
  MessageLogStore, OffsetStore, Record, Result, StoreError, TopicCatalogStore, TopicConfig,
};

static DIRECT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub const INTERNAL_INBOX_TOPIC_PREFIX: &str = "__direct.inbox.";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectMessage {
  pub message_id: String,
  pub conversation_id: String,
  pub sender: String,
  pub recipient: String,
  pub timestamp_ms: u64,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendDirectResult {
  pub message_id: String,
  pub conversation_id: String,
  pub offset: u64,
  pub next_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxMessage {
  pub offset: u64,
  pub message: DirectMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchInboxResult {
  pub recipient: String,
  pub records: Vec<InboxMessage>,
  pub next_offset: u64,
  pub high_watermark: Option<u64>,
}

#[derive(Debug)]
pub struct DirectRuntime<L, M> {
  runtime: QueueRuntime<L, M>,
}

impl<L, M> DirectRuntime<L, M>
where
  L: MessageLogStore,
  M: OffsetStore + TopicCatalogStore,
{
  pub fn new(log_store: L, meta_store: M) -> Self {
    Self {
      runtime: QueueRuntime::new(log_store, meta_store),
    }
  }

  pub fn send(
    &self,
    sender: &str,
    recipient: &str,
    conversation_id: Option<String>,
    payload: Vec<u8>,
  ) -> Result<SendDirectResult> {
    let topic = inbox_topic(recipient);
    self.ensure_inbox(recipient)?;

    let message = DirectMessage {
      message_id: next_message_id(),
      conversation_id: conversation_id
        .unwrap_or_else(|| default_conversation_id(sender, recipient)),
      sender: sender.to_owned(),
      recipient: recipient.to_owned(),
      timestamp_ms: now_ms(),
      payload,
    };
    let encoded = serde_json::to_vec(&message)
      .map_err(|err| StoreError::Codec(format!("failed to encode direct message: {err}")))?;
    let record = self.runtime.publish(&topic, 0, &encoded)?;

    Ok(SendDirectResult {
      message_id: message.message_id,
      conversation_id: message.conversation_id,
      offset: record.offset,
      next_offset: record.offset + 1,
    })
  }

  pub fn fetch_inbox(&self, recipient: &str, max_records: usize) -> Result<FetchInboxResult> {
    let topic = inbox_topic(recipient);
    let polled = self
      .runtime
      .fetch(&consumer_name(recipient), &topic, 0, None, max_records)?;

    Ok(FetchInboxResult {
      recipient: recipient.to_owned(),
      records: polled
        .records
        .into_iter()
        .map(decode_inbox_record)
        .collect::<Result<Vec<_>>>()?,
      next_offset: polled.next_offset,
      high_watermark: polled.high_watermark,
    })
  }

  pub fn ack_inbox(&self, recipient: &str, next_offset: u64) -> Result<()> {
    self.runtime.ack(
      &consumer_name(recipient),
      &inbox_topic(recipient),
      0,
      next_offset,
    )
  }

  pub fn stores(&self) -> (&L, &M) {
    self.runtime.stores()
  }

  fn ensure_inbox(&self, recipient: &str) -> Result<()> {
    let topic_name = inbox_topic(recipient);
    let topic = TopicConfig::new(topic_name.clone());
    if self.runtime.stores().0.topic_exists(&topic_name)? {
      self.runtime.stores().1.save_topic_config(&topic)?;
      return Ok(());
    }

    match self.runtime.create_topic(topic.clone()) {
      Ok(()) => Ok(()),
      Err(StoreError::TopicAlreadyExists(_)) => self.runtime.stores().1.save_topic_config(&topic),
      Err(err) => Err(err),
    }
  }
}

#[derive(Debug)]
pub struct DirectBroker<L, M> {
  runtime: DirectRuntime<L, M>,
}

impl<L, M> DirectBroker<L, M>
where
  L: MessageLogStore,
  M: OffsetStore + TopicCatalogStore,
{
  pub fn new(log_store: L, meta_store: M) -> Self {
    Self {
      runtime: DirectRuntime::new(log_store, meta_store),
    }
  }

  pub fn send(
    &self,
    sender: &str,
    recipient: &str,
    conversation_id: Option<String>,
    payload: Vec<u8>,
  ) -> Result<SendDirectResult> {
    self
      .runtime
      .send(sender, recipient, conversation_id, payload)
  }

  pub fn fetch_inbox(&self, recipient: &str, max_records: usize) -> Result<FetchInboxResult> {
    self.runtime.fetch_inbox(recipient, max_records)
  }

  pub fn ack_inbox(&self, recipient: &str, next_offset: u64) -> Result<()> {
    self.runtime.ack_inbox(recipient, next_offset)
  }

  pub fn stores(&self) -> (&L, &M) {
    self.runtime.stores()
  }
}

pub fn is_internal_topic_name(topic: &str) -> bool {
  topic.starts_with(INTERNAL_INBOX_TOPIC_PREFIX)
}

fn decode_inbox_record(record: Record) -> Result<InboxMessage> {
  let message = serde_json::from_slice::<DirectMessage>(record.payload.as_ref())
    .map_err(|err| StoreError::Codec(format!("failed to decode direct message: {err}")))?;
  Ok(InboxMessage {
    offset: record.offset,
    message,
  })
}

fn inbox_topic(recipient: &str) -> String {
  format!("{INTERNAL_INBOX_TOPIC_PREFIX}{}", hex_encode(recipient))
}

fn consumer_name(recipient: &str) -> String {
  format!("direct:{}", hex_encode(recipient))
}

fn default_conversation_id(sender: &str, recipient: &str) -> String {
  let sender = hex_encode(sender);
  let recipient = hex_encode(recipient);
  if sender <= recipient {
    format!("dm:{sender}:{recipient}")
  } else {
    format!("dm:{recipient}:{sender}")
  }
}

fn next_message_id() -> String {
  let seq = DIRECT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
  format!("dm-{:020}-{:08}", now_ms(), seq)
}

fn now_ms() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64
}

fn hex_encode(value: &str) -> String {
  let mut encoded = String::with_capacity(value.len() * 2);
  for byte in value.as_bytes() {
    let _ = write!(&mut encoded, "{byte:02x}");
  }
  encoded
}

#[cfg(test)]
mod tests {
  use store::{InMemoryStore, TopicCatalogStore};

  use super::{DirectRuntime, INTERNAL_INBOX_TOPIC_PREFIX};

  #[test]
  fn send_fetch_and_ack_flow_works() {
    let log = InMemoryStore::new();
    let meta = InMemoryStore::new();
    let runtime = DirectRuntime::new(log, meta);

    let sent = runtime
      .send("alice", "bob", None, b"hello".to_vec())
      .unwrap();
    assert_eq!(sent.offset, 0);
    assert!(sent.conversation_id.starts_with("dm:"));

    let fetched = runtime.fetch_inbox("bob", 10).unwrap();
    assert_eq!(fetched.recipient, "bob");
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].message.sender, "alice");
    assert_eq!(fetched.records[0].message.recipient, "bob");
    assert_eq!(fetched.records[0].message.payload, b"hello".to_vec());

    runtime.ack_inbox("bob", fetched.next_offset).unwrap();
    let fetched_after_ack = runtime.fetch_inbox("bob", 10).unwrap();
    assert!(fetched_after_ack.records.is_empty());
  }

  #[test]
  fn inbox_topics_are_marked_internal() {
    let log = InMemoryStore::new();
    let meta = InMemoryStore::new();
    let runtime = DirectRuntime::new(log, meta);

    runtime
      .send("alice", "bob", None, b"hello".to_vec())
      .unwrap();

    let topics = runtime.stores().1.list_topics().unwrap();
    assert_eq!(topics.len(), 1);
    assert!(topics[0].name.starts_with(INTERNAL_INBOX_TOPIC_PREFIX));
  }
}
