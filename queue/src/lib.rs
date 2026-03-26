use store::{
  MessageLogStore, OffsetStore, PollResult, Record, RecordAppend, Result, TopicCatalogStore,
  TopicConfig, TopicPartition,
};

#[derive(Debug)]
pub struct QueueRuntime<L, M> {
  log_store: L,
  meta_store: M,
}

impl<L, M> QueueRuntime<L, M>
where
  L: MessageLogStore,
  M: OffsetStore + TopicCatalogStore,
{
  pub fn new(log_store: L, meta_store: M) -> Self {
    Self {
      log_store,
      meta_store,
    }
  }

  pub fn create_topic(&self, topic: TopicConfig) -> Result<()> {
    self.log_store.create_topic(topic.clone())?;
    self.meta_store.save_topic_config(&topic)
  }

  pub fn publish(&self, topic: &str, partition: u32, payload: &[u8]) -> Result<Record> {
    let topic_partition = TopicPartition::new(topic, partition);
    self.log_store.append(&topic_partition, payload)
  }

  pub fn publish_record(
    &self,
    topic: &str,
    partition: u32,
    record: RecordAppend,
  ) -> Result<Record> {
    let topic_partition = TopicPartition::new(topic, partition);
    self.log_store.append_record(&topic_partition, record)
  }

  pub fn publish_batch(
    &self,
    topic: &str,
    partition: u32,
    payloads: &[Vec<u8>],
  ) -> Result<Vec<Record>> {
    let topic_partition = TopicPartition::new(topic, partition);
    self.log_store.append_batch(&topic_partition, payloads)
  }

  /// Fetch records from an inclusive start offset.
  ///
  /// `offset` is interpreted as "next offset to read". When omitted, the committed
  /// consumer cursor is used. Returned `next_offset` follows:
  /// - records returned: `last_record.offset + 1`
  /// - no records returned: unchanged request/committed offset
  ///
  /// `high_watermark` is the largest committed and visible offset (inclusive).
  pub fn fetch(
    &self,
    consumer: &str,
    topic: &str,
    partition: u32,
    offset: Option<u64>,
    max_records: usize,
  ) -> Result<PollResult> {
    let topic_partition = TopicPartition::new(topic, partition);
    let next_offset = match offset {
      Some(offset) => offset,
      None => self
        .meta_store
        .load_consumer_offset(consumer, &topic_partition)?
        .unwrap_or(0),
    };
    let records = self
      .log_store
      .read_from(&topic_partition, next_offset, max_records)?;
    let high_watermark = self.log_store.last_offset(&topic_partition)?;
    let computed_next_offset = records
      .last()
      .map(|record| record.offset + 1)
      .unwrap_or(next_offset);

    Ok(PollResult {
      records,
      next_offset: computed_next_offset,
      high_watermark,
    })
  }

  pub fn poll(
    &self,
    consumer: &str,
    topic: &str,
    partition: u32,
    max_records: usize,
  ) -> Result<PollResult> {
    self.fetch(consumer, topic, partition, None, max_records)
  }

  /// Persist consumer cursor as "next offset to consume".
  pub fn ack(&self, consumer: &str, topic: &str, partition: u32, next_offset: u64) -> Result<()> {
    let topic_partition = TopicPartition::new(topic, partition);
    self
      .meta_store
      .save_consumer_offset(consumer, &topic_partition, next_offset)
  }

  pub fn list_topics(&self) -> Result<Vec<TopicConfig>> {
    self.meta_store.list_topics()
  }

  pub fn stores(&self) -> (&L, &M) {
    (&self.log_store, &self.meta_store)
  }
}

#[cfg(test)]
mod tests {
  use super::QueueRuntime;
  use store::{InMemoryStore, TopicConfig};

  fn build_runtime() -> QueueRuntime<InMemoryStore, InMemoryStore> {
    QueueRuntime::new(InMemoryStore::new(), InMemoryStore::new())
  }

  #[test]
  fn create_topic_is_listed_from_catalog() {
    let runtime = build_runtime();
    runtime.create_topic(TopicConfig::new("orders")).unwrap();

    let topics = runtime.list_topics().unwrap();
    assert!(topics.iter().any(|topic| topic.name == "orders"));
  }

  #[test]
  fn fetch_uses_explicit_offset_over_committed_offset() {
    let runtime = build_runtime();
    runtime.create_topic(TopicConfig::new("orders")).unwrap();
    runtime.publish("orders", 0, b"m1").unwrap();
    runtime.publish("orders", 0, b"m2").unwrap();
    runtime.ack("c1", "orders", 0, 2).unwrap();

    let result = runtime.fetch("c1", "orders", 0, Some(0), 10).unwrap();
    assert_eq!(result.records.len(), 2);
    assert_eq!(result.records[0].payload, b"m1".to_vec());
  }

  #[test]
  fn poll_uses_committed_offset_when_present() {
    let runtime = build_runtime();
    runtime.create_topic(TopicConfig::new("orders")).unwrap();
    runtime.publish("orders", 0, b"m1").unwrap();
    runtime.publish("orders", 0, b"m2").unwrap();
    runtime.ack("c1", "orders", 0, 1).unwrap();

    let result = runtime.poll("c1", "orders", 0, 10).unwrap();
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].payload, b"m2".to_vec());
    assert_eq!(result.next_offset, 2);
  }

  #[test]
  fn publish_to_missing_topic_returns_error() {
    let runtime = build_runtime();
    let err = runtime.publish("missing", 0, b"m1").unwrap_err();
    assert!(matches!(err, store::StoreError::PartitionNotFound { .. }));
  }
}
