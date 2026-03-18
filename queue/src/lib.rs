use store::{
  MessageLogStore, OffsetStore, PollResult, Record, Result, TopicCatalogStore, TopicConfig,
  TopicPartition,
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
