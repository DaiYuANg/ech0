use store::{
  MessageLogStore, OffsetStore, PollResult, Record, Result, TopicConfig, TopicPartition,
};

#[derive(Debug)]
pub struct QueueRuntime<L, O> {
  log_store: L,
  offset_store: O,
}

impl<L, O> QueueRuntime<L, O>
where
  L: MessageLogStore,
  O: OffsetStore,
{
  pub fn new(log_store: L, offset_store: O) -> Self {
    Self {
      log_store,
      offset_store,
    }
  }

  pub fn create_topic(&self, topic: TopicConfig) -> Result<()> {
    self.log_store.create_topic(topic)
  }

  pub fn publish(&self, topic: &str, partition: u32, payload: &[u8]) -> Result<Record> {
    let topic_partition = TopicPartition::new(topic, partition);
    self.log_store.append(&topic_partition, payload)
  }

  pub fn poll(
    &self,
    consumer: &str,
    topic: &str,
    partition: u32,
    max_records: usize,
  ) -> Result<PollResult> {
    let topic_partition = TopicPartition::new(topic, partition);
    let next_offset = self
      .offset_store
      .load_consumer_offset(consumer, &topic_partition)?
      .unwrap_or(0);
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

  pub fn ack(&self, consumer: &str, topic: &str, partition: u32, next_offset: u64) -> Result<()> {
    let topic_partition = TopicPartition::new(topic, partition);
    self
      .offset_store
      .save_consumer_offset(consumer, &topic_partition, next_offset)
  }

  pub fn stores(&self) -> (&L, &O) {
    (&self.log_store, &self.offset_store)
  }
}
