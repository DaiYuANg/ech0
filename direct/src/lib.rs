use queue::QueueRuntime;
use store::{MessageLogStore, OffsetStore, PollResult, Record, Result, TopicConfig};

#[derive(Debug)]
pub struct DirectBroker<L, O> {
  runtime: QueueRuntime<L, O>,
}

impl<L, O> DirectBroker<L, O>
where
  L: MessageLogStore,
  O: OffsetStore,
{
  pub fn new(log_store: L, offset_store: O) -> Self {
    Self {
      runtime: QueueRuntime::new(log_store, offset_store),
    }
  }

  pub fn create_topic(&self, topic: TopicConfig) -> Result<()> {
    self.runtime.create_topic(topic)
  }

  pub fn publish(&self, topic: &str, partition: u32, payload: &[u8]) -> Result<Record> {
    self.runtime.publish(topic, partition, payload)
  }

  pub fn poll(
    &self,
    consumer: &str,
    topic: &str,
    partition: u32,
    max_records: usize,
  ) -> Result<PollResult> {
    self.runtime.poll(consumer, topic, partition, max_records)
  }

  pub fn ack(&self, consumer: &str, topic: &str, partition: u32, next_offset: u64) -> Result<()> {
    self.runtime.ack(consumer, topic, partition, next_offset)
  }
}
