use store::{
  MessageLogStore, OffsetStore, Result, StoreError, TopicCatalogStore, TopicConfig,
  TopicPartition,
};

#[derive(Debug, Clone)]
pub struct BrokerIdentity {
  pub node_id: u64,
  pub cluster_name: String,
}

#[derive(Debug, Clone)]
pub struct BrokerService<L, M> {
  identity: BrokerIdentity,
  log: L,
  meta: M,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRecord {
  pub offset: u64,
  pub timestamp_ms: u64,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResult {
  pub topic: String,
  pub partition: u32,
  pub records: Vec<FetchedRecord>,
  pub next_offset: u64,
  pub high_watermark: Option<u64>,
}

impl<L, M> BrokerService<L, M> {
  pub fn new(identity: BrokerIdentity, log: L, meta: M) -> Self {
    Self {
      identity,
      log,
      meta,
    }
  }

  pub fn identity(&self) -> &BrokerIdentity {
    &self.identity
  }
}

impl<L, M> BrokerService<L, M>
where
  L: MessageLogStore,
  M: OffsetStore + TopicCatalogStore,
{
  pub fn create_topic(&self, topic: impl Into<String>, partitions: u32) -> Result<TopicConfig> {
    let topic = topic.into();
    if partitions == 0 {
      return Err(StoreError::Codec("partitions must be greater than zero".to_owned()));
    }

    let mut config = TopicConfig::new(topic.clone());
    config.partitions = partitions;
    self.log.create_topic(config.clone())?;
    self.meta.save_topic_config(&config)?;
    Ok(config)
  }

  pub fn publish(
    &self,
    topic: impl Into<String>,
    partition: u32,
    payload: Vec<u8>,
  ) -> Result<(u64, u64)> {
    let tp = TopicPartition::new(topic, partition);
    let record = self.log.append(&tp, &payload)?;
    Ok((record.offset, record.offset + 1))
  }

  pub fn fetch(
    &self,
    consumer: &str,
    topic: impl Into<String>,
    partition: u32,
    offset: Option<u64>,
    max_records: usize,
  ) -> Result<FetchResult> {
    let topic = topic.into();
    let tp = TopicPartition::new(topic.clone(), partition);
    let offset = match offset {
      Some(offset) => offset,
      None => self.meta.load_consumer_offset(consumer, &tp)?.unwrap_or(0),
    };
    let records = self.log.read_from(&tp, offset, max_records)?;
    let next_offset = records
      .last()
      .map(|record| record.offset + 1)
      .unwrap_or(offset);
    let high_watermark = self.log.last_offset(&tp)?;

    Ok(FetchResult {
      topic,
      partition,
      records: records
        .into_iter()
        .map(|record| FetchedRecord {
          offset: record.offset,
          timestamp_ms: record.timestamp_ms,
          payload: record.payload.to_vec(),
        })
        .collect(),
      next_offset,
      high_watermark,
    })
  }

  pub fn commit_offset(
    &self,
    consumer: &str,
    topic: impl Into<String>,
    partition: u32,
    next_offset: u64,
  ) -> Result<()> {
    let tp = TopicPartition::new(topic, partition);
    self.meta.save_consumer_offset(consumer, &tp, next_offset)
  }

  pub fn list_topics(&self) -> Result<Vec<TopicConfig>> {
    self.meta.list_topics()
  }
}

#[cfg(test)]
mod tests {
  use store::{InMemoryStore, TopicCatalogStore};

  use super::{BrokerIdentity, BrokerService};

  #[test]
  fn fetch_uses_committed_offset_when_request_offset_missing() {
    let log = InMemoryStore::new();
    let meta = InMemoryStore::new();
    let service = BrokerService::new(
      BrokerIdentity {
        node_id: 1,
        cluster_name: "test".to_owned(),
      },
      log,
      meta,
    );

    service.create_topic("orders", 1).unwrap();
    service.publish("orders", 0, b"a".to_vec()).unwrap();
    service.publish("orders", 0, b"b".to_vec()).unwrap();
    service.commit_offset("c1", "orders", 0, 1).unwrap();

    let fetched = service.fetch("c1", "orders", 0, None, 10).unwrap();

    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].offset, 1);
    assert_eq!(fetched.next_offset, 2);
  }

  #[test]
  fn create_topic_is_visible_in_catalog() {
    let log = InMemoryStore::new();
    let meta = InMemoryStore::new();
    let service = BrokerService::new(
      BrokerIdentity {
        node_id: 1,
        cluster_name: "test".to_owned(),
      },
      log,
      meta,
    );

    service.create_topic("orders", 3).unwrap();

    let topics = service.list_topics().unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "orders");
    assert_eq!(topics[0].partitions, 3);
  }
}
