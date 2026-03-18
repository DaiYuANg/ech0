use redb::{ReadableDatabase, ReadableTable};

use super::*;

impl<TopicCodec, BrokerCodec, PartitionCodec> OffsetStore
  for RedbMetadataStore<TopicCodec, BrokerCodec, PartitionCodec>
where
  TopicCodec: Codec<TopicConfig>,
  BrokerCodec: Codec<BrokerState>,
  PartitionCodec: Codec<LocalPartitionState>,
{
  fn load_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
  ) -> Result<Option<u64>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(CONSUMER_OFFSETS)?;
    let key = Self::offset_key(consumer, topic_partition);
    Ok(table.get(key.as_str())?.map(|value| value.value()))
  }

  fn save_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> Result<()> {
    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(CONSUMER_OFFSETS)?;
      let key = Self::offset_key(consumer, topic_partition);
      table.insert(key.as_str(), next_offset)?;
    }
    write_txn.commit()?;
    Ok(())
  }
}

impl<TopicCodec, BrokerCodec, PartitionCodec> TopicCatalogStore
  for RedbMetadataStore<TopicCodec, BrokerCodec, PartitionCodec>
where
  TopicCodec: Codec<TopicConfig>,
  BrokerCodec: Codec<BrokerState>,
  PartitionCodec: Codec<LocalPartitionState>,
{
  fn save_topic_config(&self, topic: &TopicConfig) -> Result<()> {
    let bytes = self.topic_codec.encode(topic)?;
    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(TOPIC_CONFIGS)?;
      table.insert(topic.name.as_str(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
  }

  fn load_topic_config(&self, topic: &str) -> Result<Option<TopicConfig>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(TOPIC_CONFIGS)?;
    let Some(value) = table.get(topic)? else {
      return Ok(None);
    };
    self.topic_codec.decode(value.value()).map(Some)
  }

  fn list_topics(&self) -> Result<Vec<TopicConfig>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(TOPIC_CONFIGS)?;
    let mut topics = Vec::new();
    for entry in table.iter()? {
      let (_key, value) = entry?;
      topics.push(self.topic_codec.decode(value.value())?);
    }
    topics.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(topics)
  }
}

impl<TopicCodec, BrokerCodec, PartitionCodec> BrokerStateStore
  for RedbMetadataStore<TopicCodec, BrokerCodec, PartitionCodec>
where
  TopicCodec: Codec<TopicConfig>,
  BrokerCodec: Codec<BrokerState>,
  PartitionCodec: Codec<LocalPartitionState>,
{
  fn save_broker_state(&self, state: &BrokerState) -> Result<()> {
    let bytes = self.broker_codec.encode(state)?;
    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(BROKER_STATE)?;
      table.insert(LOCAL_STATE_KEY, bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
  }

  fn load_broker_state(&self) -> Result<Option<BrokerState>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(BROKER_STATE)?;
    let Some(value) = table.get(LOCAL_STATE_KEY)? else {
      return Ok(None);
    };
    self.broker_codec.decode(value.value()).map(Some)
  }
}

impl<TopicCodec, BrokerCodec, PartitionCodec> LocalPartitionStateStore
  for RedbMetadataStore<TopicCodec, BrokerCodec, PartitionCodec>
where
  TopicCodec: Codec<TopicConfig>,
  BrokerCodec: Codec<BrokerState>,
  PartitionCodec: Codec<LocalPartitionState>,
{
  fn save_local_partition_state(&self, state: &LocalPartitionState) -> Result<()> {
    let bytes = self.partition_codec.encode(state)?;
    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(LOCAL_PARTITION_STATES)?;
      let key = Self::partition_state_key(&state.topic_partition);
      table.insert(key.as_str(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
  }

  fn load_local_partition_state(
    &self,
    topic_partition: &TopicPartition,
  ) -> Result<Option<LocalPartitionState>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(LOCAL_PARTITION_STATES)?;
    let key = Self::partition_state_key(topic_partition);
    let Some(value) = table.get(key.as_str())? else {
      return Ok(None);
    };
    self.partition_codec.decode(value.value()).map(Some)
  }

  fn list_local_partition_states(&self) -> Result<Vec<LocalPartitionState>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(LOCAL_PARTITION_STATES)?;
    let mut states = Vec::new();
    for entry in table.iter()? {
      let (_key, value) = entry?;
      states.push(self.partition_codec.decode(value.value())?);
    }
    states.sort_by(|a, b| {
      a.topic_partition.topic.cmp(&b.topic_partition.topic).then(
        a.topic_partition
          .partition
          .cmp(&b.topic_partition.partition),
      )
    });
    Ok(states)
  }
}
