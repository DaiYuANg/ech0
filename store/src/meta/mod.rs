use std::{
  fs,
  marker::PhantomData,
  path::{Path, PathBuf},
};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::{
  Codec, JsonCodec, Result,
  model::{BrokerState, LocalPartitionState, TopicConfig, TopicPartition},
  traits::{BrokerStateStore, LocalPartitionStateStore, OffsetStore, TopicCatalogStore},
};

const CONSUMER_OFFSETS: TableDefinition<&str, u64> = TableDefinition::new("consumer_offsets");
const TOPIC_CONFIGS: TableDefinition<&str, &[u8]> = TableDefinition::new("topic_configs");
const BROKER_STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("broker_state");
const LOCAL_PARTITION_STATES: TableDefinition<&str, &[u8]> =
  TableDefinition::new("local_partition_states");
const LOCAL_STATE_KEY: &str = "local";

pub trait MetadataStore:
  OffsetStore + TopicCatalogStore + BrokerStateStore + LocalPartitionStateStore
{
}

#[derive(Debug)]
pub struct RedbMetadataStore<
  TopicCodec = JsonCodec<TopicConfig>,
  BrokerCodec = JsonCodec<BrokerState>,
  PartitionCodec = JsonCodec<LocalPartitionState>,
> {
  db: Database,
  path: PathBuf,
  topic_codec: TopicCodec,
  broker_codec: BrokerCodec,
  partition_codec: PartitionCodec,
  _marker: PhantomData<(TopicConfig, BrokerState, LocalPartitionState)>,
}

impl
  RedbMetadataStore<JsonCodec<TopicConfig>, JsonCodec<BrokerState>, JsonCodec<LocalPartitionState>>
{
  pub fn create(path: impl AsRef<Path>) -> Result<Self> {
    Self::create_with_codecs(path, JsonCodec::new(), JsonCodec::new(), JsonCodec::new())
  }
}

impl<TopicCodec, BrokerCodec, PartitionCodec>
  RedbMetadataStore<TopicCodec, BrokerCodec, PartitionCodec>
where
  TopicCodec: Codec<TopicConfig>,
  BrokerCodec: Codec<BrokerState>,
  PartitionCodec: Codec<LocalPartitionState>,
{
  pub fn create_with_codecs(
    path: impl AsRef<Path>,
    topic_codec: TopicCodec,
    broker_codec: BrokerCodec,
    partition_codec: PartitionCodec,
  ) -> Result<Self> {
    let path = path.as_ref().to_path_buf();
    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }
    let db = if path.exists() {
      Database::open(&path)?
    } else {
      Database::create(&path)?
    };

    {
      let write_txn = db.begin_write()?;
      write_txn.open_table(CONSUMER_OFFSETS)?;
      write_txn.open_table(TOPIC_CONFIGS)?;
      write_txn.open_table(BROKER_STATE)?;
      write_txn.open_table(LOCAL_PARTITION_STATES)?;
      write_txn.commit()?;
    }

    Ok(Self {
      db,
      path,
      topic_codec,
      broker_codec,
      partition_codec,
      _marker: PhantomData,
    })
  }

  pub fn path(&self) -> &Path {
    &self.path
  }

  fn offset_key(consumer: &str, topic_partition: &TopicPartition) -> String {
    format!(
      "{}:{}:{}",
      consumer, topic_partition.topic, topic_partition.partition
    )
  }

  fn partition_state_key(topic_partition: &TopicPartition) -> String {
    format!("{}:{}", topic_partition.topic, topic_partition.partition)
  }
}

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

impl<TopicCodec, BrokerCodec, PartitionCodec> MetadataStore
  for RedbMetadataStore<TopicCodec, BrokerCodec, PartitionCodec>
where
  TopicCodec: Codec<TopicConfig>,
  BrokerCodec: Codec<BrokerState>,
  PartitionCodec: Codec<LocalPartitionState>,
{
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::model::{PartitionAvailability, PartitionState};
  use std::time::{SystemTime, UNIX_EPOCH};

  fn temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_nanos();
    std::env::temp_dir().join(format!("ech0-meta-{name}-{nanos}.redb"))
  }

  #[test]
  fn local_partition_states_round_trip_in_sorted_order() {
    let path = temp_path("local-partition-state");
    let store = RedbMetadataStore::create(path).unwrap();

    let second = LocalPartitionState {
      topic_partition: TopicPartition::new("orders", 1),
      availability: PartitionAvailability::Recovering,
      state: PartitionState {
        leader_epoch: 3,
        high_watermark: Some(8),
        last_appended_offset: Some(10),
      },
    };
    let first = LocalPartitionState::online(TopicPartition::new("orders", 0), Some(4));

    store.save_local_partition_state(&second).unwrap();
    store.save_local_partition_state(&first).unwrap();

    let listed = store.list_local_partition_states().unwrap();
    assert_eq!(listed, vec![first.clone(), second.clone()]);
    assert_eq!(
      store
        .load_local_partition_state(&TopicPartition::new("orders", 1))
        .unwrap(),
      Some(second)
    );
  }
}
