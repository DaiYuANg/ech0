mod catalog;
mod consensus;
mod group;
#[cfg(test)]
mod tests;

use std::{
  fs,
  marker::PhantomData,
  path::{Path, PathBuf},
};

use redb::{Database, TableDefinition};

use crate::{
  Codec, JsonCodec, Result,
  model::{BrokerState, LocalPartitionState, TopicConfig, TopicPartition},
  traits::{
    BrokerStateStore, ConsensusLogStore, ConsensusMetadataStore, ConsumerGroupStore,
    LocalPartitionStateStore, OffsetStore, TopicCatalogStore,
  },
};

const CONSUMER_OFFSETS: TableDefinition<&str, u64> = TableDefinition::new("consumer_offsets");
const TOPIC_CONFIGS: TableDefinition<&str, &[u8]> = TableDefinition::new("topic_configs");
const BROKER_STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("broker_state");
const LOCAL_PARTITION_STATES: TableDefinition<&str, &[u8]> =
  TableDefinition::new("local_partition_states");
const CONSUMER_GROUP_MEMBERS: TableDefinition<&str, &[u8]> =
  TableDefinition::new("consumer_group_members");
const CONSUMER_GROUP_ASSIGNMENTS: TableDefinition<&str, &[u8]> =
  TableDefinition::new("consumer_group_assignments");
const CONSENSUS_METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("consensus_metadata");
const CONSENSUS_LOGS: TableDefinition<&str, &[u8]> = TableDefinition::new("consensus_logs");
const LOCAL_STATE_KEY: &str = "local";
const CONSENSUS_PURGED_INDEX_KEY: &str = "purged_index";

pub trait MetadataStore:
  OffsetStore
  + TopicCatalogStore
  + BrokerStateStore
  + LocalPartitionStateStore
  + ConsumerGroupStore
  + ConsensusLogStore
  + ConsensusMetadataStore
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
      write_txn.open_table(CONSUMER_GROUP_MEMBERS)?;
      write_txn.open_table(CONSUMER_GROUP_ASSIGNMENTS)?;
      write_txn.open_table(CONSENSUS_METADATA)?;
      write_txn.open_table(CONSENSUS_LOGS)?;
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

  fn consumer_group_member_key(group: &str, member_id: &str) -> String {
    format!("{group}\u{1f}{member_id}")
  }

  fn parse_consumer_group_member_key(key: &str) -> Option<(String, String)> {
    let (group, member_id) = key.split_once('\u{1f}')?;
    Some((group.to_owned(), member_id.to_owned()))
  }

  fn consumer_group_assignment_key(group: &str) -> String {
    group.to_owned()
  }

  fn consensus_metadata_key(group: &str, key: &str) -> String {
    format!("{group}\u{1f}{key}")
  }

  fn consensus_log_prefix(group: &str) -> String {
    format!("{group}\u{1f}")
  }

  fn consensus_log_key(group: &str, index: u64) -> String {
    format!("{}{index:020}", Self::consensus_log_prefix(group))
  }

  fn parse_consensus_log_index(group: &str, key: &str) -> Result<Option<u64>> {
    let Some(suffix) = key.strip_prefix(Self::consensus_log_prefix(group).as_str()) else {
      return Ok(None);
    };
    suffix.parse::<u64>().map(Some).map_err(|err| {
      crate::StoreError::Corruption(format!(
        "invalid consensus log key {key:?}: failed to parse index: {err}"
      ))
    })
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
