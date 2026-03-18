use redb::{ReadableDatabase, ReadableTable};

use super::*;

impl<TopicCodec, BrokerCodec, PartitionCodec> ConsensusMetadataStore
  for RedbMetadataStore<TopicCodec, BrokerCodec, PartitionCodec>
where
  TopicCodec: Codec<TopicConfig>,
  BrokerCodec: Codec<BrokerState>,
  PartitionCodec: Codec<LocalPartitionState>,
{
  fn save_consensus_value(&self, group: &str, key: &str, payload: &[u8]) -> Result<()> {
    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(CONSENSUS_METADATA)?;
      let storage_key = Self::consensus_metadata_key(group, key);
      table.insert(storage_key.as_str(), payload)?;
    }
    write_txn.commit()?;
    Ok(())
  }

  fn load_consensus_value(&self, group: &str, key: &str) -> Result<Option<Vec<u8>>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(CONSENSUS_METADATA)?;
    let storage_key = Self::consensus_metadata_key(group, key);
    Ok(
      table
        .get(storage_key.as_str())?
        .map(|value| value.value().to_vec()),
    )
  }
}

impl<TopicCodec, BrokerCodec, PartitionCodec> ConsensusLogStore
  for RedbMetadataStore<TopicCodec, BrokerCodec, PartitionCodec>
where
  TopicCodec: Codec<TopicConfig>,
  BrokerCodec: Codec<BrokerState>,
  PartitionCodec: Codec<LocalPartitionState>,
{
  fn append_consensus_entry(&self, group: &str, index: u64, payload: &[u8]) -> Result<()> {
    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(CONSENSUS_LOGS)?;
      let storage_key = Self::consensus_log_key(group, index);
      table.insert(storage_key.as_str(), payload)?;
    }
    write_txn.commit()?;
    Ok(())
  }

  fn load_consensus_entry(&self, group: &str, index: u64) -> Result<Option<Vec<u8>>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(CONSENSUS_LOGS)?;
    let storage_key = Self::consensus_log_key(group, index);
    Ok(
      table
        .get(storage_key.as_str())?
        .map(|value| value.value().to_vec()),
    )
  }

  fn load_consensus_entries(
    &self,
    group: &str,
    start: u64,
    end: Option<u64>,
  ) -> Result<Vec<(u64, Vec<u8>)>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(CONSENSUS_LOGS)?;
    let mut entries = Vec::new();

    for entry in table.iter()? {
      let (key, value) = entry?;
      let Some(index) = Self::parse_consensus_log_index(group, key.value())? else {
        continue;
      };
      if index < start {
        continue;
      }
      if end.map(|limit| index >= limit).unwrap_or(false) {
        continue;
      }
      entries.push((index, value.value().to_vec()));
    }

    entries.sort_by_key(|(index, _)| *index);
    Ok(entries)
  }

  fn load_last_consensus_index(&self, group: &str) -> Result<Option<u64>> {
    Ok(
      self
        .load_consensus_entries(group, 0, None)?
        .into_iter()
        .map(|(index, _)| index)
        .last(),
    )
  }

  fn truncate_consensus_from(&self, group: &str, index: u64) -> Result<()> {
    let keys = self
      .load_consensus_entries(group, index, None)?
      .into_iter()
      .map(|(entry_index, _)| Self::consensus_log_key(group, entry_index))
      .collect::<Vec<_>>();

    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(CONSENSUS_LOGS)?;
      for key in &keys {
        table.remove(key.as_str())?;
      }
    }
    write_txn.commit()?;
    Ok(())
  }

  fn purge_consensus_to(&self, group: &str, index: u64) -> Result<()> {
    let end = index.checked_add(1);
    let keys = self
      .load_consensus_entries(group, 0, end)?
      .into_iter()
      .map(|(entry_index, _)| Self::consensus_log_key(group, entry_index))
      .collect::<Vec<_>>();

    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(CONSENSUS_LOGS)?;
      for key in &keys {
        table.remove(key.as_str())?;
      }
    }
    {
      let mut table = write_txn.open_table(CONSENSUS_METADATA)?;
      let metadata_key = Self::consensus_metadata_key(group, CONSENSUS_PURGED_INDEX_KEY);
      let encoded = index.to_string();
      table.insert(metadata_key.as_str(), encoded.as_bytes())?;
    }
    write_txn.commit()?;
    Ok(())
  }

  fn load_purged_consensus_index(&self, group: &str) -> Result<Option<u64>> {
    let Some(payload) = self.load_consensus_value(group, CONSENSUS_PURGED_INDEX_KEY)? else {
      return Ok(None);
    };
    let raw = std::str::from_utf8(&payload).map_err(|err| {
      crate::StoreError::Corruption(format!(
        "invalid purged consensus index for group {group}: {err}"
      ))
    })?;
    raw.parse::<u64>().map(Some).map_err(|err| {
      crate::StoreError::Corruption(format!(
        "invalid purged consensus index for group {group}: {err}"
      ))
    })
  }
}
