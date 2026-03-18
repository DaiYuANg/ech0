use super::*;

impl ConsensusMetadataStore for InMemoryStore {
  fn save_consensus_value(&self, group: &str, key: &str, payload: &[u8]) -> Result<()> {
    let mut metadata = self
      .consensus_metadata
      .write()
      .expect("poisoned consensus_metadata lock");
    metadata.insert((group.to_owned(), key.to_owned()), payload.to_vec());
    Ok(())
  }

  fn load_consensus_value(&self, group: &str, key: &str) -> Result<Option<Vec<u8>>> {
    let metadata = self
      .consensus_metadata
      .read()
      .expect("poisoned consensus_metadata lock");
    Ok(metadata.get(&(group.to_owned(), key.to_owned())).cloned())
  }
}

impl ConsensusLogStore for InMemoryStore {
  fn append_consensus_entry(&self, group: &str, index: u64, payload: &[u8]) -> Result<()> {
    let mut logs = self
      .consensus_logs
      .write()
      .expect("poisoned consensus_logs lock");
    let entries = logs.entry(group.to_owned()).or_default();
    entries.insert(index, payload.to_vec());
    Ok(())
  }

  fn load_consensus_entry(&self, group: &str, index: u64) -> Result<Option<Vec<u8>>> {
    let logs = self
      .consensus_logs
      .read()
      .expect("poisoned consensus_logs lock");
    Ok(
      logs
        .get(group)
        .and_then(|entries| entries.get(&index).cloned()),
    )
  }

  fn load_consensus_entries(
    &self,
    group: &str,
    start: u64,
    end: Option<u64>,
  ) -> Result<Vec<(u64, Vec<u8>)>> {
    let logs = self
      .consensus_logs
      .read()
      .expect("poisoned consensus_logs lock");
    let Some(entries) = logs.get(group) else {
      return Ok(Vec::new());
    };

    Ok(
      entries
        .range(start..)
        .take_while(|(index, _)| end.map(|limit| **index < limit).unwrap_or(true))
        .map(|(index, payload)| (*index, payload.clone()))
        .collect(),
    )
  }

  fn load_last_consensus_index(&self, group: &str) -> Result<Option<u64>> {
    let logs = self
      .consensus_logs
      .read()
      .expect("poisoned consensus_logs lock");
    Ok(
      logs
        .get(group)
        .and_then(|entries| entries.last_key_value().map(|(index, _)| *index)),
    )
  }

  fn truncate_consensus_from(&self, group: &str, index: u64) -> Result<()> {
    let mut logs = self
      .consensus_logs
      .write()
      .expect("poisoned consensus_logs lock");
    if let Some(entries) = logs.get_mut(group) {
      entries.retain(|entry_index, _| *entry_index < index);
    }
    Ok(())
  }

  fn purge_consensus_to(&self, group: &str, index: u64) -> Result<()> {
    {
      let mut logs = self
        .consensus_logs
        .write()
        .expect("poisoned consensus_logs lock");
      if let Some(entries) = logs.get_mut(group) {
        entries.retain(|entry_index, _| *entry_index > index);
      }
    }

    let mut purged = self
      .consensus_purged_indices
      .write()
      .expect("poisoned consensus_purged_indices lock");
    purged.insert(group.to_owned(), index);
    Ok(())
  }

  fn load_purged_consensus_index(&self, group: &str) -> Result<Option<u64>> {
    let purged = self
      .consensus_purged_indices
      .read()
      .expect("poisoned consensus_purged_indices lock");
    Ok(purged.get(group).copied())
  }
}
