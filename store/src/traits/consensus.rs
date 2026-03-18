use crate::Result;

pub trait ConsensusMetadataStore: Send + Sync {
  fn save_consensus_value(&self, _group: &str, _key: &str, _payload: &[u8]) -> Result<()> {
    Err(crate::StoreError::Unsupported(
      "consensus metadata store is not implemented yet",
    ))
  }

  fn load_consensus_value(&self, _group: &str, _key: &str) -> Result<Option<Vec<u8>>> {
    Err(crate::StoreError::Unsupported(
      "consensus metadata store is not implemented yet",
    ))
  }
}

pub trait ConsensusLogStore: Send + Sync {
  fn append_consensus_entry(&self, _group: &str, _index: u64, _payload: &[u8]) -> Result<()> {
    Err(crate::StoreError::Unsupported(
      "consensus log store is not implemented yet",
    ))
  }

  fn load_consensus_entry(&self, _group: &str, _index: u64) -> Result<Option<Vec<u8>>> {
    Err(crate::StoreError::Unsupported(
      "consensus log store is not implemented yet",
    ))
  }

  fn load_consensus_entries(
    &self,
    _group: &str,
    _start: u64,
    _end: Option<u64>,
  ) -> Result<Vec<(u64, Vec<u8>)>> {
    Err(crate::StoreError::Unsupported(
      "consensus log store is not implemented yet",
    ))
  }

  fn load_last_consensus_index(&self, _group: &str) -> Result<Option<u64>> {
    Err(crate::StoreError::Unsupported(
      "consensus log store is not implemented yet",
    ))
  }

  fn truncate_consensus_from(&self, _group: &str, _index: u64) -> Result<()> {
    Err(crate::StoreError::Unsupported(
      "consensus log store is not implemented yet",
    ))
  }

  fn purge_consensus_to(&self, _group: &str, _index: u64) -> Result<()> {
    Err(crate::StoreError::Unsupported(
      "consensus log store is not implemented yet",
    ))
  }

  fn load_purged_consensus_index(&self, _group: &str) -> Result<Option<u64>> {
    Err(crate::StoreError::Unsupported(
      "consensus log store is not implemented yet",
    ))
  }
}
