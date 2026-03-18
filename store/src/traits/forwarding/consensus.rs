use std::sync::Arc;

use crate::Result;

use super::super::{ConsensusLogStore, ConsensusMetadataStore};

impl<T> ConsensusMetadataStore for &T
where
  T: ConsensusMetadataStore + ?Sized,
{
  fn save_consensus_value(&self, group: &str, key: &str, payload: &[u8]) -> Result<()> {
    (**self).save_consensus_value(group, key, payload)
  }
  fn load_consensus_value(&self, group: &str, key: &str) -> Result<Option<Vec<u8>>> {
    (**self).load_consensus_value(group, key)
  }
}

impl<T> ConsensusMetadataStore for Arc<T>
where
  T: ConsensusMetadataStore + ?Sized,
{
  fn save_consensus_value(&self, group: &str, key: &str, payload: &[u8]) -> Result<()> {
    (**self).save_consensus_value(group, key, payload)
  }
  fn load_consensus_value(&self, group: &str, key: &str) -> Result<Option<Vec<u8>>> {
    (**self).load_consensus_value(group, key)
  }
}

impl<T> ConsensusLogStore for &T
where
  T: ConsensusLogStore + ?Sized,
{
  fn append_consensus_entry(&self, group: &str, index: u64, payload: &[u8]) -> Result<()> {
    (**self).append_consensus_entry(group, index, payload)
  }
  fn load_consensus_entry(&self, group: &str, index: u64) -> Result<Option<Vec<u8>>> {
    (**self).load_consensus_entry(group, index)
  }
  fn load_consensus_entries(
    &self,
    group: &str,
    start: u64,
    end: Option<u64>,
  ) -> Result<Vec<(u64, Vec<u8>)>> {
    (**self).load_consensus_entries(group, start, end)
  }
  fn load_last_consensus_index(&self, group: &str) -> Result<Option<u64>> {
    (**self).load_last_consensus_index(group)
  }
  fn truncate_consensus_from(&self, group: &str, index: u64) -> Result<()> {
    (**self).truncate_consensus_from(group, index)
  }
  fn purge_consensus_to(&self, group: &str, index: u64) -> Result<()> {
    (**self).purge_consensus_to(group, index)
  }
  fn load_purged_consensus_index(&self, group: &str) -> Result<Option<u64>> {
    (**self).load_purged_consensus_index(group)
  }
}

impl<T> ConsensusLogStore for Arc<T>
where
  T: ConsensusLogStore + ?Sized,
{
  fn append_consensus_entry(&self, group: &str, index: u64, payload: &[u8]) -> Result<()> {
    (**self).append_consensus_entry(group, index, payload)
  }
  fn load_consensus_entry(&self, group: &str, index: u64) -> Result<Option<Vec<u8>>> {
    (**self).load_consensus_entry(group, index)
  }
  fn load_consensus_entries(
    &self,
    group: &str,
    start: u64,
    end: Option<u64>,
  ) -> Result<Vec<(u64, Vec<u8>)>> {
    (**self).load_consensus_entries(group, start, end)
  }
  fn load_last_consensus_index(&self, group: &str) -> Result<Option<u64>> {
    (**self).load_last_consensus_index(group)
  }
  fn truncate_consensus_from(&self, group: &str, index: u64) -> Result<()> {
    (**self).truncate_consensus_from(group, index)
  }
  fn purge_consensus_to(&self, group: &str, index: u64) -> Result<()> {
    (**self).purge_consensus_to(group, index)
  }
  fn load_purged_consensus_index(&self, group: &str) -> Result<Option<u64>> {
    (**self).load_purged_consensus_index(group)
  }
}
