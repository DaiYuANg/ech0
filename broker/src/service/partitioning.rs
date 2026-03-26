use std::collections::BTreeMap;
use std::sync::Mutex;

use store::{Result, StoreError, TopicConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublishPartitioning {
  Explicit(u32),
  RoundRobin,
  KeyHash,
}

impl PublishPartitioning {
  pub(crate) fn as_label(&self) -> &'static str {
    match self {
      Self::Explicit(_) => "explicit",
      Self::RoundRobin => "round_robin",
      Self::KeyHash => "key_hash",
    }
  }
}

#[derive(Debug, Default)]
pub(crate) struct PartitionRouter {
  round_robin_cursors: Mutex<BTreeMap<String, u32>>,
}

impl PartitionRouter {
  pub(crate) fn select_partition(
    &self,
    topic_config: &TopicConfig,
    partitioning: PublishPartitioning,
    key: Option<&[u8]>,
  ) -> Result<u32> {
    if topic_config.partitions == 0 {
      return Err(StoreError::Corruption(format!(
        "topic {} has zero partitions",
        topic_config.name
      )));
    }

    match partitioning {
      PublishPartitioning::Explicit(partition) => Ok(partition),
      PublishPartitioning::RoundRobin => self.next_round_robin_partition(topic_config),
      PublishPartitioning::KeyHash => {
        let key = key.ok_or_else(|| {
          StoreError::Codec("key_hash partitioning requires a non-empty key".to_owned())
        })?;
        if key.is_empty() {
          return Err(StoreError::Codec(
            "key_hash partitioning requires a non-empty key".to_owned(),
          ));
        }
        Ok((fnv1a_hash64(key) % u64::from(topic_config.partitions)) as u32)
      }
    }
  }

  fn next_round_robin_partition(&self, topic_config: &TopicConfig) -> Result<u32> {
    let mut cursors = self.round_robin_cursors.lock().map_err(|_| {
      StoreError::Corruption("partition router mutex poisoned".to_owned())
    })?;
    let cursor = cursors.entry(topic_config.name.clone()).or_insert(0);
    let partition = *cursor % topic_config.partitions;
    *cursor = (*cursor + 1) % topic_config.partitions;
    Ok(partition)
  }
}

fn fnv1a_hash64(bytes: &[u8]) -> u64 {
  const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
  const PRIME: u64 = 0x100000001b3;

  let mut hash = OFFSET_BASIS;
  for byte in bytes {
    hash ^= u64::from(*byte);
    hash = hash.wrapping_mul(PRIME);
  }
  hash
}
