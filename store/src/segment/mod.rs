mod log_store;
mod partition;
mod records;
mod recovery;
#[cfg(test)]
mod tests;

use std::{
  collections::HashMap,
  fs::{self, File, OpenOptions},
  io::{self, Read, Seek, SeekFrom, Write},
  path::{Path, PathBuf},
  sync::Mutex,
};

use bytes::Bytes;
use crc32fast::Hasher;

use crate::{
  Result, StoreError,
  model::{LocalPartitionState, Record, TopicConfig, TopicPartition, TopicValidationIssue, now_ms},
  traits::{MessageLogStore, MutablePartitionLogStore, TopicCatalogStore},
};

const LOG_SUFFIX: &str = ".log";
const INDEX_SUFFIX: &str = ".idx";
const CHECKPOINT_FILE: &str = "checkpoint";
const TOPIC_CONFIG_FILE: &str = "topic.json";
const CURRENT_VERSION: u8 = 1;
const RECORD_HEADER_LEN: usize = 1 + 8 + 8 + 4 + 4;
const INDEX_ENTRY_LEN: usize = 8 + 8;

#[derive(Debug, Clone)]
pub struct SegmentLogOptions {
  pub root_dir: PathBuf,
  pub default_segment_max_bytes: u64,
  pub default_index_interval_bytes: u64,
}

impl SegmentLogOptions {
  pub fn new(root_dir: impl Into<PathBuf>) -> Self {
    Self {
      root_dir: root_dir.into(),
      default_segment_max_bytes: 16 * 1024 * 1024,
      default_index_interval_bytes: 4 * 1024,
    }
  }
}

#[derive(Debug, Clone)]
struct SegmentDescriptor {
  base_offset: u64,
  log_path: PathBuf,
  index_path: PathBuf,
  next_write_pos: u64,
  last_offset: Option<u64>,
  last_indexed_pos: u64,
}

#[derive(Debug, Clone)]
struct TopicRuntime {
  config: TopicConfig,
  segments: Vec<SegmentDescriptor>,
  next_offset: u64,
}

#[derive(Debug)]
pub struct SegmentLog {
  root_dir: PathBuf,
  options: SegmentLogOptions,
  state: Mutex<HashMap<TopicPartition, TopicRuntime>>,
}

#[derive(Debug, Clone, Copy)]
enum ReadRecordOutcome {
  EndOfFile,
  TruncatedTail,
}

#[derive(Debug, Clone, Copy)]
struct RecoveryScanState {
  valid_len: u64,
  last_offset: Option<u64>,
}

impl SegmentLog {
  pub fn open(options: SegmentLogOptions) -> Result<Self> {
    fs::create_dir_all(&options.root_dir)?;
    Ok(Self {
      root_dir: options.root_dir.clone(),
      options,
      state: Mutex::new(HashMap::new()),
    })
  }

  pub fn validate_topic_manifest_against_catalog<C>(&self, catalog: &C, topic: &str) -> Result<()>
  where
    C: TopicCatalogStore,
  {
    let manifest = self.load_topic_config(topic)?;
    let Some(catalog_topic) = catalog.load_topic_config(topic)? else {
      return Err(StoreError::TopicUnavailable {
        topic: topic.to_owned(),
        reason: "segment manifest exists but metadata catalog entry is missing".to_owned(),
      });
    };

    if manifest != catalog_topic {
      return Err(StoreError::TopicUnavailable {
        topic: topic.to_owned(),
        reason: "segment manifest and metadata catalog disagree".to_owned(),
      });
    }
    Ok(())
  }

  pub fn validate_all_topics_against_catalog<C>(
    &self,
    catalog: &C,
  ) -> Result<Vec<TopicValidationIssue>>
  where
    C: TopicCatalogStore,
  {
    let mut issues = Vec::new();
    for topic in self.discover_topics()? {
      if let Err(err) = self.validate_topic_manifest_against_catalog(catalog, &topic) {
        issues.push(TopicValidationIssue {
          topic: topic.clone(),
          reason: err.to_string(),
        });
      }
    }
    issues.sort_by(|a, b| a.topic.cmp(&b.topic));
    Ok(issues)
  }

  fn discover_topics(&self) -> Result<Vec<String>> {
    let mut topics = Vec::new();
    for entry in fs::read_dir(&self.root_dir)? {
      let entry = entry?;
      if !entry.file_type()?.is_dir() {
        continue;
      }
      topics.push(entry.file_name().to_string_lossy().into_owned());
    }
    topics.sort();
    Ok(topics)
  }

  fn topic_root_dir(&self, topic: &str) -> PathBuf {
    self.root_dir.join(topic)
  }

  fn partition_dir(&self, topic_partition: &TopicPartition) -> PathBuf {
    self
      .topic_root_dir(&topic_partition.topic)
      .join(topic_partition.partition.to_string())
  }

  fn topic_config_path(&self, topic: &str) -> PathBuf {
    self.topic_root_dir(topic).join(TOPIC_CONFIG_FILE)
  }

  fn checkpoint_path(&self, topic_partition: &TopicPartition) -> PathBuf {
    self.partition_dir(topic_partition).join(CHECKPOINT_FILE)
  }

  fn segment_file_name(base_offset: u64, suffix: &str) -> String {
    format!("{base_offset:020}{suffix}")
  }

  fn segment_paths(
    &self,
    topic_partition: &TopicPartition,
    base_offset: u64,
  ) -> (PathBuf, PathBuf) {
    let dir = self.partition_dir(topic_partition);
    (
      dir.join(Self::segment_file_name(base_offset, LOG_SUFFIX)),
      dir.join(Self::segment_file_name(base_offset, INDEX_SUFFIX)),
    )
  }

  fn load_topic_config(&self, topic: &str) -> Result<TopicConfig> {
    let path = self.topic_config_path(topic);
    if !path.exists() {
      return Err(StoreError::TopicNotFound(topic.to_owned()));
    }
    let bytes = fs::read(path)?;
    serde_json::from_slice::<TopicConfig>(&bytes)
      .map_err(|err| StoreError::Codec(format!("failed to decode topic config manifest: {err}")))
  }

  fn persist_topic_config(&self, topic: &TopicConfig) -> Result<()> {
    fs::create_dir_all(self.topic_root_dir(&topic.name))?;
    let bytes = serde_json::to_vec_pretty(topic)
      .map_err(|err| StoreError::Codec(format!("failed to encode topic config manifest: {err}")))?;
    fs::write(self.topic_config_path(&topic.name), bytes)?;
    Ok(())
  }

  fn with_runtime<T>(
    &self,
    topic_partition: &TopicPartition,
    f: impl FnOnce(&mut TopicRuntime) -> Result<T>,
  ) -> Result<T> {
    let mut guard = self.state.lock().expect("poisoned segment state lock");
    let key = topic_partition.clone();
    if !guard.contains_key(&key) {
      let runtime = self.hydrate_runtime(topic_partition)?;
      guard.insert(key.clone(), runtime);
    }
    let runtime = guard
      .get_mut(&key)
      .ok_or_else(|| StoreError::PartitionNotFound {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
      })?;
    f(runtime)
  }
}
