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

  fn hydrate_runtime(&self, topic_partition: &TopicPartition) -> Result<TopicRuntime> {
    let topic_dir = self.partition_dir(topic_partition);
    if !topic_dir.exists() {
      return Err(StoreError::PartitionNotFound {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
      });
    }

    let config = self.load_topic_config(&topic_partition.topic)?;
    if topic_partition.partition >= config.partitions {
      return Err(StoreError::PartitionNotFound {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
      });
    }

    let checkpoint = self.read_checkpoint(topic_partition)?;
    let mut segments = self.load_segments(topic_partition, &config)?;
    if segments.is_empty() {
      let base_offset = checkpoint.unwrap_or(0);
      segments.push(self.create_segment_files(topic_partition, base_offset)?);
    }

    let actual_next_offset = segments
      .last()
      .and_then(|segment| segment.last_offset.map(|offset| offset + 1))
      .unwrap_or(0);

    let next_offset = match checkpoint {
      Some(checkpoint_offset) if checkpoint_offset <= actual_next_offset => {
        checkpoint_offset.max(actual_next_offset)
      }
      Some(checkpoint_offset) => {
        self.write_checkpoint(topic_partition, actual_next_offset)?;
        actual_next_offset.min(checkpoint_offset)
      }
      None => {
        self.write_checkpoint(topic_partition, actual_next_offset)?;
        actual_next_offset
      }
    };

    Ok(TopicRuntime {
      config,
      segments,
      next_offset,
    })
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

  fn read_checkpoint(&self, topic_partition: &TopicPartition) -> Result<Option<u64>> {
    let path = self.checkpoint_path(topic_partition);
    if !path.exists() {
      return Ok(None);
    }
    let raw = fs::read_to_string(path)?;
    let value = raw
      .trim()
      .parse::<u64>()
      .map_err(|err| StoreError::Corruption(format!("invalid checkpoint: {err}")))?;
    Ok(Some(value))
  }

  fn write_checkpoint(&self, topic_partition: &TopicPartition, next_offset: u64) -> Result<()> {
    fs::write(
      self.checkpoint_path(topic_partition),
      format!("{next_offset}\n"),
    )?;
    Ok(())
  }

  fn load_segments(
    &self,
    topic_partition: &TopicPartition,
    config: &TopicConfig,
  ) -> Result<Vec<SegmentDescriptor>> {
    let dir = self.partition_dir(topic_partition);
    let mut base_offsets = Vec::new();
    for entry in fs::read_dir(&dir)? {
      let entry = entry?;
      let path = entry.path();
      if path.extension().and_then(|ext| ext.to_str()) != Some(&LOG_SUFFIX[1..]) {
        continue;
      }
      let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        continue;
      };
      let base_offset = stem.parse::<u64>().map_err(|err| {
        StoreError::Corruption(format!("invalid segment file name {stem}: {err}"))
      })?;
      base_offsets.push(base_offset);
    }
    base_offsets.sort_unstable();

    let mut segments = Vec::with_capacity(base_offsets.len());
    for base_offset in base_offsets {
      let (log_path, index_path) = self.segment_paths(topic_partition, base_offset);
      if !index_path.exists() {
        File::create(&index_path)?;
      }
      segments.push(self.recover_segment(
        base_offset,
        log_path,
        index_path,
        config.index_interval_bytes,
      )?);
    }
    Ok(segments)
  }

  fn create_segment_files(
    &self,
    topic_partition: &TopicPartition,
    base_offset: u64,
  ) -> Result<SegmentDescriptor> {
    let (log_path, index_path) = self.segment_paths(topic_partition, base_offset);
    if !log_path.exists() {
      File::create(&log_path)?;
    }
    if !index_path.exists() {
      File::create(&index_path)?;
    }
    Ok(SegmentDescriptor {
      base_offset,
      log_path,
      index_path,
      next_write_pos: 0,
      last_offset: None,
      last_indexed_pos: 0,
    })
  }

  fn recover_segment(
    &self,
    base_offset: u64,
    log_path: PathBuf,
    index_path: PathBuf,
    index_interval_bytes: u64,
  ) -> Result<SegmentDescriptor> {
    let scan = Self::scan_segment_and_repair_tail(&log_path)?;
    let last_indexed_pos = Self::rebuild_index(&log_path, &index_path, index_interval_bytes)?;

    Ok(SegmentDescriptor {
      base_offset,
      log_path,
      index_path,
      next_write_pos: scan.valid_len,
      last_offset: scan.last_offset,
      last_indexed_pos,
    })
  }

  fn scan_segment_and_repair_tail(log_path: &Path) -> Result<RecoveryScanState> {
    let mut file = OpenOptions::new().read(true).write(true).open(log_path)?;
    let mut valid_len = 0u64;
    let mut last_offset = None;

    loop {
      file.seek(SeekFrom::Start(valid_len))?;
      match Self::read_record_from_file(&mut file)? {
        Ok((record, record_len)) => {
          valid_len += record_len as u64;
          last_offset = Some(record.offset);
        }
        Err(ReadRecordOutcome::EndOfFile) => break,
        Err(ReadRecordOutcome::TruncatedTail) => {
          file.set_len(valid_len)?;
          file.flush()?;
          break;
        }
      }
    }

    Ok(RecoveryScanState {
      valid_len,
      last_offset,
    })
  }

  fn rebuild_index(log_path: &Path, index_path: &Path, index_interval_bytes: u64) -> Result<u64> {
    let mut log = OpenOptions::new().read(true).open(log_path)?;
    let tmp_index_path = index_path.with_extension("idx.rebuild");
    let mut index = File::create(&tmp_index_path)?;

    let mut file_pos = 0u64;
    let mut last_indexed_pos = 0u64;
    loop {
      log.seek(SeekFrom::Start(file_pos))?;
      match Self::read_record_from_file(&mut log)? {
        Ok((record, record_len)) => {
          if file_pos == 0 || file_pos.saturating_sub(last_indexed_pos) >= index_interval_bytes {
            index.write_all(&record.offset.to_le_bytes())?;
            index.write_all(&file_pos.to_le_bytes())?;
            last_indexed_pos = file_pos;
          }
          file_pos += record_len as u64;
        }
        Err(ReadRecordOutcome::EndOfFile | ReadRecordOutcome::TruncatedTail) => break,
      }
    }

    index.flush()?;
    fs::rename(tmp_index_path, index_path)?;
    Ok(last_indexed_pos)
  }

  fn should_roll_segment(
    runtime: &TopicRuntime,
    active: &SegmentDescriptor,
    payload_len: usize,
  ) -> bool {
    let projected_len = active.next_write_pos + (RECORD_HEADER_LEN + payload_len) as u64;
    projected_len > runtime.config.segment_max_bytes && active.next_write_pos > 0
  }

  fn append_index_entry(index_path: &Path, offset: u64, position: u64) -> Result<()> {
    let mut file = OpenOptions::new()
      .create(true)
      .append(true)
      .open(index_path)?;
    file.write_all(&offset.to_le_bytes())?;
    file.write_all(&position.to_le_bytes())?;
    file.flush()?;
    Ok(())
  }

  fn write_record(file: &mut File, record: &Record) -> Result<usize> {
    let checksum = Self::checksum(record.offset, record.timestamp_ms, record.payload.as_ref());
    file.write_all(&[CURRENT_VERSION])?;
    file.write_all(&record.offset.to_le_bytes())?;
    file.write_all(&record.timestamp_ms.to_le_bytes())?;
    file.write_all(&(record.payload.len() as u32).to_le_bytes())?;
    file.write_all(&checksum.to_le_bytes())?;
    file.write_all(record.payload.as_ref())?;
    file.flush()?;
    Ok(RECORD_HEADER_LEN + record.payload.len())
  }

  fn checksum(offset: u64, timestamp_ms: u64, payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&offset.to_le_bytes());
    hasher.update(&timestamp_ms.to_le_bytes());
    hasher.update(&(payload.len() as u32).to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
  }

  fn read_record_from_file(
    file: &mut File,
  ) -> Result<std::result::Result<(Record, usize), ReadRecordOutcome>> {
    let start_pos = file.stream_position()?;
    let mut header = [0u8; RECORD_HEADER_LEN];
    match file.read_exact(&mut header) {
      Ok(()) => {}
      Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
        if file.metadata()?.len() == start_pos {
          return Ok(Err(ReadRecordOutcome::EndOfFile));
        }
        return Ok(Err(ReadRecordOutcome::TruncatedTail));
      }
      Err(err) => return Err(err.into()),
    }

    let version = header[0];
    if version != CURRENT_VERSION {
      return Err(StoreError::Corruption(format!(
        "unsupported record version: {version}"
      )));
    }
    let offset = u64::from_le_bytes(header[1..9].try_into().unwrap());
    let timestamp_ms = u64::from_le_bytes(header[9..17].try_into().unwrap());
    let payload_len = u32::from_le_bytes(header[17..21].try_into().unwrap()) as usize;
    let checksum = u32::from_le_bytes(header[21..25].try_into().unwrap());

    let mut payload = vec![0u8; payload_len];
    match file.read_exact(&mut payload) {
      Ok(()) => {}
      Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
        return Ok(Err(ReadRecordOutcome::TruncatedTail));
      }
      Err(err) => return Err(err.into()),
    }

    let computed = Self::checksum(offset, timestamp_ms, &payload);
    if checksum != computed {
      return Err(StoreError::Corruption(format!(
        "record checksum mismatch at offset {offset}: expected {checksum}, got {computed}"
      )));
    }

    Ok(Ok((
      Record {
        offset,
        timestamp_ms,
        payload: Bytes::from(payload),
      },
      RECORD_HEADER_LEN + payload_len,
    )))
  }

  fn read_records_from_segment(
    segment: &SegmentDescriptor,
    offset: u64,
    max_records: usize,
  ) -> Result<Vec<Record>> {
    let mut file = OpenOptions::new().read(true).open(&segment.log_path)?;
    let start_pos = Self::find_read_position(segment, offset)?;
    file.seek(SeekFrom::Start(start_pos))?;

    let mut records = Vec::new();
    while records.len() < max_records {
      match Self::read_record_from_file(&mut file)? {
        Ok((record, _record_len)) => {
          if record.offset >= offset {
            records.push(record);
          }
        }
        Err(ReadRecordOutcome::EndOfFile | ReadRecordOutcome::TruncatedTail) => break,
      }
    }
    Ok(records)
  }

  fn find_read_position(segment: &SegmentDescriptor, offset: u64) -> Result<u64> {
    if !segment.index_path.exists() {
      return Ok(0);
    }
    let mut file = OpenOptions::new().read(true).open(&segment.index_path)?;
    let metadata = file.metadata()?;
    if metadata.len() < INDEX_ENTRY_LEN as u64 {
      return Ok(0);
    }

    let mut buf = [0u8; INDEX_ENTRY_LEN];
    let mut best_position = 0u64;
    loop {
      match file.read_exact(&mut buf) {
        Ok(()) => {
          let indexed_offset = u64::from_le_bytes(buf[0..8].try_into().unwrap());
          let indexed_position = u64::from_le_bytes(buf[8..16].try_into().unwrap());
          if indexed_offset <= offset {
            best_position = indexed_position;
          } else {
            break;
          }
        }
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
        Err(err) => return Err(err.into()),
      }
    }
    Ok(best_position)
  }

  fn append_existing_record(file: &mut File, record: &Record) -> Result<usize> {
    Self::write_record(file, record)
  }

  fn collect_records_before_offset(
    runtime: &TopicRuntime,
    truncate_offset: u64,
  ) -> Result<Vec<Record>> {
    if truncate_offset == 0 {
      return Ok(Vec::new());
    }

    let mut preserved = Vec::new();
    for segment in &runtime.segments {
      let records = Self::read_records_from_segment(segment, 0, usize::MAX)?;
      for record in records {
        if record.offset < truncate_offset {
          preserved.push(record);
        } else {
          return Ok(preserved);
        }
      }
    }
    Ok(preserved)
  }

  fn rebuild_partition_from_records(
    &self,
    topic_partition: &TopicPartition,
    config: &TopicConfig,
    records: &[Record],
  ) -> Result<TopicRuntime> {
    let partition_dir = self.partition_dir(topic_partition);
    if partition_dir.exists() {
      fs::remove_dir_all(&partition_dir)?;
    }
    fs::create_dir_all(&partition_dir)?;

    let mut segments = vec![self.create_segment_files(topic_partition, 0)?];
    for record in records {
      let active_snapshot = segments.last().cloned().ok_or_else(|| {
        StoreError::Corruption("missing segment while rebuilding partition".to_owned())
      })?;

      if Self::should_roll_segment_for_config(config, &active_snapshot, record.payload.len()) {
        let next_base = record.offset;
        segments.push(self.create_segment_files(topic_partition, next_base)?);
      }

      let active = segments.last_mut().ok_or_else(|| {
        StoreError::Corruption("missing active segment while rebuilding partition".to_owned())
      })?;
      let start_pos = active.next_write_pos;
      let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&active.log_path)?;
      let written = Self::append_existing_record(&mut file, record)?;
      active.next_write_pos += written as u64;
      active.last_offset = Some(record.offset);

      if start_pos == 0
        || active
          .next_write_pos
          .saturating_sub(active.last_indexed_pos)
          >= config.index_interval_bytes
      {
        Self::append_index_entry(&active.index_path, record.offset, start_pos)?;
        active.last_indexed_pos = start_pos;
      }
    }

    let next_offset = records.last().map(|record| record.offset + 1).unwrap_or(0);
    self.write_checkpoint(topic_partition, next_offset)?;

    Ok(TopicRuntime {
      config: config.clone(),
      segments,
      next_offset,
    })
  }

  pub fn local_partition_state(
    &self,
    topic_partition: &TopicPartition,
  ) -> Result<LocalPartitionState> {
    self.validate_partition(topic_partition)?;
    self.with_runtime(topic_partition, |runtime| {
      Ok(LocalPartitionState::online(
        topic_partition.clone(),
        runtime
          .segments
          .last()
          .and_then(|segment| segment.last_offset),
      ))
    })
  }

  pub fn truncate_from(
    &self,
    topic_partition: &TopicPartition,
    truncate_offset: u64,
  ) -> Result<()> {
    let config = self.validate_partition(topic_partition)?;
    let mut guard = self.state.lock().expect("poisoned segment state lock");
    let key = topic_partition.clone();
    if !guard.contains_key(&key) {
      let runtime = self.hydrate_runtime(topic_partition)?;
      guard.insert(key.clone(), runtime);
    }

    let runtime = guard
      .get(&key)
      .cloned()
      .ok_or_else(|| StoreError::PartitionNotFound {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
      })?;

    if truncate_offset > runtime.next_offset {
      return Err(StoreError::InvalidOffset {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
        offset: truncate_offset,
      });
    }

    let preserved = Self::collect_records_before_offset(&runtime, truncate_offset)?;
    let rebuilt = self.rebuild_partition_from_records(topic_partition, &config, &preserved)?;
    guard.insert(key, rebuilt);
    Ok(())
  }

  fn should_roll_segment_for_config(
    config: &TopicConfig,
    active: &SegmentDescriptor,
    payload_len: usize,
  ) -> bool {
    (active.next_write_pos + (RECORD_HEADER_LEN + payload_len) as u64) > config.segment_max_bytes
      && active.next_write_pos > 0
  }

  fn validate_partition(&self, topic_partition: &TopicPartition) -> Result<TopicConfig> {
    let config = self.load_topic_config(&topic_partition.topic)?;
    if topic_partition.partition >= config.partitions {
      return Err(StoreError::PartitionNotFound {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
      });
    }
    Ok(config)
  }
}

impl MessageLogStore for SegmentLog {
  fn create_topic(&self, topic: TopicConfig) -> Result<()> {
    if topic.partitions == 0 {
      return Err(StoreError::Corruption(
        "topic partitions must be greater than zero".to_owned(),
      ));
    }
    if self.topic_root_dir(&topic.name).exists() {
      return Err(StoreError::TopicAlreadyExists(topic.name));
    }

    self.persist_topic_config(&topic)?;
    let mut runtime_by_partition = Vec::new();
    for partition in 0..topic.partitions {
      let dir = self.partition_dir(&topic.partition(partition));
      fs::create_dir_all(&dir)?;
      let segment = self.create_segment_files(&topic.partition(partition), 0)?;
      self.write_checkpoint(&topic.partition(partition), 0)?;
      runtime_by_partition.push((
        partition,
        TopicRuntime {
          config: topic.clone(),
          segments: vec![segment],
          next_offset: 0,
        },
      ));
    }

    let mut guard = self.state.lock().expect("poisoned segment state lock");
    for (partition, runtime) in runtime_by_partition {
      guard.insert(topic.partition(partition), runtime);
    }
    Ok(())
  }

  fn topic_exists(&self, topic: &str) -> Result<bool> {
    Ok(self.topic_root_dir(topic).exists())
  }

  fn append(&self, topic_partition: &TopicPartition, payload: &[u8]) -> Result<Record> {
    self.validate_partition(topic_partition)?;
    self.with_runtime(topic_partition, |runtime| {
      let active = runtime
        .segments
        .last()
        .cloned()
        .ok_or_else(|| StoreError::Corruption("missing active segment".to_owned()))?;

      if Self::should_roll_segment(runtime, &active, payload.len()) {
        let next_base = runtime.next_offset;
        let segment = self.create_segment_files(topic_partition, next_base)?;
        runtime.segments.push(segment);
      }

      let next_offset = runtime.next_offset;
      let record = Record {
        offset: next_offset,
        timestamp_ms: now_ms(),
        payload: Bytes::copy_from_slice(payload),
      };

      let active = runtime.segments.last_mut().ok_or_else(|| {
        StoreError::Corruption("missing active segment after rollover".to_owned())
      })?;

      let start_pos = active.next_write_pos;
      let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&active.log_path)?;
      let written = Self::write_record(&mut file, &record)?;
      active.next_write_pos += written as u64;
      active.last_offset = Some(record.offset);

      if start_pos == 0
        || active
          .next_write_pos
          .saturating_sub(active.last_indexed_pos)
          >= runtime.config.index_interval_bytes
      {
        Self::append_index_entry(&active.index_path, record.offset, start_pos)?;
        active.last_indexed_pos = start_pos;
      }

      runtime.next_offset += 1;
      self.write_checkpoint(topic_partition, runtime.next_offset)?;
      Ok(record)
    })
  }

  fn read_from(
    &self,
    topic_partition: &TopicPartition,
    offset: u64,
    max_records: usize,
  ) -> Result<Vec<Record>> {
    if max_records == 0 {
      return Ok(Vec::new());
    }
    self.validate_partition(topic_partition)?;
    self.with_runtime(topic_partition, |runtime| {
      let mut records = Vec::new();
      for segment in &runtime.segments {
        if let Some(last_offset) = segment.last_offset {
          if last_offset < offset {
            continue;
          }
        }
        if offset < segment.base_offset && !records.is_empty() {
          break;
        }
        let remaining = max_records.saturating_sub(records.len());
        if remaining == 0 {
          break;
        }
        records.extend(Self::read_records_from_segment(segment, offset, remaining)?);
        if records.len() >= max_records {
          break;
        }
      }
      Ok(records)
    })
  }

  fn last_offset(&self, topic_partition: &TopicPartition) -> Result<Option<u64>> {
    self.validate_partition(topic_partition)?;
    self.with_runtime(topic_partition, |runtime| {
      Ok(
        runtime
          .segments
          .last()
          .and_then(|segment| segment.last_offset),
      )
    })
  }
}

impl MutablePartitionLogStore for SegmentLog {
  fn truncate_from(&self, topic_partition: &TopicPartition, offset: u64) -> Result<()> {
    SegmentLog::truncate_from(self, topic_partition, offset)
  }

  fn local_partition_state(&self, topic_partition: &TopicPartition) -> Result<LocalPartitionState> {
    SegmentLog::local_partition_state(self, topic_partition)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    BrokerState, JsonCodec, PartitionAvailability, RedbMetadataStore, TopicCatalogStore,
  };
  use std::time::{SystemTime, UNIX_EPOCH};

  fn temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_nanos();
    std::env::temp_dir().join(format!("ech0-{name}-{nanos}"))
  }

  #[test]
  fn validate_all_topics_reports_only_broken_topics() {
    let root = temp_path("validate-topics");
    let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
    let meta = RedbMetadataStore::create_with_codecs(
      root.join("meta.redb"),
      JsonCodec::<TopicConfig>::new(),
      JsonCodec::<BrokerState>::new(),
      JsonCodec::new(),
    )
    .unwrap();

    let healthy = TopicConfig::new("healthy");
    log.create_topic(healthy.clone()).unwrap();
    meta.save_topic_config(&healthy).unwrap();

    let mut broken_manifest = TopicConfig::new("broken");
    broken_manifest.partitions = 2;
    log.create_topic(broken_manifest.clone()).unwrap();

    let mut broken_catalog = TopicConfig::new("broken");
    broken_catalog.partitions = 1;
    meta.save_topic_config(&broken_catalog).unwrap();

    let issues = log.validate_all_topics_against_catalog(&meta).unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].topic, "broken");
    assert!(issues[0].reason.contains("unavailable"));
  }

  #[test]
  fn truncate_from_discards_tail_and_rebuilds_indexes() {
    let root = temp_path("truncate");
    let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
    let mut topic = TopicConfig::new("orders");
    topic.segment_max_bytes = 80;
    topic.index_interval_bytes = 16;
    let topic_partition = topic.partition(0);
    log.create_topic(topic).unwrap();

    for payload in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
      log.append(&topic_partition, payload).unwrap();
    }

    log.truncate_from(&topic_partition, 2).unwrap();

    let records = log.read_from(&topic_partition, 0, 10).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[1].offset, 1);
    assert_eq!(records[1].payload.as_ref(), b"two");
    assert_eq!(log.last_offset(&topic_partition).unwrap(), Some(1));

    let checkpoint = std::fs::read_to_string(root.join("segments/orders/0/checkpoint")).unwrap();
    assert_eq!(checkpoint.trim(), "2");
  }

  #[test]
  fn truncate_then_append_keeps_offsets_monotonic() {
    let root = temp_path("truncate-append");
    let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
    let mut topic = TopicConfig::new("events");
    topic.segment_max_bytes = 80;
    topic.index_interval_bytes = 16;
    let topic_partition = topic.partition(0);
    log.create_topic(topic).unwrap();

    for payload in [b"zero".as_slice(), b"one".as_slice(), b"two".as_slice()] {
      log.append(&topic_partition, payload).unwrap();
    }

    log.truncate_from(&topic_partition, 2).unwrap();
    let appended = log.append(&topic_partition, b"replacement").unwrap();
    assert_eq!(appended.offset, 2);

    let records = log.read_from(&topic_partition, 0, 10).unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[2].offset, 2);
    assert_eq!(records[2].payload.as_ref(), b"replacement");
  }

  #[test]
  fn truncate_rebuilds_multi_segment_partition() {
    let root = temp_path("truncate-multi-segment");
    let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
    let mut topic = TopicConfig::new("metrics");
    topic.segment_max_bytes = 70;
    topic.index_interval_bytes = 8;
    let topic_partition = topic.partition(0);
    log.create_topic(topic).unwrap();

    for idx in 0..6u8 {
      let payload = vec![idx; 12];
      log.append(&topic_partition, &payload).unwrap();
    }

    log.truncate_from(&topic_partition, 3).unwrap();
    let records = log.read_from(&topic_partition, 0, 10).unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(
      records
        .iter()
        .map(|record| record.offset)
        .collect::<Vec<_>>(),
      vec![0, 1, 2]
    );

    let partition_dir = root.join("segments/metrics/0");
    let segment_count = std::fs::read_dir(partition_dir)
      .unwrap()
      .filter_map(|entry| entry.ok())
      .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("log"))
      .count();
    assert!(segment_count >= 1);
  }

  #[test]
  fn local_partition_state_tracks_last_offset_and_recovers_after_reopen() {
    let root = temp_path("local-state-recover");
    let log_root = root.join("segments");
    let topic_partition = TopicPartition::new("audit", 0);

    let log = SegmentLog::open(SegmentLogOptions::new(&log_root)).unwrap();
    let mut topic = TopicConfig::new("audit");
    topic.segment_max_bytes = 80;
    topic.index_interval_bytes = 8;
    log.create_topic(topic).unwrap();
    log.append(&topic_partition, b"first").unwrap();
    log.append(&topic_partition, b"second").unwrap();

    let before = log.local_partition_state(&topic_partition).unwrap();
    assert_eq!(before.availability, PartitionAvailability::Online);
    assert_eq!(before.state.last_appended_offset, Some(1));
    assert_eq!(before.state.high_watermark, Some(1));

    let reopened = SegmentLog::open(SegmentLogOptions::new(&log_root)).unwrap();
    let after = reopened.local_partition_state(&topic_partition).unwrap();
    assert_eq!(after.state.last_appended_offset, Some(1));
    assert_eq!(after.state.high_watermark, Some(1));
  }

  #[test]
  fn unavailable_topic_does_not_hide_healthy_partition_state() {
    let root = temp_path("unavailable-topic-does-not-hide-healthy");
    let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
    let meta = RedbMetadataStore::create_with_codecs(
      root.join("meta.redb"),
      JsonCodec::<TopicConfig>::new(),
      JsonCodec::<BrokerState>::new(),
      JsonCodec::new(),
    )
    .unwrap();

    let healthy = TopicConfig::new("healthy");
    log.create_topic(healthy.clone()).unwrap();
    meta.save_topic_config(&healthy).unwrap();
    let healthy_partition = healthy.partition(0);
    log.append(&healthy_partition, b"ok").unwrap();
    let state = log.local_partition_state(&healthy_partition).unwrap();
    assert_eq!(state.state.last_appended_offset, Some(0));

    let broken_manifest = TopicConfig::new("broken");
    log.create_topic(broken_manifest.clone()).unwrap();
    let mut broken_catalog = TopicConfig::new("broken");
    broken_catalog.partitions = 2;
    meta.save_topic_config(&broken_catalog).unwrap();

    let issues = log.validate_all_topics_against_catalog(&meta).unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].topic, "broken");

    let state_after_validation = log.local_partition_state(&healthy_partition).unwrap();
    assert_eq!(state_after_validation.state.last_appended_offset, Some(0));
  }
}
