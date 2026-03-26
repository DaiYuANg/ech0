use super::*;

impl SegmentLog {
  pub fn compact_once(&self) -> Result<(usize, usize)> {
    let mut compacted_partitions = 0usize;
    let mut removed_records = 0usize;
    for topic in self.discover_topics()? {
      let config = self.load_topic_config(&topic)?;
      if !config.compaction_enabled
        || !matches!(
          config.cleanup_policy,
          TopicCleanupPolicy::Compact | TopicCleanupPolicy::CompactAndDelete
        )
      {
        continue;
      }
      for partition in 0..config.partitions {
        let topic_partition = config.partition(partition);
        let removed = self.compact_partition(&topic_partition, &config)?;
        if removed > 0 {
          compacted_partitions += 1;
          removed_records += removed;
        }
      }
    }
    Ok((compacted_partitions, removed_records))
  }

  pub fn enforce_retention_once(&self) -> Result<usize> {
    let mut removed_segments = 0_usize;
    for topic in self.discover_topics()? {
      let config = self.load_topic_config(&topic)?;
      for partition in 0..config.partitions {
        let topic_partition = config.partition(partition);
        removed_segments += self.enforce_partition_retention(&topic_partition, &config)?;
      }
    }
    Ok(removed_segments)
  }

  fn enforce_partition_retention(&self, topic_partition: &TopicPartition, config: &TopicConfig) -> Result<usize> {
    if !matches!(
      config.cleanup_policy,
      TopicCleanupPolicy::Delete | TopicCleanupPolicy::CompactAndDelete
    ) {
      return Ok(0);
    }
    self.validate_partition(topic_partition)?;
    self.with_runtime(topic_partition, |runtime| {
      let mut removed = 0_usize;
      removed += Self::enforce_partition_retention_by_age(runtime, config.retention_ms)?;
      removed += Self::enforce_partition_retention_by_size(runtime, config.retention_max_bytes)?;
      Ok(removed)
    })
  }

  fn enforce_partition_retention_by_age(
    runtime: &mut TopicRuntime,
    retention_ms: Option<u64>,
  ) -> Result<usize> {
    let Some(retention_ms) = retention_ms else {
      return Ok(0);
    };
    if runtime.segments.len() <= 1 {
      return Ok(0);
    }

    let now_ms = now_ms();
    let mut removed = 0usize;
    while runtime.segments.len() > 1 {
      let Some(last_timestamp_ms) = runtime.segments[0].last_timestamp_ms else {
        break;
      };
      if now_ms.saturating_sub(last_timestamp_ms) < retention_ms {
        break;
      }
      Self::remove_oldest_segment(runtime)?;
      removed += 1;
    }
    Ok(removed)
  }

  fn enforce_partition_retention_by_size(
    runtime: &mut TopicRuntime,
    retention_max_bytes: u64,
  ) -> Result<usize> {
    if retention_max_bytes == 0 || runtime.segments.len() <= 1 {
      return Ok(0);
    }

    let mut total_bytes = runtime
      .segments
      .iter()
      .map(|segment| segment.next_write_pos)
      .sum::<u64>();
    if total_bytes <= retention_max_bytes {
      return Ok(0);
    }

    let mut removed = 0usize;
    while runtime.segments.len() > 1 && total_bytes > retention_max_bytes {
      total_bytes = total_bytes.saturating_sub(runtime.segments[0].next_write_pos);
      Self::remove_oldest_segment(runtime)?;
      removed += 1;
    }
    Ok(removed)
  }

  fn remove_oldest_segment(runtime: &mut TopicRuntime) -> Result<()> {
    let oldest = runtime.segments.remove(0);
    fs::remove_file(&oldest.log_path)?;
    fs::remove_file(&oldest.index_path)?;
    Ok(())
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

  fn collect_compacted_records(runtime: &TopicRuntime) -> Result<Vec<Record>> {
    use std::collections::HashMap;

    let mut records = Vec::new();
    for segment in &runtime.segments {
      records.extend(Self::read_records_from_segment(segment, segment.base_offset, usize::MAX)?);
    }

    let mut latest_offset_by_key = HashMap::new();
    for record in &records {
      if let Some(key) = &record.key {
        latest_offset_by_key.insert(key.clone(), record.offset);
      }
    }

    Ok(
      records
        .into_iter()
        .filter(|record| match &record.key {
          Some(key) => latest_offset_by_key.get(key) == Some(&record.offset)
            && Self::should_keep_compacted_keyed_record(runtime, record),
          None => true,
        })
        .collect(),
    )
  }

  fn latest_offset_by_key(runtime: &TopicRuntime) -> Result<std::collections::HashMap<Bytes, u64>> {
    use std::collections::HashMap;

    let mut latest_offset_by_key = HashMap::new();
    for segment in &runtime.segments {
      let records = Self::read_records_from_segment(segment, segment.base_offset, usize::MAX)?;
      for record in records {
        if let Some(key) = record.key {
          latest_offset_by_key.insert(key, record.offset);
        }
      }
    }
    Ok(latest_offset_by_key)
  }

  fn collect_compacted_segment_prefix_records(
    runtime: &TopicRuntime,
    prefix_segment_count: usize,
    latest_offset_by_key: &std::collections::HashMap<Bytes, u64>,
  ) -> Result<(Vec<Record>, usize)> {
    let mut preserved = Vec::new();
    let mut removed = 0usize;
    for segment in runtime.segments.iter().take(prefix_segment_count) {
      let records = Self::read_records_from_segment(segment, segment.base_offset, usize::MAX)?;
      for record in records {
        let keep = match &record.key {
          Some(key) => latest_offset_by_key.get(key) == Some(&record.offset)
            && Self::should_keep_compacted_keyed_record(runtime, &record),
          None => true,
        };
        if keep {
          preserved.push(record);
        } else {
          removed += 1;
        }
      }
    }
    Ok((preserved, removed))
  }

  fn should_keep_compacted_keyed_record(runtime: &TopicRuntime, record: &Record) -> bool {
    if !record.is_tombstone() {
      return true;
    }

    match runtime.config.compaction_tombstone_retention_ms {
      Some(retention_ms) => now_ms().saturating_sub(record.timestamp_ms) < retention_ms,
      None => false,
    }
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

      if Self::should_roll_segment_for_config(config, &active_snapshot, record) {
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
      if active.first_timestamp_ms.is_none() {
        active.first_timestamp_ms = Some(record.timestamp_ms);
      }
      active.last_timestamp_ms = Some(record.timestamp_ms);
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
      appends_since_checkpoint: 0,
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
    let key = topic_partition.clone();
    if !self.state.contains_key(&key) {
      let runtime = self.hydrate_runtime(topic_partition)?;
      self.state.insert(key.clone(), runtime);
    }

    let runtime = self
      .state
      .get(&key)
      .map(|e| e.value().clone())
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
    self.state.insert(key, rebuilt);
    Ok(())
  }

  fn compact_partition(&self, topic_partition: &TopicPartition, config: &TopicConfig) -> Result<usize> {
    let key = topic_partition.clone();
    if !self.state.contains_key(&key) {
      let runtime = self.hydrate_runtime(topic_partition)?;
      self.state.insert(key.clone(), runtime);
    }

    let runtime = self
      .state
      .get(&key)
      .map(|entry| entry.value().clone())
      .ok_or_else(|| StoreError::PartitionNotFound {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
      })?;

    let total_records = runtime
      .segments
      .iter()
      .try_fold(0usize, |count, segment| {
        Self::read_records_from_segment(segment, segment.base_offset, usize::MAX)
          .map(|records| count + records.len())
      })?;
    if total_records <= 1 {
      return Ok(0);
    }

    let (rebuilt, removed) = if runtime.segments.len() <= 1 {
      let compacted = Self::collect_compacted_records(&runtime)?;
      let removed = total_records.saturating_sub(compacted.len());
      if removed == 0 {
        return Ok(0);
      }
      (
        self.rebuild_partition_from_records(topic_partition, config, &compacted)?,
        removed,
      )
    } else {
      let sealed_segment_count = runtime.segments.len().saturating_sub(1);
      let prefix_segment_count = sealed_segment_count.min(
        self
          .options
          .compaction_sealed_segment_batch
          .max(1),
      );
      self.compact_sealed_segment_prefix(
        topic_partition,
        config,
        &runtime,
        prefix_segment_count,
      )?
    };
    self.state.insert(key, rebuilt);
    Ok(removed)
  }

  fn compact_sealed_segment_prefix(
    &self,
    topic_partition: &TopicPartition,
    config: &TopicConfig,
    runtime: &TopicRuntime,
    prefix_segment_count: usize,
  ) -> Result<(TopicRuntime, usize)> {
    let latest_offset_by_key = Self::latest_offset_by_key(runtime)?;
    let (preserved, removed) = Self::collect_compacted_segment_prefix_records(
      runtime,
      prefix_segment_count,
      &latest_offset_by_key,
    )?;
    if removed == 0 {
      return Ok((runtime.clone(), 0));
    }

    let partition_dir = self.partition_dir(topic_partition);
    let staging_dir = partition_dir.join(format!(
      ".compact-prefix-{}-{}",
      runtime
        .segments
        .first()
        .map(|segment| segment.base_offset)
        .unwrap_or(0),
      now_ms()
    ));
    let staged = self.rebuild_segments_in_dir(&staging_dir, config, &preserved)?;

    let result = (|| -> Result<(TopicRuntime, usize)> {
      for segment in runtime.segments.iter().take(prefix_segment_count) {
        fs::remove_file(&segment.log_path)?;
        fs::remove_file(&segment.index_path)?;
      }

      let mut compacted_segments = Vec::new();
      for segment in staged {
        let (target_log_path, target_index_path) =
          self.segment_paths(topic_partition, segment.base_offset);
        fs::rename(&segment.log_path, &target_log_path)?;
        fs::rename(&segment.index_path, &target_index_path)?;
        compacted_segments.push(self.recover_segment(
          segment.base_offset,
          target_log_path,
          target_index_path,
          config.index_interval_bytes,
        )?);
      }

      self.write_checkpoint(topic_partition, runtime.next_offset)?;
      let mut segments = compacted_segments;
      segments.extend(runtime.segments.iter().skip(prefix_segment_count).cloned());
      Ok((
        TopicRuntime {
          config: config.clone(),
          segments,
          next_offset: runtime.next_offset,
          appends_since_checkpoint: 0,
        },
        removed,
      ))
    })();

    if staging_dir.exists() {
      let _ = fs::remove_dir_all(&staging_dir);
    }
    result
  }

  fn rebuild_segments_in_dir(
    &self,
    dir: &Path,
    config: &TopicConfig,
    records: &[Record],
  ) -> Result<Vec<SegmentDescriptor>> {
    if dir.exists() {
      fs::remove_dir_all(dir)?;
    }
    fs::create_dir_all(dir)?;
    if records.is_empty() {
      return Ok(Vec::new());
    }

    let mut segments = vec![Self::create_segment_files_in_dir(dir, records[0].offset)?];
    for record in records {
      let active_snapshot = segments.last().cloned().ok_or_else(|| {
        StoreError::Corruption("missing staged segment while compacting prefix".to_owned())
      })?;
      if Self::should_roll_segment_for_config(config, &active_snapshot, record) {
        segments.push(Self::create_segment_files_in_dir(dir, record.offset)?);
      }

      let active = segments.last_mut().ok_or_else(|| {
        StoreError::Corruption("missing active staged segment while compacting prefix".to_owned())
      })?;
      let start_pos = active.next_write_pos;
      let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&active.log_path)?;
      let written = Self::append_existing_record(&mut file, record)?;
      active.next_write_pos += written as u64;
      if active.first_timestamp_ms.is_none() {
        active.first_timestamp_ms = Some(record.timestamp_ms);
      }
      active.last_timestamp_ms = Some(record.timestamp_ms);
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
    Ok(segments)
  }

  fn create_segment_files_in_dir(dir: &Path, base_offset: u64) -> Result<SegmentDescriptor> {
    let log_path = dir.join(Self::segment_file_name(base_offset, LOG_SUFFIX));
    let index_path = dir.join(Self::segment_file_name(base_offset, INDEX_SUFFIX));
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
      first_timestamp_ms: None,
      last_timestamp_ms: None,
      last_offset: None,
      last_indexed_pos: 0,
    })
  }

  fn should_roll_segment_for_config(
    config: &TopicConfig,
    active: &SegmentDescriptor,
    record: &Record,
  ) -> bool {
    (active.next_write_pos + Self::encoded_record_len(record) as u64) > config.segment_max_bytes
      && active.next_write_pos > 0
  }

  pub(super) fn validate_partition(&self, topic_partition: &TopicPartition) -> Result<TopicConfig> {
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
