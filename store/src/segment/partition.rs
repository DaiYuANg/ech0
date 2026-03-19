use super::*;

impl SegmentLog {
  pub fn enforce_retention_once(&self) -> Result<usize> {
    let mut removed_segments = 0_usize;
    for topic in self.discover_topics()? {
      let config = self.load_topic_config(&topic)?;
      for partition in 0..config.partitions {
        let topic_partition = config.partition(partition);
        removed_segments +=
          self.enforce_partition_retention(&topic_partition, config.retention_max_bytes)?;
      }
    }
    Ok(removed_segments)
  }

  fn enforce_partition_retention(
    &self,
    topic_partition: &TopicPartition,
    retention_max_bytes: u64,
  ) -> Result<usize> {
    if retention_max_bytes == 0 {
      return Ok(0);
    }
    self.validate_partition(topic_partition)?;
    self.with_runtime(topic_partition, |runtime| {
      if runtime.segments.len() <= 1 {
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

      let mut removed = 0_usize;
      while runtime.segments.len() > 1 && total_bytes > retention_max_bytes {
        let oldest = runtime.segments.remove(0);
        total_bytes = total_bytes.saturating_sub(oldest.next_write_pos);
        fs::remove_file(oldest.log_path)?;
        fs::remove_file(oldest.index_path)?;
        removed += 1;
      }
      Ok(removed)
    })
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

  fn should_roll_segment_for_config(
    config: &TopicConfig,
    active: &SegmentDescriptor,
    payload_len: usize,
  ) -> bool {
    (active.next_write_pos + (RECORD_HEADER_LEN + payload_len) as u64) > config.segment_max_bytes
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
