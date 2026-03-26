use super::*;

impl SegmentLog {
  pub(super) fn hydrate_runtime(&self, topic_partition: &TopicPartition) -> Result<TopicRuntime> {
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
      appends_since_checkpoint: 0,
    })
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

  pub(super) fn write_checkpoint(
    &self,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> Result<()> {
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

  pub(super) fn create_segment_files(
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
      first_timestamp_ms: None,
      last_timestamp_ms: None,
      last_offset: None,
      last_indexed_pos: 0,
    })
  }

  pub(super) fn recover_segment(
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
      first_timestamp_ms: scan.first_timestamp_ms,
      last_timestamp_ms: scan.last_timestamp_ms,
      last_offset: scan.last_offset,
      last_indexed_pos,
    })
  }

  fn scan_segment_and_repair_tail(log_path: &Path) -> Result<RecoveryScanState> {
    let mut file = OpenOptions::new().read(true).write(true).open(log_path)?;
    let mut valid_len = 0u64;
    let mut first_timestamp_ms = None;
    let mut last_timestamp_ms = None;
    let mut last_offset = None;

    loop {
      file.seek(SeekFrom::Start(valid_len))?;
      match Self::read_record_from_file(&mut file)? {
        Ok((record, record_len)) => {
          valid_len += record_len as u64;
          if first_timestamp_ms.is_none() {
            first_timestamp_ms = Some(record.timestamp_ms);
          }
          last_timestamp_ms = Some(record.timestamp_ms);
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
      first_timestamp_ms,
      last_timestamp_ms,
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
}
