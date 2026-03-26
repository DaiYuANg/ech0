use super::*;
use crate::RecordAppend;

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
    for partition in 0..topic.partitions {
      let dir = self.partition_dir(&topic.partition(partition));
      fs::create_dir_all(&dir)?;
      let segment = self.create_segment_files(&topic.partition(partition), 0)?;
      self.write_checkpoint(&topic.partition(partition), 0)?;
      self.state.insert(
        topic.partition(partition),
        TopicRuntime {
          config: topic.clone(),
          segments: vec![segment],
          next_offset: 0,
          appends_since_checkpoint: 0,
        },
      );
    }
    Ok(())
  }

  fn topic_exists(&self, topic: &str) -> Result<bool> {
    Ok(self.topic_root_dir(topic).exists())
  }

  fn append_record(&self, topic_partition: &TopicPartition, record: RecordAppend) -> Result<Record> {
    self.validate_partition(topic_partition)?;
    self.with_runtime(topic_partition, |runtime| {
      let active = runtime
        .segments
        .last()
        .cloned()
        .ok_or_else(|| StoreError::Corruption("missing active segment".to_owned()))?;

      let next_offset = runtime.next_offset;
      let record = Record {
        offset: next_offset,
        timestamp_ms: record.timestamp_ms.unwrap_or_else(now_ms),
        key: record.key,
        headers: record.headers,
        attributes: record.attributes,
        payload: record.payload,
      };
      if Self::should_roll_segment(runtime, &active, &record) {
        let next_base = runtime.next_offset;
        let segment = self.create_segment_files(topic_partition, next_base)?;
        runtime.segments.push(segment);
      }

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
      if active.first_timestamp_ms.is_none() {
        active.first_timestamp_ms = Some(record.timestamp_ms);
      }
      active.last_timestamp_ms = Some(record.timestamp_ms);
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
      runtime.appends_since_checkpoint += 1;
      if runtime.appends_since_checkpoint >= self.options.checkpoint_interval {
        self.write_checkpoint(topic_partition, runtime.next_offset)?;
        runtime.appends_since_checkpoint = 0;
      }
      Ok(record)
    })
  }

  fn append_records_batch(
    &self,
    topic_partition: &TopicPartition,
    records: Vec<RecordAppend>,
  ) -> Result<Vec<Record>> {
    let total_records = records.len();
    if total_records == 0 {
      return Ok(Vec::new());
    }
    self.validate_partition(topic_partition)?;
    self.with_runtime(topic_partition, |runtime| {
      let mut appended = Vec::with_capacity(total_records);
      let mut file = None;

      for (i, record) in records.into_iter().enumerate() {
        let active = runtime
          .segments
          .last()
          .cloned()
          .ok_or_else(|| StoreError::Corruption("missing active segment".to_owned()))?;

        let record = Record {
          offset: runtime.next_offset,
          timestamp_ms: record.timestamp_ms.unwrap_or_else(now_ms),
          key: record.key,
          headers: record.headers,
          attributes: record.attributes,
          payload: record.payload,
        };
        if Self::should_roll_segment(runtime, &active, &record) {
          file = None;
          let next_base = runtime.next_offset;
          let segment = self.create_segment_files(topic_partition, next_base)?;
          runtime.segments.push(segment);
        }

        let active = runtime.segments.last_mut().ok_or_else(|| {
          StoreError::Corruption("missing active segment after rollover".to_owned())
        })?;

        let active_path = active.log_path.clone();
        if file
          .as_ref()
          .map(|(path, _)| path != &active_path)
          .unwrap_or(true)
        {
          file = Some((
            active_path.clone(),
            OpenOptions::new()
              .create(true)
              .append(true)
              .open(&active.log_path)?,
          ));
        }

        let start_pos = active.next_write_pos;
        let (_, file_handle) = file
          .as_mut()
          .ok_or_else(|| StoreError::Corruption("missing active log file handle".to_owned()))?;
        let written = Self::write_record_no_flush(file_handle, &record)?;
        if i + 1 == total_records {
          file_handle.flush()?;
        }
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
            >= runtime.config.index_interval_bytes
        {
          Self::append_index_entry(&active.index_path, record.offset, start_pos)?;
          active.last_indexed_pos = start_pos;
        }

        runtime.next_offset += 1;
        runtime.appends_since_checkpoint += 1;
        appended.push(record);
      }

      if runtime.appends_since_checkpoint >= self.options.checkpoint_interval {
        self.write_checkpoint(topic_partition, runtime.next_offset)?;
        runtime.appends_since_checkpoint = 0;
      }
      Ok(appended)
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

  fn append_batch(
    &self,
    topic_partition: &TopicPartition,
    payloads: &[Vec<u8>],
  ) -> Result<Vec<Record>> {
    self.append_records_batch(
      topic_partition,
      payloads
        .iter()
        .map(|payload| RecordAppend::new(Bytes::copy_from_slice(payload)))
        .collect(),
    )
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
