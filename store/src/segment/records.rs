use super::*;

impl SegmentLog {
  pub(super) fn should_roll_segment(
    runtime: &TopicRuntime,
    active: &SegmentDescriptor,
    payload_len: usize,
  ) -> bool {
    let projected_len = active.next_write_pos + (RECORD_HEADER_LEN + payload_len) as u64;
    projected_len > runtime.config.segment_max_bytes && active.next_write_pos > 0
  }

  pub(super) fn append_index_entry(index_path: &Path, offset: u64, position: u64) -> Result<()> {
    let mut file = OpenOptions::new()
      .create(true)
      .append(true)
      .open(index_path)?;
    file.write_all(&offset.to_le_bytes())?;
    file.write_all(&position.to_le_bytes())?;
    file.flush()?;
    Ok(())
  }

  pub(super) fn write_record(file: &mut File, record: &Record) -> Result<usize> {
    let n = Self::write_record_no_flush(file, record)?;
    file.flush()?;
    Ok(n)
  }

  pub(super) fn write_record_no_flush(file: &mut File, record: &Record) -> Result<usize> {
    let checksum = Self::checksum(record.offset, record.timestamp_ms, record.payload.as_ref());
    file.write_all(&[CURRENT_VERSION])?;
    file.write_all(&record.offset.to_le_bytes())?;
    file.write_all(&record.timestamp_ms.to_le_bytes())?;
    file.write_all(&(record.payload.len() as u32).to_le_bytes())?;
    file.write_all(&checksum.to_le_bytes())?;
    file.write_all(record.payload.as_ref())?;
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

  pub(super) fn read_record_from_file(
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

  pub(super) fn read_records_from_segment(
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

  pub(super) fn append_existing_record(file: &mut File, record: &Record) -> Result<usize> {
    Self::write_record(file, record)
  }
}
