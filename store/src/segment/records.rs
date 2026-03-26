use super::*;
use crate::RecordHeader;

impl SegmentLog {
  pub(super) fn should_roll_segment(
    runtime: &TopicRuntime,
    active: &SegmentDescriptor,
    record: &Record,
  ) -> bool {
    let projected_len = active.next_write_pos + Self::encoded_record_len(record) as u64;
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
    let key_bytes = record.key.as_ref().map(|k| k.as_ref()).unwrap_or(&[]);
    let key_len = key_bytes.len();
    let headers_count = record.headers.len();
    let payload_len = record.payload.len();
    if key_len > u32::MAX as usize {
      return Err(StoreError::Codec("record key exceeds max length".to_owned()));
    }
    if headers_count > u32::MAX as usize {
      return Err(StoreError::Codec("record headers exceed max count".to_owned()));
    }
    if payload_len > u32::MAX as usize {
      return Err(StoreError::Codec("record payload exceeds max length".to_owned()));
    }
    for header in &record.headers {
      if header.key.len() > u32::MAX as usize {
        return Err(StoreError::Codec("record header key exceeds max length".to_owned()));
      }
      if header.value.len() > u32::MAX as usize {
        return Err(StoreError::Codec("record header value exceeds max length".to_owned()));
      }
    }
    let checksum = Self::checksum(record);
    file.write_all(&[CURRENT_VERSION])?;
    file.write_all(&record.offset.to_le_bytes())?;
    file.write_all(&record.timestamp_ms.to_le_bytes())?;
    file.write_all(&(key_len as u32).to_le_bytes())?;
    file.write_all(&(headers_count as u32).to_le_bytes())?;
    file.write_all(&record.attributes.to_le_bytes())?;
    file.write_all(&(payload_len as u32).to_le_bytes())?;
    file.write_all(&checksum.to_le_bytes())?;
    file.write_all(key_bytes)?;
    for header in &record.headers {
      let key = header.key.as_bytes();
      file.write_all(&(key.len() as u32).to_le_bytes())?;
      file.write_all(&(header.value.len() as u32).to_le_bytes())?;
      file.write_all(key)?;
      file.write_all(header.value.as_ref())?;
    }
    file.write_all(record.payload.as_ref())?;
    Ok(Self::encoded_record_len(record))
  }

  fn checksum(record: &Record) -> u32 {
    let mut hasher = Hasher::new();
    let key_bytes = record.key.as_ref().map(|k| k.as_ref()).unwrap_or(&[]);
    hasher.update(&record.offset.to_le_bytes());
    hasher.update(&record.timestamp_ms.to_le_bytes());
    hasher.update(&(key_bytes.len() as u32).to_le_bytes());
    hasher.update(&(record.headers.len() as u32).to_le_bytes());
    hasher.update(&record.attributes.to_le_bytes());
    hasher.update(&(record.payload.len() as u32).to_le_bytes());
    hasher.update(key_bytes);
    for header in &record.headers {
      hasher.update(&(header.key.len() as u32).to_le_bytes());
      hasher.update(&(header.value.len() as u32).to_le_bytes());
      hasher.update(header.key.as_bytes());
      hasher.update(header.value.as_ref());
    }
    hasher.update(record.payload.as_ref());
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
    let key_len = u32::from_le_bytes(header[17..21].try_into().unwrap()) as usize;
    let headers_count = u32::from_le_bytes(header[21..25].try_into().unwrap()) as usize;
    let attributes = u16::from_le_bytes(header[25..27].try_into().unwrap());
    let payload_len = u32::from_le_bytes(header[27..31].try_into().unwrap()) as usize;
    let checksum = u32::from_le_bytes(header[31..35].try_into().unwrap());

    let mut key = vec![0u8; key_len];
    match file.read_exact(&mut key) {
      Ok(()) => {}
      Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
        return Ok(Err(ReadRecordOutcome::TruncatedTail));
      }
      Err(err) => return Err(err.into()),
    }
    let mut headers = Vec::with_capacity(headers_count);
    for _ in 0..headers_count {
      let mut header_lens = [0u8; 8];
      match file.read_exact(&mut header_lens) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
          return Ok(Err(ReadRecordOutcome::TruncatedTail));
        }
        Err(err) => return Err(err.into()),
      }
      let header_key_len = u32::from_le_bytes(header_lens[0..4].try_into().unwrap()) as usize;
      let header_value_len = u32::from_le_bytes(header_lens[4..8].try_into().unwrap()) as usize;
      let mut header_key = vec![0u8; header_key_len];
      match file.read_exact(&mut header_key) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
          return Ok(Err(ReadRecordOutcome::TruncatedTail));
        }
        Err(err) => return Err(err.into()),
      }
      let header_key = String::from_utf8(header_key)
        .map_err(|err| StoreError::Corruption(format!("record header key is not utf8: {err}")))?;
      let mut header_value = vec![0u8; header_value_len];
      match file.read_exact(&mut header_value) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
          return Ok(Err(ReadRecordOutcome::TruncatedTail));
        }
        Err(err) => return Err(err.into()),
      }
      headers.push(RecordHeader {
        key: header_key,
        value: Bytes::from(header_value),
      });
    }
    let mut payload = vec![0u8; payload_len];
    match file.read_exact(&mut payload) {
      Ok(()) => {}
      Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
        return Ok(Err(ReadRecordOutcome::TruncatedTail));
      }
      Err(err) => return Err(err.into()),
    }
    let record = Record {
      offset,
      timestamp_ms,
      key: if key.is_empty() {
        None
      } else {
        Some(Bytes::from(key))
      },
      headers,
      attributes,
      payload: Bytes::from(payload),
    };
    let computed = Self::checksum(&record);
    if checksum != computed {
      return Err(StoreError::Corruption(format!(
        "record checksum mismatch at offset {offset}: expected {checksum}, got {computed}"
      )));
    }
    let record_len = Self::encoded_record_len(&record);
    Ok(Ok((record, record_len)))
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

  pub(super) fn encoded_record_len(record: &Record) -> usize {
    let key_len = record.key.as_ref().map(|k| k.len()).unwrap_or(0);
    let headers_len = record
      .headers
      .iter()
      .map(|header| 4 + 4 + header.key.len() + header.value.len())
      .sum::<usize>();
    RECORD_HEADER_LEN + key_len + headers_len + record.payload.len()
  }
}
