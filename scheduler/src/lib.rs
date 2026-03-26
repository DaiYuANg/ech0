use store::{
  MessageLogStore, OffsetStore, Record, RecordAppend, RecordHeader, Result, StoreError,
  TopicCatalogStore, TopicConfig, TopicPartition,
};

pub const INTERNAL_DELAY_TOPIC_PREFIX: &str = "__delay";
pub const HEADER_DELAY_DELIVER_AT_MS: &str = "x-delay-deliver-at-ms";
pub const HEADER_DELAY_TARGET_TOPIC: &str = "x-delay-target-topic";
pub const HEADER_DELAY_TARGET_PARTITION: &str = "x-delay-target-partition";

pub fn delay_topic_name(target_topic: &str) -> String {
  format!("{INTERNAL_DELAY_TOPIC_PREFIX}.{target_topic}")
}

pub fn is_delay_topic_name(topic: &str) -> bool {
  topic.starts_with(INTERNAL_DELAY_TOPIC_PREFIX)
    && (topic == INTERNAL_DELAY_TOPIC_PREFIX
      || topic.as_bytes().get(INTERNAL_DELAY_TOPIC_PREFIX.len()) == Some(&b'.')
      || topic.as_bytes().get(INTERNAL_DELAY_TOPIC_PREFIX.len()) == Some(&b'/'))
}

pub fn enqueue_delayed<L, M>(
  log: &L,
  meta: &M,
  target_topic: &str,
  partition: u32,
  payload: Vec<u8>,
  deliver_at_ms: u64,
) -> Result<Record>
where
  L: MessageLogStore,
  M: TopicCatalogStore,
{
  let Some(target_config) = meta.load_topic_config(target_topic)? else {
    return Err(StoreError::TopicNotFound(target_topic.to_owned()));
  };
  if partition >= target_config.partitions {
    return Err(StoreError::PartitionNotFound {
      topic: target_topic.to_owned(),
      partition,
    });
  }
  let delay_topic = delay_topic_name(target_topic);
  ensure_delay_topic(log, meta, &target_config, &delay_topic)?;
  let mut append = RecordAppend::new(payload);
  append.headers.push(header(
    HEADER_DELAY_DELIVER_AT_MS,
    deliver_at_ms.to_string(),
  ));
  append.headers.push(header(HEADER_DELAY_TARGET_TOPIC, target_topic));
  append.headers.push(header(
    HEADER_DELAY_TARGET_PARTITION,
    partition.to_string(),
  ));
  log.append_record(&TopicPartition::new(delay_topic, partition), append)
}

pub fn process_due_once<L, M>(
  log: &L,
  meta: &M,
  consumer_prefix: &str,
  max_records_per_partition: usize,
  now_ms: u64,
) -> Result<usize>
where
  L: MessageLogStore,
  M: OffsetStore + TopicCatalogStore,
{
  let mut moved_total = 0usize;
  for topic in meta.list_topics()? {
    if !is_delay_topic_name(&topic.name) {
      continue;
    }
    let default_target = topic
      .name
      .strip_prefix(&format!("{INTERNAL_DELAY_TOPIC_PREFIX}."))
      .unwrap_or_default()
      .to_owned();
    for partition in 0..topic.partitions {
      let consumer = format!("{consumer_prefix}:{}:{partition}", topic.name);
      moved_total += process_delay_partition(
        log,
        meta,
        &topic.name,
        &default_target,
        partition,
        &consumer,
        max_records_per_partition.max(1),
        now_ms,
      )?;
    }
  }
  Ok(moved_total)
}

fn process_delay_partition<L, M>(
  log: &L,
  meta: &M,
  delay_topic: &str,
  default_target_topic: &str,
  partition: u32,
  consumer: &str,
  max_records: usize,
  now_ms: u64,
) -> Result<usize>
where
  L: MessageLogStore,
  M: OffsetStore + TopicCatalogStore,
{
  let delay_tp = TopicPartition::new(delay_topic, partition);
  let start_offset = meta.load_consumer_offset(consumer, &delay_tp)?.unwrap_or(0);
  let records = log.read_from(&delay_tp, start_offset, max_records)?;
  if records.is_empty() {
    return Ok(0);
  }

  let mut moved = 0usize;
  let mut commit_next_offset: Option<u64> = None;
  for record in records {
    let record_offset = record.offset;
    let deliver_at = header_u64(&record, HEADER_DELAY_DELIVER_AT_MS)?
      .ok_or_else(|| StoreError::Codec("delay record missing deliver_at_ms header".to_owned()))?;
    if deliver_at > now_ms {
      break;
    }
    let target_topic = header_string(&record, HEADER_DELAY_TARGET_TOPIC)
      .unwrap_or_else(|| default_target_topic.to_owned());
    let target_partition =
      header_u32(&record, HEADER_DELAY_TARGET_PARTITION)?.unwrap_or(partition);
    let append = strip_delay_headers(record);
    log.append_record(&TopicPartition::new(target_topic, target_partition), append)?;
    moved += 1;
    commit_next_offset = Some(record_offset + 1);
  }

  if let Some(next_offset) = commit_next_offset {
    meta.save_consumer_offset(consumer, &delay_tp, next_offset)?;
  }
  Ok(moved)
}

fn ensure_delay_topic<L, M>(
  log: &L,
  meta: &M,
  target: &TopicConfig,
  delay_topic: &str,
) -> Result<()>
where
  L: MessageLogStore,
  M: TopicCatalogStore,
{
  if log.topic_exists(delay_topic)? {
    return Ok(());
  }
  let mut config = TopicConfig::new(delay_topic.to_owned());
  config.partitions = target.partitions;
  config.retention_max_bytes = target.retention_max_bytes;
  config.retention_ms = target.retention_ms;
  config.max_message_bytes = target.max_message_bytes;
  config.max_batch_bytes = target.max_batch_bytes;
  config.cleanup_policy = store::TopicCleanupPolicy::Delete;
  config.compaction_enabled = false;
  log.create_topic(config.clone())?;
  meta.save_topic_config(&config)
}

fn strip_delay_headers(record: Record) -> RecordAppend {
  let mut append = RecordAppend::new(record.payload);
  append.timestamp_ms = Some(record.timestamp_ms);
  append.key = record.key;
  append.attributes = record.attributes;
  append.headers = record
    .headers
    .into_iter()
    .filter(|header| {
      header.key != HEADER_DELAY_DELIVER_AT_MS
        && header.key != HEADER_DELAY_TARGET_TOPIC
        && header.key != HEADER_DELAY_TARGET_PARTITION
    })
    .collect();
  append
}

fn header(key: &str, value: impl Into<String>) -> RecordHeader {
  RecordHeader {
    key: key.to_owned(),
    value: value.into().into_bytes().into(),
  }
}

fn header_string(record: &Record, key: &str) -> Option<String> {
  let value = record.headers.iter().find(|h| h.key == key)?;
  String::from_utf8(value.value.to_vec()).ok()
}

fn header_u64(record: &Record, key: &str) -> Result<Option<u64>> {
  match header_string(record, key) {
    Some(raw) => raw
      .parse::<u64>()
      .map(Some)
      .map_err(|err| StoreError::Codec(format!("invalid header {key}: {err}"))),
    None => Ok(None),
  }
}

fn header_u32(record: &Record, key: &str) -> Result<Option<u32>> {
  match header_string(record, key) {
    Some(raw) => raw
      .parse::<u32>()
      .map(Some)
      .map_err(|err| StoreError::Codec(format!("invalid header {key}: {err}"))),
    None => Ok(None),
  }
}

#[cfg(test)]
mod tests {
  use super::{delay_topic_name, enqueue_delayed, process_due_once};
  use store::{InMemoryStore, MessageLogStore, OffsetStore, TopicCatalogStore, TopicConfig, TopicPartition};

  #[test]
  fn delayed_record_is_forwarded_when_due() {
    let log = InMemoryStore::new();
    let meta = InMemoryStore::new();
    log.create_topic(TopicConfig::new("orders")).unwrap();
    meta.save_topic_config(&TopicConfig::new("orders")).unwrap();
    enqueue_delayed(&log, &meta, "orders", 0, b"hello".to_vec(), 100).unwrap();

    let moved = process_due_once(&log, &meta, "delay-worker", 10, 100).unwrap();
    assert_eq!(moved, 1);

    let records = log.read_from(&TopicPartition::new("orders", 0), 0, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, b"hello".to_vec());

    let committed = meta
      .load_consumer_offset("delay-worker:__delay.orders:0", &TopicPartition::new("__delay.orders", 0))
      .unwrap();
    assert_eq!(committed, Some(1));
  }

  #[test]
  fn delayed_record_stays_when_not_due() {
    let log = InMemoryStore::new();
    let meta = InMemoryStore::new();
    log.create_topic(TopicConfig::new("orders")).unwrap();
    meta.save_topic_config(&TopicConfig::new("orders")).unwrap();
    enqueue_delayed(&log, &meta, "orders", 0, b"hello".to_vec(), 200).unwrap();

    let moved = process_due_once(&log, &meta, "delay-worker", 10, 100).unwrap();
    assert_eq!(moved, 0);
    assert_eq!(delay_topic_name("orders"), "__delay.orders");
  }
}
