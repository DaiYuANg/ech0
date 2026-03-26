use super::*;
use crate::{
  BrokerState, JsonCodec, PartitionAvailability, RecordAppend, RedbMetadataStore,
  TopicCatalogStore,
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

#[test]
fn retention_cleanup_removes_old_segments() {
  let root = temp_path("retention-cleanup");
  let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
  let mut topic = TopicConfig::new("orders");
  topic.partitions = 1;
  topic.segment_max_bytes = 70;
  topic.index_interval_bytes = 8;
  topic.retention_max_bytes = 90;
  let topic_partition = topic.partition(0);
  log.create_topic(topic).unwrap();

  for idx in 0..8u8 {
    let payload = vec![idx; 16];
    log.append(&topic_partition, &payload).unwrap();
  }

  let removed = log.enforce_retention_once().unwrap();
  assert!(removed > 0);

  let records = log.read_from(&topic_partition, 0, 100).unwrap();
  assert!(!records.is_empty());
  assert!(records.len() < 8);
}

#[test]
fn retention_cleanup_removes_expired_segments_by_age() {
  let root = temp_path("retention-cleanup-age");
  let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
  let mut topic = TopicConfig::new("events");
  topic.partitions = 1;
  topic.segment_max_bytes = 80;
  topic.index_interval_bytes = 8;
  topic.retention_ms = Some(1_000);
  let topic_partition = topic.partition(0);
  log.create_topic(topic).unwrap();

  let old_timestamp = now_ms().saturating_sub(10_000);
  for idx in 0..2u8 {
    let mut record = RecordAppend::new(vec![idx; 16]);
    record.timestamp_ms = Some(old_timestamp);
    log.append_record(&topic_partition, record).unwrap();
  }

  for idx in 2..4u8 {
    let mut record = RecordAppend::new(vec![idx; 16]);
    record.timestamp_ms = Some(now_ms());
    log.append_record(&topic_partition, record).unwrap();
  }

  let removed = log.enforce_retention_once().unwrap();
  assert!(removed > 0);

  let records = log.read_from(&topic_partition, 0, 100).unwrap();
  assert_eq!(
    records.iter().map(|record| record.offset).collect::<Vec<_>>(),
    vec![2, 3]
  );
}

#[test]
fn compact_only_topics_skip_delete_retention_cleanup() {
  let root = temp_path("retention-cleanup-compact-only");
  let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
  let mut topic = TopicConfig::new("compacted");
  topic.partitions = 1;
  topic.segment_max_bytes = 70;
  topic.index_interval_bytes = 8;
  topic.retention_max_bytes = 90;
  topic.cleanup_policy = TopicCleanupPolicy::Compact;
  topic.compaction_enabled = true;
  let topic_partition = topic.partition(0);
  log.create_topic(topic).unwrap();

  for idx in 0..8u8 {
    let payload = vec![idx; 16];
    log.append(&topic_partition, &payload).unwrap();
  }

  let removed = log.enforce_retention_once().unwrap();
  assert_eq!(removed, 0);

  let records = log.read_from(&topic_partition, 0, 100).unwrap();
  assert_eq!(records.len(), 8);
}

#[test]
fn compaction_keeps_latest_record_per_key_and_preserves_keyless_records() {
  let root = temp_path("compaction-keeps-latest");
  let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
  let mut topic = TopicConfig::new("orders");
  topic.cleanup_policy = TopicCleanupPolicy::CompactAndDelete;
  topic.compaction_enabled = true;
  let topic_partition = topic.partition(0);
  log.create_topic(topic).unwrap();

  let mut first = RecordAppend::new(b"old-a".to_vec());
  first.key = Some(bytes::Bytes::from_static(b"a"));
  log.append_record(&topic_partition, first).unwrap();

  let mut keyless = RecordAppend::new(b"event".to_vec());
  keyless.key = None;
  log.append_record(&topic_partition, keyless).unwrap();

  let mut second = RecordAppend::new(b"new-a".to_vec());
  second.key = Some(bytes::Bytes::from_static(b"a"));
  log.append_record(&topic_partition, second).unwrap();

  let mut third = RecordAppend::new(b"only-b".to_vec());
  third.key = Some(bytes::Bytes::from_static(b"b"));
  log.append_record(&topic_partition, third).unwrap();

  let (compacted_partitions, removed_records) = log.compact_once().unwrap();
  assert_eq!(compacted_partitions, 1);
  assert_eq!(removed_records, 1);

  let records = log.read_from(&topic_partition, 0, 100).unwrap();
  assert_eq!(records.len(), 3);
  assert_eq!(records[0].payload.as_ref(), b"event");
  assert_eq!(records[1].payload.as_ref(), b"new-a");
  assert_eq!(records[2].payload.as_ref(), b"only-b");
  assert_eq!(records[1].key.as_ref().map(|key| key.as_ref()), Some(&b"a"[..]));
  assert_eq!(records[2].key.as_ref().map(|key| key.as_ref()), Some(&b"b"[..]));
}

#[test]
fn compaction_drops_tombstoned_keys() {
  let root = temp_path("compaction-drops-tombstones");
  let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
  let mut topic = TopicConfig::new("orders");
  topic.cleanup_policy = TopicCleanupPolicy::CompactAndDelete;
  topic.compaction_enabled = true;
  let topic_partition = topic.partition(0);
  log.create_topic(topic).unwrap();

  let mut original = RecordAppend::new(b"value".to_vec());
  original.key = Some(bytes::Bytes::from_static(b"a"));
  log.append_record(&topic_partition, original).unwrap();

  let mut tombstone = RecordAppend::new(Vec::new());
  tombstone.key = Some(bytes::Bytes::from_static(b"a"));
  tombstone.attributes |= crate::RECORD_ATTRIBUTE_TOMBSTONE;
  log.append_record(&topic_partition, tombstone).unwrap();

  let keyless = RecordAppend::new(b"event".to_vec());
  log.append_record(&topic_partition, keyless).unwrap();

  let (compacted_partitions, removed_records) = log.compact_once().unwrap();
  assert_eq!(compacted_partitions, 1);
  assert_eq!(removed_records, 2);

  let records = log.read_from(&topic_partition, 0, 100).unwrap();
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].payload.as_ref(), b"event");
}

#[test]
fn append_records_batch_preserves_keyed_and_tombstone_records() {
  let root = temp_path("append-records-batch");
  let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
  let topic = TopicConfig::new("orders");
  let topic_partition = topic.partition(0);
  log.create_topic(topic).unwrap();

  let mut keyed = RecordAppend::new(b"value".to_vec());
  keyed.key = Some(bytes::Bytes::from_static(b"k1"));
  let mut tombstone = RecordAppend::new(Vec::new());
  tombstone.key = Some(bytes::Bytes::from_static(b"k2"));
  tombstone.attributes |= crate::RECORD_ATTRIBUTE_TOMBSTONE;

  let appended = log
    .append_records_batch(&topic_partition, vec![keyed, tombstone])
    .unwrap();
  assert_eq!(appended.len(), 2);
  assert_eq!(appended[0].offset, 0);
  assert_eq!(appended[0].key.as_ref().map(|key| key.as_ref()), Some(&b"k1"[..]));
  assert_eq!(appended[1].offset, 1);
  assert!(appended[1].is_tombstone());

  let records = log.read_from(&topic_partition, 0, 10).unwrap();
  assert_eq!(records, appended);
}

#[test]
fn compaction_keeps_recent_tombstones_with_retention_window() {
  let root = temp_path("compaction-keeps-recent-tombstones");
  let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
  let mut topic = TopicConfig::new("orders");
  topic.cleanup_policy = TopicCleanupPolicy::CompactAndDelete;
  topic.compaction_enabled = true;
  topic.compaction_tombstone_retention_ms = Some(60_000);
  let topic_partition = topic.partition(0);
  log.create_topic(topic).unwrap();

  let mut original = RecordAppend::new(b"value".to_vec());
  original.key = Some(bytes::Bytes::from_static(b"a"));
  log.append_record(&topic_partition, original).unwrap();

  let mut tombstone = RecordAppend::new(Vec::new());
  tombstone.key = Some(bytes::Bytes::from_static(b"a"));
  tombstone.attributes |= crate::RECORD_ATTRIBUTE_TOMBSTONE;
  tombstone.timestamp_ms = Some(now_ms());
  log.append_record(&topic_partition, tombstone).unwrap();

  let (compacted_partitions, removed_records) = log.compact_once().unwrap();
  assert_eq!(compacted_partitions, 1);
  assert_eq!(removed_records, 1);

  let records = log.read_from(&topic_partition, 0, 100).unwrap();
  assert_eq!(records.len(), 1);
  assert!(records[0].is_tombstone());
  assert_eq!(records[0].key.as_ref().map(|key| key.as_ref()), Some(&b"a"[..]));
}

#[test]
fn compaction_progresses_incrementally_across_sealed_segment_prefixes() {
  let root = temp_path("compaction-incremental-prefixes");
  let log = SegmentLog::open(SegmentLogOptions::new(root.join("segments"))).unwrap();
  let mut topic = TopicConfig::new("orders");
  topic.cleanup_policy = TopicCleanupPolicy::CompactAndDelete;
  topic.compaction_enabled = true;
  topic.segment_max_bytes = 80;
  topic.index_interval_bytes = 8;
  let topic_partition = topic.partition(0);
  log.create_topic(topic).unwrap();

  for payload in [b"v0-aaaaaaaaaaaaaaaaaaaa".to_vec(), b"v1-bbbbbbbbbbbbbbbbbbbb".to_vec()] {
    let mut record = RecordAppend::new(payload);
    record.key = Some(bytes::Bytes::from_static(b"k"));
    log.append_record(&topic_partition, record).unwrap();
  }
  let mut old = RecordAppend::new(b"old-ccccccccccccccccccc".to_vec());
  old.key = Some(bytes::Bytes::from_static(b"k"));
  log.append_record(&topic_partition, old).unwrap();

  let mut new = RecordAppend::new(b"new-ddddddddddddddddddd".to_vec());
  new.key = Some(bytes::Bytes::from_static(b"k"));
  log.append_record(&topic_partition, new).unwrap();

  let segment_log_count = || {
    std::fs::read_dir(root.join("segments/orders/0"))
      .unwrap()
      .filter_map(|entry| entry.ok())
      .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("log"))
      .count()
  };

  assert_eq!(segment_log_count(), 4);

  let (first_partitions, first_removed) = log.compact_once().unwrap();
  assert_eq!(first_partitions, 1);
  assert_eq!(first_removed, 2);
  assert_eq!(segment_log_count(), 2);

  let records_after_first = log.read_from(&topic_partition, 0, 100).unwrap();
  assert_eq!(records_after_first.len(), 2);
  assert_eq!(records_after_first[0].payload.as_ref(), b"old-ccccccccccccccccccc");
  assert_eq!(records_after_first[1].payload.as_ref(), b"new-ddddddddddddddddddd");

  let (second_partitions, second_removed) = log.compact_once().unwrap();
  assert_eq!(second_partitions, 1);
  assert_eq!(second_removed, 1);
  assert_eq!(segment_log_count(), 1);

  let records_after_second = log.read_from(&topic_partition, 0, 100).unwrap();
  assert_eq!(records_after_second.len(), 1);
  assert_eq!(records_after_second[0].payload.as_ref(), b"new-ddddddddddddddddddd");
}
