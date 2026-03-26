use super::*;
use crate::{
  InMemoryStore, LocalPartitionStateStore, MessageLogStore, PartitionAvailability, TopicConfig,
};

#[test]
fn append_command_updates_local_partition_state() {
  let log = InMemoryStore::new();
  let states = InMemoryStore::new();
  log.create_topic(TopicConfig::new("orders")).unwrap();
  let executor = LocalPartitionCommandExecutor::new(log, states);
  let tp = TopicPartition::new("orders", 0);

  let result = executor
    .apply(LocalPartitionCommand::Append {
      topic_partition: tp.clone(),
      record: RecordAppend::new(b"hello".to_vec()),
    })
    .unwrap();

  assert_eq!(result, ApplyResult::Appended { next_offset: 1 });
  assert_eq!(executor.stores().0.last_offset(&tp).unwrap(), Some(0));
  let state = executor
    .stores()
    .1
    .load_local_partition_state(&tp)
    .unwrap()
    .unwrap();
  assert_eq!(state.availability, PartitionAvailability::Online);
  assert_eq!(state.state.last_appended_offset, Some(0));
}

#[test]
fn truncate_command_rewrites_partition_state() {
  let log = InMemoryStore::new();
  let states = InMemoryStore::new();
  let mut topic = TopicConfig::new("orders");
  topic.partitions = 2;
  log.create_topic(topic).unwrap();
  let executor = LocalPartitionCommandExecutor::new(log, states);
  let tp = TopicPartition::new("orders", 1);

  executor
    .apply(LocalPartitionCommand::Append {
      topic_partition: tp.clone(),
      record: RecordAppend::new(b"a".to_vec()),
    })
    .unwrap();
  executor
    .apply(LocalPartitionCommand::Append {
      topic_partition: tp.clone(),
      record: RecordAppend::new(b"b".to_vec()),
    })
    .unwrap();

  let result = executor
    .apply(LocalPartitionCommand::Truncate {
      topic_partition: tp.clone(),
      offset: 1,
    })
    .unwrap();

  assert_eq!(result, ApplyResult::Truncated { next_offset: 1 });
  assert_eq!(executor.stores().0.last_offset(&tp).unwrap(), Some(0));
  let state = executor
    .stores()
    .1
    .load_local_partition_state(&tp)
    .unwrap()
    .unwrap();
  assert_eq!(state.state.high_watermark, Some(0));
}

#[test]
fn envelope_json_round_trip_preserves_replicated_command_shape() {
  let envelope = PartitionCommandEnvelope::new(
    LocalPartitionCommand::Append {
      topic_partition: TopicPartition::new("orders", 2),
      record: RecordAppend::new(b"hello".to_vec()),
    },
    CommandSource::Consensus,
  )
  .with_leader_epoch(7);

  let bytes = envelope.encode_json().unwrap();
  let decoded = PartitionCommandEnvelope::decode_json(&bytes).unwrap();

  assert_eq!(decoded.source, CommandSource::Consensus);
  assert_eq!(decoded.leader_epoch, Some(7));
  assert!(decoded.command.is_replicated_command());
  assert_eq!(
    decoded.command.topic_partition(),
    &TopicPartition::new("orders", 2)
  );
}

#[test]
fn local_only_commands_are_rejected_from_replicated_envelopes() {
  let envelope = PartitionCommandEnvelope::new(
    LocalPartitionCommand::RecoverPartition {
      topic_partition: TopicPartition::new("orders", 0),
    },
    CommandSource::Recovery,
  );

  let err = envelope.replicated_command().unwrap_err();
  assert!(matches!(err, StoreError::Unsupported(_)));
}
