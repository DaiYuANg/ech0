use std::{collections::HashMap, sync::RwLock};

use bytes::Bytes;

use crate::{
  Result, StoreError,
  model::{Record, TopicConfig, TopicPartition},
  traits::{MessageLogStore, MutablePartitionLogStore, OffsetStore},
};

#[derive(Debug, Default)]
pub struct InMemoryStore {
  topics: RwLock<HashMap<TopicPartition, Vec<Record>>>,
  consumer_offsets: RwLock<HashMap<(String, TopicPartition), u64>>,
}

impl InMemoryStore {
  pub fn new() -> Self {
    Self::default()
  }
}

impl MessageLogStore for InMemoryStore {
  fn create_topic(&self, topic: TopicConfig) -> Result<()> {
    let mut topics = self.topics.write().expect("poisoned topics lock");
    if (0..topic.partitions).any(|partition| topics.contains_key(&topic.partition(partition))) {
      return Err(StoreError::TopicAlreadyExists(topic.name));
    }
    for partition in 0..topic.partitions {
      topics.insert(topic.partition(partition), Vec::new());
    }
    Ok(())
  }

  fn topic_exists(&self, topic: &str) -> Result<bool> {
    let topics = self.topics.read().expect("poisoned topics lock");
    Ok(
      topics
        .keys()
        .any(|topic_partition| topic_partition.topic == topic),
    )
  }

  fn append(&self, topic_partition: &TopicPartition, payload: &[u8]) -> Result<Record> {
    let mut topics = self.topics.write().expect("poisoned topics lock");
    let entries = topics
      .get_mut(topic_partition)
      .ok_or_else(|| StoreError::PartitionNotFound {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
      })?;

    let record = Record::new(entries.len() as u64, Bytes::copy_from_slice(payload));
    entries.push(record.clone());
    Ok(record)
  }

  fn read_from(
    &self,
    topic_partition: &TopicPartition,
    offset: u64,
    max_records: usize,
  ) -> Result<Vec<Record>> {
    let topics = self.topics.read().expect("poisoned topics lock");
    let entries = topics
      .get(topic_partition)
      .ok_or_else(|| StoreError::PartitionNotFound {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
      })?;

    Ok(
      entries
        .iter()
        .skip(offset as usize)
        .take(max_records)
        .cloned()
        .collect(),
    )
  }

  fn last_offset(&self, topic_partition: &TopicPartition) -> Result<Option<u64>> {
    let topics = self.topics.read().expect("poisoned topics lock");
    let entries = topics
      .get(topic_partition)
      .ok_or_else(|| StoreError::PartitionNotFound {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
      })?;
    Ok(entries.last().map(|record| record.offset))
  }
}

impl OffsetStore for InMemoryStore {
  fn load_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
  ) -> Result<Option<u64>> {
    let offsets = self
      .consumer_offsets
      .read()
      .expect("poisoned consumer_offsets lock");
    Ok(
      offsets
        .get(&(consumer.to_owned(), topic_partition.clone()))
        .copied(),
    )
  }

  fn save_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> Result<()> {
    let mut offsets = self
      .consumer_offsets
      .write()
      .expect("poisoned consumer_offsets lock");
    offsets.insert((consumer.to_owned(), topic_partition.clone()), next_offset);
    Ok(())
  }
}

impl MutablePartitionLogStore for InMemoryStore {
  fn truncate_from(&self, topic_partition: &TopicPartition, offset: u64) -> Result<()> {
    let mut topics = self.topics.write().expect("poisoned topics lock");
    let records = topics
      .get_mut(topic_partition)
      .ok_or_else(|| StoreError::PartitionNotFound {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
      })?;
    records.retain(|record| record.offset < offset);
    Ok(())
  }

  fn local_partition_state(
    &self,
    topic_partition: &TopicPartition,
  ) -> Result<crate::model::LocalPartitionState> {
    let last_offset = self.last_offset(topic_partition)?;
    Ok(crate::model::LocalPartitionState::online(
      topic_partition.clone(),
      last_offset,
    ))
  }
}
