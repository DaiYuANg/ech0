use bytes::Bytes;

use super::*;
use crate::StoreError;

impl MessageLogStore for InMemoryStore {
  fn create_topic(&self, topic: TopicConfig) -> Result<()> {
    let mut topics = self.topics.write().expect("poisoned topics lock");
    if (0..topic.partitions).any(|partition| topics.contains_key(&topic.partition(partition))) {
      return Err(StoreError::TopicAlreadyExists(topic.name));
    }
    let topic_name = topic.name.clone();
    for partition in 0..topic.partitions {
      topics.insert(topic.partition(partition), Vec::new());
    }
    drop(topics);

    let mut topic_configs = self
      .topic_configs
      .write()
      .expect("poisoned topic_configs lock");
    topic_configs.insert(topic_name, topic);
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

impl LocalPartitionStateStore for InMemoryStore {
  fn save_local_partition_state(&self, state: &LocalPartitionState) -> Result<()> {
    let mut partition_states = self
      .local_partition_states
      .write()
      .expect("poisoned local_partition_states lock");
    partition_states.insert(state.topic_partition.clone(), state.clone());
    Ok(())
  }

  fn load_local_partition_state(
    &self,
    topic_partition: &TopicPartition,
  ) -> Result<Option<LocalPartitionState>> {
    let partition_states = self
      .local_partition_states
      .read()
      .expect("poisoned local_partition_states lock");
    Ok(partition_states.get(topic_partition).cloned())
  }

  fn list_local_partition_states(&self) -> Result<Vec<LocalPartitionState>> {
    let partition_states = self
      .local_partition_states
      .read()
      .expect("poisoned local_partition_states lock");
    let mut states: Vec<_> = partition_states.values().cloned().collect();
    states.sort_by(|a, b| {
      a.topic_partition.topic.cmp(&b.topic_partition.topic).then(
        a.topic_partition
          .partition
          .cmp(&b.topic_partition.partition),
      )
    });
    Ok(states)
  }
}

impl TopicCatalogStore for InMemoryStore {
  fn save_topic_config(&self, topic: &TopicConfig) -> Result<()> {
    let mut topic_configs = self
      .topic_configs
      .write()
      .expect("poisoned topic_configs lock");
    topic_configs.insert(topic.name.clone(), topic.clone());
    Ok(())
  }

  fn load_topic_config(&self, topic: &str) -> Result<Option<TopicConfig>> {
    let topic_configs = self
      .topic_configs
      .read()
      .expect("poisoned topic_configs lock");
    Ok(topic_configs.get(topic).cloned())
  }

  fn list_topics(&self) -> Result<Vec<TopicConfig>> {
    let topic_configs = self
      .topic_configs
      .read()
      .expect("poisoned topic_configs lock");
    let mut topics: Vec<_> = topic_configs.values().cloned().collect();
    topics.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(topics)
  }
}
