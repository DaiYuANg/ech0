use std::collections::BTreeMap;
use std::sync::{OnceLock, RwLock};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopicProducerStats {
  pub produced_records_total: u64,
  pub hottest_partition: Option<u32>,
  pub hottest_partition_records: u64,
  pub last_partitioning: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotPartitionStats {
  pub topic: String,
  pub partition: u32,
  pub produced_records_total: u64,
  pub last_partitioning: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProducerRuntimeState {
  pub produce_requests_total: u64,
  pub produced_records_total: u64,
  pub explicit_requests_total: u64,
  pub round_robin_requests_total: u64,
  pub key_hash_requests_total: u64,
  pub explicit_records_total: u64,
  pub round_robin_records_total: u64,
  pub key_hash_records_total: u64,
  pub topics: BTreeMap<String, TopicProducerStats>,
  pub hot_partitions: Vec<HotPartitionStats>,
}

#[derive(Debug, Default)]
struct ProducerRuntimeMutableState {
  produce_requests_total: u64,
  produced_records_total: u64,
  explicit_requests_total: u64,
  round_robin_requests_total: u64,
  key_hash_requests_total: u64,
  explicit_records_total: u64,
  round_robin_records_total: u64,
  key_hash_records_total: u64,
  topic_partition_counts: BTreeMap<(String, u32), u64>,
  topic_partition_last_partitioning: BTreeMap<(String, u32), String>,
}

static PRODUCER_STATE: OnceLock<RwLock<ProducerRuntimeMutableState>> = OnceLock::new();

fn state_lock() -> &'static RwLock<ProducerRuntimeMutableState> {
  PRODUCER_STATE.get_or_init(|| RwLock::new(ProducerRuntimeMutableState::default()))
}

pub fn record_publish(topic: &str, partition: u32, partitioning: &str, records: u64) {
  if let Ok(mut state) = state_lock().write() {
    state.produce_requests_total = state.produce_requests_total.saturating_add(1);
    state.produced_records_total = state.produced_records_total.saturating_add(records);
    match partitioning {
      "explicit" => {
        state.explicit_requests_total = state.explicit_requests_total.saturating_add(1);
        state.explicit_records_total = state.explicit_records_total.saturating_add(records);
      }
      "round_robin" => {
        state.round_robin_requests_total = state.round_robin_requests_total.saturating_add(1);
        state.round_robin_records_total = state.round_robin_records_total.saturating_add(records);
      }
      "key_hash" => {
        state.key_hash_requests_total = state.key_hash_requests_total.saturating_add(1);
        state.key_hash_records_total = state.key_hash_records_total.saturating_add(records);
      }
      _ => {}
    }

    let key = (topic.to_owned(), partition);
    let entry = state.topic_partition_counts.entry(key.clone()).or_insert(0);
    *entry = entry.saturating_add(records);
    state
      .topic_partition_last_partitioning
      .insert(key, partitioning.to_owned());
  }
}

pub fn snapshot() -> ProducerRuntimeState {
  let Ok(state) = state_lock().read() else {
    return ProducerRuntimeState::default();
  };

  let mut topics = BTreeMap::new();
  let mut hot_partitions = state
    .topic_partition_counts
    .iter()
    .map(|((topic, partition), produced_records_total)| HotPartitionStats {
      topic: topic.clone(),
      partition: *partition,
      produced_records_total: *produced_records_total,
      last_partitioning: state
        .topic_partition_last_partitioning
        .get(&(topic.clone(), *partition))
        .cloned(),
    })
    .collect::<Vec<_>>();
  hot_partitions.sort_by(|left, right| {
    right
      .produced_records_total
      .cmp(&left.produced_records_total)
      .then_with(|| left.topic.cmp(&right.topic))
      .then_with(|| left.partition.cmp(&right.partition))
  });

  for item in &hot_partitions {
    let topic_stats = topics.entry(item.topic.clone()).or_insert_with(TopicProducerStats::default);
    topic_stats.produced_records_total = topic_stats
      .produced_records_total
      .saturating_add(item.produced_records_total);
    if topic_stats.hottest_partition_records < item.produced_records_total {
      topic_stats.hottest_partition = Some(item.partition);
      topic_stats.hottest_partition_records = item.produced_records_total;
      topic_stats.last_partitioning = item.last_partitioning.clone();
    }
  }

  ProducerRuntimeState {
    produce_requests_total: state.produce_requests_total,
    produced_records_total: state.produced_records_total,
    explicit_requests_total: state.explicit_requests_total,
    round_robin_requests_total: state.round_robin_requests_total,
    key_hash_requests_total: state.key_hash_requests_total,
    explicit_records_total: state.explicit_records_total,
    round_robin_records_total: state.round_robin_records_total,
    key_hash_records_total: state.key_hash_records_total,
    topics,
    hot_partitions: hot_partitions.into_iter().take(5).collect(),
  }
}

