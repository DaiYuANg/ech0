use std::sync::Arc;
use std::{collections::BTreeMap, collections::BTreeSet};
use tracing::info;

use direct::{DirectRuntime, FetchInboxResult, SendDirectResult, is_internal_topic_name as is_direct_internal_topic_name};
use queue::QueueRuntime;
use scheduler::{enqueue_delayed, is_delay_topic_name as is_scheduler_delay_topic_name};
use store::{
  ConsensusLogStore, ConsensusMetadataStore, ConsumerGroupAssignment, ConsumerGroupMember,
  ConsumerGroupStore, GroupPartitionAssignment, LocalPartitionStateStore, MessageLogStore,
  MutablePartitionLogStore, OffsetStore, Record, RecordAppend, RecordHeader, Result, StoreError,
  TopicCatalogStore, TopicConfig, TopicPartition,
};

use crate::{config::RaftReadPolicy, metrics, raft::BrokerRaftRuntime};

mod mode;
#[cfg(test)]
mod tests;
mod types;

use mode::{
  ModeAwareLogStore, ModeAwareMetadataStore, build_metadata_writer, build_partition_appender,
};
pub use types::{
  BrokerIdentity, BrokerRuntimeMode, DelayScheduleResult, FetchResult, FetchedRecord,
  GroupAssignmentSnapshot, GroupAssignmentStrategy, GroupCoordinatorOptions, GroupMemberLease,
  GroupMemberLoad, GroupRebalanceExplain, ProcessRetryResult, RetryResult, TopicPolicyOverrides,
};

const INTERNAL_RETRY_TOPIC_PREFIX: &str = "__retry";
const RETRY_HEADER_ORIGINAL_TOPIC: &str = "x-retry-original-topic";
const RETRY_HEADER_ORIGINAL_PARTITION: &str = "x-retry-original-partition";
const RETRY_HEADER_ORIGINAL_OFFSET: &str = "x-retry-original-offset";
const RETRY_HEADER_RETRY_COUNT: &str = "x-retry-count";
const RETRY_HEADER_LAST_ERROR: &str = "x-retry-last-error";
const RETRY_HEADER_FAILED_CONSUMER: &str = "x-retry-failed-consumer";

/// Dead-letter records use these headers; payload is the original application bytes (`original_payload`).
const DLQ_HEADER_ORIGINAL_TOPIC: &str = "x-dlq-original-topic";
const DLQ_HEADER_ORIGINAL_PARTITION: &str = "x-dlq-original-partition";
const DLQ_HEADER_ORIGINAL_OFFSET: &str = "x-dlq-original-offset";
const DLQ_HEADER_RETRY_COUNT: &str = "x-dlq-retry-count";
const DLQ_HEADER_ERROR_CODE: &str = "x-dlq-error-code";
const DLQ_HEADER_ERROR_MESSAGE: &str = "x-dlq-error-message";
const DLQ_ERROR_CODE_RETRY_EXHAUSTED: &str = "retry_exhausted";

#[derive(Debug)]
pub struct BrokerService<L, M> {
  identity: BrokerIdentity,
  runtime_mode: BrokerRuntimeMode,
  raft_runtime: Option<Arc<BrokerRaftRuntime<L, M>>>,
  group_options: GroupCoordinatorOptions,
  metadata: Arc<ModeAwareMetadataStore<M>>,
  queue: QueueRuntime<Arc<ModeAwareLogStore<L>>, Arc<ModeAwareMetadataStore<M>>>,
  direct: DirectRuntime<Arc<ModeAwareLogStore<L>>, Arc<ModeAwareMetadataStore<M>>>,
}

impl<L, M> BrokerService<L, M>
where
  L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
  M: OffsetStore
    + TopicCatalogStore
    + ConsumerGroupStore
    + LocalPartitionStateStore
    + ConsensusLogStore
    + ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  pub fn new(identity: BrokerIdentity, log: L, meta: M) -> Result<Self>
  where
    L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
    M: OffsetStore
      + TopicCatalogStore
      + ConsumerGroupStore
      + LocalPartitionStateStore
      + ConsensusLogStore
      + ConsensusMetadataStore
      + Send
      + Sync
      + 'static,
  {
    Self::new_with_mode_and_group_options(
      identity,
      log,
      meta,
      BrokerRuntimeMode::Standalone,
      GroupCoordinatorOptions::default(),
    )
  }

  pub fn new_with_mode(
    identity: BrokerIdentity,
    log: L,
    meta: M,
    runtime_mode: BrokerRuntimeMode,
  ) -> Result<Self>
  where
    L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
    M: OffsetStore
      + TopicCatalogStore
      + ConsumerGroupStore
      + LocalPartitionStateStore
      + ConsensusLogStore
      + ConsensusMetadataStore
      + Send
      + Sync
      + 'static,
  {
    Self::new_with_mode_and_group_options(
      identity,
      log,
      meta,
      runtime_mode,
      GroupCoordinatorOptions::default(),
    )
  }

  pub fn new_with_mode_and_group_options(
    identity: BrokerIdentity,
    log: L,
    meta: M,
    runtime_mode: BrokerRuntimeMode,
    group_options: GroupCoordinatorOptions,
  ) -> Result<Self>
  where
    L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
    M: OffsetStore
      + TopicCatalogStore
      + ConsumerGroupStore
      + LocalPartitionStateStore
      + ConsensusLogStore
      + ConsensusMetadataStore
      + Send
      + Sync
      + 'static,
  {
    let log = Arc::new(log);
    let meta = Arc::new(meta);
    let raft_runtime = match &runtime_mode {
      BrokerRuntimeMode::Standalone => None,
      BrokerRuntimeMode::Raft(config) => Some(Arc::new(BrokerRaftRuntime::new(
        config.clone(),
        Arc::clone(&log),
        Arc::clone(&meta),
      )?)),
    };
    let mode_aware_meta = Arc::new(ModeAwareMetadataStore::new(
      Arc::clone(&meta),
      build_metadata_writer(&runtime_mode, Arc::clone(&meta), raft_runtime.clone()),
      runtime_mode.clone(),
    ));
    let mode_aware_log = Arc::new(ModeAwareLogStore::new(
      Arc::clone(&log),
      build_partition_appender(
        &runtime_mode,
        Arc::clone(&log),
        Arc::clone(&meta),
        raft_runtime.clone(),
      ),
      runtime_mode.clone(),
    ));
    Ok(Self {
      identity,
      runtime_mode,
      raft_runtime: raft_runtime.clone(),
      group_options,
      metadata: Arc::clone(&mode_aware_meta),
      queue: QueueRuntime::new(Arc::clone(&mode_aware_log), Arc::clone(&mode_aware_meta)),
      direct: DirectRuntime::new(mode_aware_log, mode_aware_meta),
    })
  }

  pub fn identity(&self) -> &BrokerIdentity {
    &self.identity
  }

  pub fn runtime_mode(&self) -> &BrokerRuntimeMode {
    &self.runtime_mode
  }
}

impl<L, M> BrokerService<L, M>
where
  L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
  M: OffsetStore
    + TopicCatalogStore
    + ConsumerGroupStore
    + LocalPartitionStateStore
    + ConsensusLogStore
    + ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  fn read_policy(&self) -> RaftReadPolicy {
    match &self.runtime_mode {
      BrokerRuntimeMode::Standalone => RaftReadPolicy::Local,
      BrokerRuntimeMode::Raft(config) => config.read_policy,
    }
  }

  fn ensure_read_allowed(&self) -> Result<()> {
    match self.read_policy() {
      RaftReadPolicy::Local => Ok(()),
      RaftReadPolicy::Leader => self.ensure_local_leader(),
      RaftReadPolicy::Linearizable => {
        self.ensure_local_leader()?;
        self
          .raft_runtime
          .as_ref()
          .expect("raft runtime must be initialized for linearizable reads")
          .ensure_linearizable_read()
      }
    }
  }

  fn ensure_local_leader(&self) -> Result<()> {
    let leader_id = self
      .raft_runtime
      .as_ref()
      .expect("raft runtime must be initialized for leader reads")
      .current_leader()?;

    if leader_id == Some(self.identity.node_id) {
      Ok(())
    } else {
      Err(StoreError::NotLeader { leader_id })
    }
  }

  pub fn create_topic(&self, topic: impl Into<String>, partitions: u32) -> Result<TopicConfig> {
    self.create_topic_with_policies(topic, partitions, TopicPolicyOverrides::default())
  }

  pub fn create_topic_with_retention(
    &self,
    topic: impl Into<String>,
    partitions: u32,
    retention_max_bytes: Option<u64>,
  ) -> Result<TopicConfig> {
    self.create_topic_with_policies(
      topic,
      partitions,
      TopicPolicyOverrides {
        retention_max_bytes,
        ..TopicPolicyOverrides::default()
      },
    )
  }

  pub fn create_topic_with_policies(
    &self,
    topic: impl Into<String>,
    partitions: u32,
    policies: TopicPolicyOverrides,
  ) -> Result<TopicConfig> {
    let topic = topic.into();
    if partitions == 0 {
      return Err(StoreError::Codec(
        "partitions must be greater than zero".to_owned(),
      ));
    }
    if policies.retention_max_bytes == Some(0) {
      return Err(StoreError::Codec(
        "retention_max_bytes must be greater than zero".to_owned(),
      ));
    }
    if policies.max_message_bytes == Some(0) {
      return Err(StoreError::Codec(
        "max_message_bytes must be greater than zero".to_owned(),
      ));
    }
    if policies.max_batch_bytes == Some(0) {
      return Err(StoreError::Codec(
        "max_batch_bytes must be greater than zero".to_owned(),
      ));
    }
    if policies.retention_ms == Some(0) {
      return Err(StoreError::Codec(
        "retention_ms must be greater than zero".to_owned(),
      ));
    }

    let mut config = TopicConfig::new(topic.clone());
    config.partitions = partitions;
    if let Some(retention_max_bytes) = policies.retention_max_bytes {
      config.retention_max_bytes = retention_max_bytes;
    }
    if let Some(cleanup_policy) = policies.cleanup_policy {
      config.cleanup_policy = cleanup_policy;
    }
    if let Some(max_message_bytes) = policies.max_message_bytes {
      config.max_message_bytes = max_message_bytes;
    }
    if let Some(max_batch_bytes) = policies.max_batch_bytes {
      config.max_batch_bytes = max_batch_bytes;
    }
    if let Some(retention_ms) = policies.retention_ms {
      config.retention_ms = Some(retention_ms);
    }
    if let Some(retry_policy) = policies.retry_policy {
      config.retry_policy = retry_policy;
    }
    if let Some(dead_letter_topic) = policies.dead_letter_topic {
      config.dead_letter_topic = Some(dead_letter_topic);
    }
    if let Some(delay_enabled) = policies.delay_enabled {
      config.delay_enabled = delay_enabled;
    }
    if let Some(compaction_enabled) = policies.compaction_enabled {
      config.compaction_enabled = compaction_enabled;
    }
    self.queue.create_topic(config.clone())?;
    Ok(config)
  }

  pub fn publish(
    &self,
    topic: impl Into<String>,
    partition: u32,
    payload: Vec<u8>,
  ) -> Result<(u64, u64)> {
    let topic = topic.into();
    let record = self.queue.publish(&topic, partition, &payload)?;
    Ok((record.offset, record.offset + 1))
  }

  pub fn publish_batch(
    &self,
    topic: impl Into<String>,
    partition: u32,
    payloads: Vec<Vec<u8>>,
  ) -> Result<(u64, u64, u64)> {
    if payloads.is_empty() {
      return Err(StoreError::Codec(
        "payloads must not be empty for batch produce".to_owned(),
      ));
    }
    let topic = topic.into();
    let records = self.queue.publish_batch(&topic, partition, &payloads)?;
    let base_offset = records
      .first()
      .map(|record| record.offset)
      .ok_or_else(|| StoreError::Corruption("batch append returned no records".to_owned()))?;
    let last_offset = records
      .last()
      .map(|record| record.offset)
      .ok_or_else(|| StoreError::Corruption("batch append returned no records".to_owned()))?;
    Ok((base_offset, last_offset, last_offset + 1))
  }

  pub fn schedule_delayed(
    &self,
    topic: impl Into<String>,
    partition: u32,
    payload: Vec<u8>,
    deliver_at_ms: u64,
  ) -> Result<DelayScheduleResult> {
    let topic = topic.into();
    let record = enqueue_delayed(
      self.queue.stores().0,
      self.queue.stores().1,
      &topic,
      partition,
      payload,
      deliver_at_ms,
    )?;
    Ok(DelayScheduleResult {
      delay_topic: format!("__delay.{topic}"),
      partition,
      offset: record.offset,
      next_offset: record.offset + 1,
      deliver_at_ms,
    })
  }

  /// Fetch records from an inclusive start offset and return the next cursor.
  ///
  /// `offset` is interpreted as "next offset to read". If omitted, the committed
  /// consumer cursor is used.
  pub fn fetch(
    &self,
    consumer: &str,
    topic: impl Into<String>,
    partition: u32,
    offset: Option<u64>,
    max_records: usize,
  ) -> Result<FetchResult> {
    self.ensure_read_allowed()?;
    let topic = topic.into();
    let fetched = self
      .queue
      .fetch(consumer, &topic, partition, offset, max_records)?;

    Ok(FetchResult {
      topic,
      partition,
      records: fetched
        .records
        .into_iter()
        .map(|record| FetchedRecord {
          offset: record.offset,
          timestamp_ms: record.timestamp_ms,
          payload: record.payload.to_vec(),
        })
        .collect(),
      next_offset: fetched.next_offset,
      high_watermark: fetched.high_watermark,
    })
  }

  /// Commit consumer cursor as "next offset to consume".
  pub fn commit_offset(
    &self,
    consumer: &str,
    topic: impl Into<String>,
    partition: u32,
    next_offset: u64,
  ) -> Result<()> {
    self
      .queue
      .ack(consumer, &topic.into(), partition, next_offset)
  }

  pub fn nack_and_retry(
    &self,
    consumer: &str,
    topic: impl Into<String>,
    partition: u32,
    offset: u64,
    last_error: Option<String>,
  ) -> Result<RetryResult> {
    let topic = topic.into();
    let topic_partition = TopicPartition::new(topic.clone(), partition);
    let source_record = self.read_record_at_offset(&topic_partition, offset)?;
    let origin = retry_origin_from_record(&source_record, &topic, partition)?;
    let retry_topic = retry_topic_name(&origin.topic);
    self.ensure_retry_topic(&origin.topic, &retry_topic)?;

    let mut retry_record = RecordAppend::new(source_record.payload.clone());
    retry_record.timestamp_ms = Some(source_record.timestamp_ms);
    retry_record.key = source_record.key.clone();
    retry_record.attributes = source_record.attributes;
    retry_record.headers = source_record.headers.clone();
    upsert_header(
      &mut retry_record.headers,
      RETRY_HEADER_ORIGINAL_TOPIC,
      origin.topic.clone(),
    );
    upsert_header(
      &mut retry_record.headers,
      RETRY_HEADER_ORIGINAL_PARTITION,
      origin.partition.to_string(),
    );
    upsert_header(
      &mut retry_record.headers,
      RETRY_HEADER_ORIGINAL_OFFSET,
      origin.offset.to_string(),
    );
    upsert_header(
      &mut retry_record.headers,
      RETRY_HEADER_RETRY_COUNT,
      (origin.retry_count + 1).to_string(),
    );
    upsert_header(
      &mut retry_record.headers,
      RETRY_HEADER_LAST_ERROR,
      last_error.unwrap_or_else(|| "unknown".to_owned()),
    );
    upsert_header(
      &mut retry_record.headers,
      RETRY_HEADER_FAILED_CONSUMER,
      consumer.to_owned(),
    );

    let appended = self
      .queue
      .publish_record(&retry_topic, origin.partition, retry_record)?;
    Ok(RetryResult {
      retry_topic,
      retry_partition: origin.partition,
      retry_offset: appended.offset,
      retry_next_offset: appended.offset + 1,
      retry_count: origin.retry_count + 1,
    })
  }

  pub fn process_retry_batch(
    &self,
    consumer: &str,
    source_topic: impl Into<String>,
    partition: u32,
    max_records: usize,
  ) -> Result<ProcessRetryResult> {
    let source_topic = source_topic.into();
    let retry_topic = retry_topic_name(&source_topic);
    let fetched = self
      .queue
      .fetch(consumer, &retry_topic, partition, None, max_records)?;
    if fetched.records.is_empty() {
      return Ok(ProcessRetryResult {
        retry_topic,
        partition,
        moved_to_origin: 0,
        moved_to_dead_letter: 0,
        committed_next_offset: None,
      });
    }

    let mut moved_to_origin = 0usize;
    let mut moved_to_dead_letter = 0usize;
    let mut last_processed_offset: Option<u64> = None;
    for retry_record in fetched.records {
      let origin = retry_origin_from_record(&retry_record, &source_topic, partition)?;
      let Some(origin_topic_config) = self.metadata.load_topic_config(&origin.topic)? else {
        return Err(StoreError::TopicNotFound(origin.topic));
      };

      if origin.retry_count >= origin_topic_config.retry_policy.max_attempts {
        let Some(dead_letter_topic) = origin_topic_config.dead_letter_topic.clone() else {
          return Err(StoreError::Unsupported(
            "dead_letter_topic must be configured when retry exceeds max_attempts",
          ));
        };
        self.ensure_aux_topic(&origin_topic_config, &dead_letter_topic)?;
        let record = build_dlq_append(&retry_record, &origin);
        self
          .queue
          .publish_record(&dead_letter_topic, origin.partition, record)?;
        moved_to_dead_letter += 1;
      } else {
        let record = clone_as_append(&retry_record);
        self
          .queue
          .publish_record(&origin.topic, origin.partition, record)?;
        moved_to_origin += 1;
      }
      last_processed_offset = Some(retry_record.offset);
    }

    let committed_next_offset = last_processed_offset.map(|offset| offset + 1);
    if let Some(next_offset) = committed_next_offset {
      self.queue.ack(consumer, &retry_topic, partition, next_offset)?;
    }
    Ok(ProcessRetryResult {
      retry_topic,
      partition,
      moved_to_origin,
      moved_to_dead_letter,
      committed_next_offset,
    })
  }

  pub fn process_retry_topics_once(
    &self,
    consumer_prefix: &str,
    max_records_per_partition: usize,
  ) -> Result<usize> {
    let topics = self.metadata.list_topics()?;
    let mut moved_total = 0usize;
    for topic in topics {
      if is_internal_topic_name(&topic.name) {
        continue;
      }
      let retry_topic = retry_topic_name(&topic.name);
      if !self.queue.stores().0.topic_exists(&retry_topic)? {
        continue;
      }
      for partition in 0..topic.partitions {
        let consumer = format!("{consumer_prefix}:{}:{partition}", topic.name);
        let result =
          self.process_retry_batch(&consumer, topic.name.clone(), partition, max_records_per_partition)?;
        moved_total += result.moved_to_origin + result.moved_to_dead_letter;
      }
    }
    Ok(moved_total)
  }

  pub fn list_topics(&self) -> Result<Vec<TopicConfig>> {
    self.ensure_read_allowed()?;
    let mut topics = self.queue.list_topics()?;
    topics.retain(|topic| !is_internal_topic_name(&topic.name));
    Ok(topics)
  }

  pub fn send_direct(
    &self,
    sender: &str,
    recipient: &str,
    conversation_id: Option<String>,
    payload: Vec<u8>,
  ) -> Result<SendDirectResult> {
    self
      .direct
      .send(sender, recipient, conversation_id, payload)
  }

  pub fn fetch_inbox(&self, recipient: &str, max_records: usize) -> Result<FetchInboxResult> {
    self.ensure_read_allowed()?;
    self.direct.fetch_inbox(recipient, max_records)
  }

  pub fn ack_direct(&self, recipient: &str, next_offset: u64) -> Result<()> {
    self.direct.ack_inbox(recipient, next_offset)
  }

  pub fn join_consumer_group(
    &self,
    group: &str,
    member_id: &str,
    topics: Vec<String>,
    session_timeout_ms: u64,
  ) -> Result<GroupMemberLease> {
    validate_group_member_input(group, member_id, &topics, session_timeout_ms)?;
    let now = current_time_ms();
    let removed_expired = self.metadata.delete_expired_group_members(now)?;
    let previous = self.metadata.load_group_member(group, member_id)?;
    let joined_at_ms = previous
      .as_ref()
      .map(|existing| existing.joined_at_ms)
      .unwrap_or(now);
    let membership_changed = previous
      .as_ref()
      .map(|existing| {
        existing.topics != topics || existing.session_timeout_ms != session_timeout_ms
      })
      .unwrap_or(true)
      || removed_expired > 0;
    let member = ConsumerGroupMember {
      group: group.to_owned(),
      member_id: member_id.to_owned(),
      topics,
      session_timeout_ms,
      joined_at_ms,
      last_heartbeat_ms: now,
    };
    self.metadata.save_group_member(&member)?;
    if membership_changed {
      self.rebalance_consumer_group(group)?;
    }
    Ok(member.into())
  }

  pub fn heartbeat_consumer_group(
    &self,
    group: &str,
    member_id: &str,
    session_timeout_ms: Option<u64>,
  ) -> Result<GroupMemberLease> {
    let now = current_time_ms();
    let removed_expired = self.metadata.delete_expired_group_members(now)?;
    let Some(mut member) = self.metadata.load_group_member(group, member_id)? else {
      return Err(StoreError::Codec(format!(
        "consumer group member not found: group={group}, member_id={member_id}"
      )));
    };
    if let Some(timeout_ms) = session_timeout_ms {
      if timeout_ms == 0 {
        return Err(StoreError::Codec(
          "session_timeout_ms must be greater than zero".to_owned(),
        ));
      }
      member.session_timeout_ms = timeout_ms;
    }
    member.last_heartbeat_ms = now;
    self.metadata.save_group_member(&member)?;
    if removed_expired > 0 {
      self.rebalance_consumer_group(group)?;
    }
    Ok(member.into())
  }

  pub fn leave_consumer_group(&self, group: &str, member_id: &str) -> Result<()> {
    let existed = self.metadata.load_group_member(group, member_id)?.is_some();
    self.metadata.delete_group_member(group, member_id)?;
    if existed {
      self.rebalance_consumer_group(group)?;
    }
    Ok(())
  }

  pub fn list_active_group_members(&self, group: &str) -> Result<Vec<GroupMemberLease>> {
    let now = current_time_ms();
    let removed_expired = self.metadata.delete_expired_group_members(now)?;
    if removed_expired > 0 {
      self.rebalance_consumer_group(group)?;
    }
    self
      .metadata
      .list_group_members(group)
      .map(|members| members.into_iter().map(Into::into).collect())
  }

  pub fn rebalance_consumer_group(&self, group: &str) -> Result<GroupAssignmentSnapshot> {
    let now = current_time_ms();
    let plan = self.build_rebalance_plan(group, now)?;
    let assignment = plan.assignment;
    let strategy_label = match self.group_options.assignment_strategy {
      GroupAssignmentStrategy::RoundRobin => "round_robin",
      GroupAssignmentStrategy::Range => "range",
    };
    metrics::record_rebalance(
      strategy_label,
      assignment.assignments.len() as u64,
      plan.moved_partitions,
    );
    info!(
      group = %group,
      generation = assignment.generation,
      strategy = strategy_label,
      sticky = self.group_options.sticky_assignments,
      members = plan.member_loads.len(),
      assignments = assignment.assignments.len(),
      moved_partitions = plan.moved_partitions,
      sticky_candidates = plan.sticky_candidates,
      sticky_applied = plan.sticky_applied,
      member_loads = ?plan.member_loads,
      "consumer group rebalanced"
    );
    self.metadata.save_group_assignment(&assignment)?;
    Ok(assignment.into())
  }

  pub fn explain_consumer_group_rebalance(&self, group: &str) -> Result<GroupRebalanceExplain> {
    let now = current_time_ms();
    let plan = self.build_rebalance_plan(group, now)?;
    let strategy = match self.group_options.assignment_strategy {
      GroupAssignmentStrategy::RoundRobin => "round_robin",
      GroupAssignmentStrategy::Range => "range",
    };
    let mut member_loads: Vec<GroupMemberLoad> = plan
      .member_loads
      .into_iter()
      .map(|(member_id, partitions)| GroupMemberLoad {
        member_id,
        partitions,
      })
      .collect();
    member_loads.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    Ok(GroupRebalanceExplain {
      group: group.to_owned(),
      next_generation: plan.assignment.generation,
      strategy,
      sticky_assignments: self.group_options.sticky_assignments,
      active_members: member_loads.len(),
      total_assignments: plan.assignment.assignments.len(),
      moved_partitions: plan.moved_partitions,
      sticky_candidates: plan.sticky_candidates,
      sticky_applied: plan.sticky_applied,
      member_loads,
    })
  }

  pub fn load_consumer_group_assignment(
    &self,
    group: &str,
  ) -> Result<Option<GroupAssignmentSnapshot>> {
    self
      .metadata
      .load_group_assignment(group)
      .map(|assignment| assignment.map(Into::into))
  }
}

impl<L, M> BrokerService<L, M>
where
  L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
  M: OffsetStore
    + TopicCatalogStore
    + ConsumerGroupStore
    + LocalPartitionStateStore
    + ConsensusLogStore
    + ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  fn build_rebalance_plan(&self, group: &str, now: u64) -> Result<RebalancePlan> {
    if group.trim().is_empty() {
      return Err(StoreError::Codec("group must not be empty".to_owned()));
    }

    self.metadata.delete_expired_group_members(now)?;
    let members = self.metadata.list_group_members(group)?;
    let previous_assignment = self.metadata.load_group_assignment(group)?;
    let previous_generation = previous_assignment
      .as_ref()
      .map(|assignment| assignment.generation)
      .unwrap_or(0);

    let mut subscribers_by_topic: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for member in &members {
      let mut unique_topics: BTreeSet<String> = BTreeSet::new();
      for topic in &member.topics {
        unique_topics.insert(topic.clone());
      }
      for topic in unique_topics {
        if self.metadata.load_topic_config(&topic)?.is_none() {
          continue;
        }
        subscribers_by_topic
          .entry(topic)
          .or_default()
          .push(member.member_id.clone());
      }
    }

    let mut previous_owner_by_topic_partition: BTreeMap<(String, u32), String> = BTreeMap::new();
    if let Some(previous_assignment) = &previous_assignment {
      for assignment in &previous_assignment.assignments {
        previous_owner_by_topic_partition.insert(
          (assignment.topic.clone(), assignment.partition),
          assignment.member_id.clone(),
        );
      }
    }

    let mut assignments: Vec<GroupPartitionAssignment> = Vec::new();
    let mut sticky_candidates = 0u64;
    let mut sticky_applied = 0u64;
    for (topic, subscribers) in subscribers_by_topic {
      if subscribers.is_empty() {
        continue;
      }
      let Some(topic_config) = self.metadata.load_topic_config(&topic)? else {
        continue;
      };
      let topic_result = self.compute_topic_assignments(
        &topic,
        topic_config.partitions,
        &subscribers,
        &previous_owner_by_topic_partition,
      );
      sticky_candidates += topic_result.stats.candidates;
      sticky_applied += topic_result.stats.applied;
      assignments.extend(topic_result.assignments);
    }
    assignments.sort_by(|a, b| {
      a.topic
        .cmp(&b.topic)
        .then(a.partition.cmp(&b.partition))
        .then(a.member_id.cmp(&b.member_id))
    });

    let mut moved_partitions = 0u64;
    for assignment in &assignments {
      if let Some(previous_owner) =
        previous_owner_by_topic_partition.get(&(assignment.topic.clone(), assignment.partition))
      {
        if previous_owner != &assignment.member_id {
          moved_partitions += 1;
        }
      }
    }

    let mut member_loads: BTreeMap<String, usize> = BTreeMap::new();
    for assignment in &assignments {
      *member_loads
        .entry(assignment.member_id.clone())
        .or_insert(0) += 1;
    }

    Ok(RebalancePlan {
      assignment: ConsumerGroupAssignment {
        group: group.to_owned(),
        generation: previous_generation.saturating_add(1),
        assignments,
        updated_at_ms: now,
      },
      moved_partitions,
      sticky_candidates,
      sticky_applied,
      member_loads,
    })
  }

  fn compute_topic_assignments(
    &self,
    topic: &str,
    partitions: u32,
    subscribers: &[String],
    previous_owner_by_topic_partition: &BTreeMap<(String, u32), String>,
  ) -> TopicAssignmentResult {
    let mut assignments = match self.group_options.assignment_strategy {
      GroupAssignmentStrategy::RoundRobin => {
        self.round_robin_assignments(topic, partitions, subscribers)
      }
      GroupAssignmentStrategy::Range => self.range_assignments(topic, partitions, subscribers),
    };

    if !self.group_options.sticky_assignments {
      return TopicAssignmentResult {
        assignments,
        stats: StickyStats::default(),
      };
    }

    let subscriber_set: BTreeSet<String> = subscribers.iter().cloned().collect();
    let mut load_by_member: BTreeMap<String, usize> = BTreeMap::new();
    for member_id in subscribers {
      load_by_member.insert(member_id.clone(), 0);
    }
    for assignment in &assignments {
      if let Some(load) = load_by_member.get_mut(&assignment.member_id) {
        *load += 1;
      }
    }
    let partitions_count = partitions as usize;
    let members_count = subscribers.len().max(1);
    let min_load = partitions_count / members_count;
    let max_load = partitions_count.div_ceil(members_count);
    let mut stats = StickyStats::default();

    for assignment in &mut assignments {
      let Some(previous_owner) =
        previous_owner_by_topic_partition.get(&(topic.to_owned(), assignment.partition))
      else {
        continue;
      };
      if !subscriber_set.contains(previous_owner) || assignment.member_id == *previous_owner {
        continue;
      }
      stats.candidates += 1;
      let Some(previous_owner_load) = load_by_member.get(previous_owner).copied() else {
        continue;
      };
      let Some(current_owner_load) = load_by_member.get(&assignment.member_id).copied() else {
        continue;
      };
      if previous_owner_load < max_load && current_owner_load > min_load {
        if let Some(load) = load_by_member.get_mut(previous_owner) {
          *load += 1;
        }
        if let Some(load) = load_by_member.get_mut(&assignment.member_id) {
          *load = load.saturating_sub(1);
        }
        assignment.member_id = previous_owner.clone();
        stats.applied += 1;
      }
    }
    TopicAssignmentResult { assignments, stats }
  }

  fn round_robin_assignments(
    &self,
    topic: &str,
    partitions: u32,
    subscribers: &[String],
  ) -> Vec<GroupPartitionAssignment> {
    let mut assignments = Vec::new();
    for partition in 0..partitions {
      let member_idx = (partition as usize) % subscribers.len();
      assignments.push(GroupPartitionAssignment {
        member_id: subscribers[member_idx].clone(),
        topic: topic.to_owned(),
        partition,
      });
    }
    assignments
  }

  fn range_assignments(
    &self,
    topic: &str,
    partitions: u32,
    subscribers: &[String],
  ) -> Vec<GroupPartitionAssignment> {
    let mut assignments = Vec::new();
    let partitions = partitions as usize;
    let members = subscribers.len();
    let partitions_per_member = partitions / members;
    let extra = partitions % members;
    let mut partition_cursor = 0usize;

    for (member_idx, member_id) in subscribers.iter().enumerate() {
      let owned = partitions_per_member + usize::from(member_idx < extra);
      for _ in 0..owned {
        assignments.push(GroupPartitionAssignment {
          member_id: member_id.clone(),
          topic: topic.to_owned(),
          partition: partition_cursor as u32,
        });
        partition_cursor += 1;
      }
    }
    assignments
  }
}

#[derive(Debug, Default)]
struct StickyStats {
  candidates: u64,
  applied: u64,
}

#[derive(Debug)]
struct TopicAssignmentResult {
  assignments: Vec<GroupPartitionAssignment>,
  stats: StickyStats,
}

#[derive(Debug)]
struct RebalancePlan {
  assignment: ConsumerGroupAssignment,
  moved_partitions: u64,
  sticky_candidates: u64,
  sticky_applied: u64,
  member_loads: BTreeMap<String, usize>,
}

fn validate_group_member_input(
  group: &str,
  member_id: &str,
  topics: &[String],
  session_timeout_ms: u64,
) -> Result<()> {
  if group.trim().is_empty() {
    return Err(StoreError::Codec("group must not be empty".to_owned()));
  }
  if member_id.trim().is_empty() {
    return Err(StoreError::Codec("member_id must not be empty".to_owned()));
  }
  if topics.is_empty() {
    return Err(StoreError::Codec(
      "topics must not be empty for group membership".to_owned(),
    ));
  }
  if topics.iter().any(|topic| topic.trim().is_empty()) {
    return Err(StoreError::Codec(
      "topic names must not be empty for group membership".to_owned(),
    ));
  }
  if session_timeout_ms == 0 {
    return Err(StoreError::Codec(
      "session_timeout_ms must be greater than zero".to_owned(),
    ));
  }
  Ok(())
}

fn current_time_ms() -> u64 {
  std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64
}

#[derive(Debug)]
struct RetryOrigin {
  topic: String,
  partition: u32,
  offset: u64,
  retry_count: u32,
}

impl<L, M> BrokerService<L, M>
where
  L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
  M: OffsetStore
    + TopicCatalogStore
    + ConsumerGroupStore
    + LocalPartitionStateStore
    + ConsensusLogStore
    + ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  fn read_record_at_offset(&self, topic_partition: &TopicPartition, offset: u64) -> Result<Record> {
    let records = self.queue.stores().0.read_from(topic_partition, offset, 1)?;
    let Some(record) = records.into_iter().next() else {
      return Err(StoreError::InvalidOffset {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
        offset,
      });
    };
    if record.offset != offset {
      return Err(StoreError::InvalidOffset {
        topic: topic_partition.topic.clone(),
        partition: topic_partition.partition,
        offset,
      });
    }
    Ok(record)
  }

  fn ensure_retry_topic(&self, source_topic: &str, retry_topic: &str) -> Result<()> {
    let Some(source) = self.metadata.load_topic_config(source_topic)? else {
      return Err(StoreError::TopicNotFound(source_topic.to_owned()));
    };
    self.ensure_aux_topic(&source, retry_topic)
  }

  fn ensure_aux_topic(&self, source: &TopicConfig, aux_topic: &str) -> Result<()> {
    if self.queue.stores().0.topic_exists(aux_topic)? {
      return Ok(());
    }
    let mut config = TopicConfig::new(aux_topic.to_owned());
    config.partitions = source.partitions;
    config.retention_max_bytes = source.retention_max_bytes;
    config.retention_ms = source.retention_ms;
    config.max_message_bytes = source.max_message_bytes;
    config.max_batch_bytes = source.max_batch_bytes;
    config.retry_policy = source.retry_policy.clone();
    config.dead_letter_topic = source.dead_letter_topic.clone();
    config.delay_enabled = source.delay_enabled;
    config.compaction_enabled = false;
    self.queue.create_topic(config)
  }
}

fn retry_topic_name(source_topic: &str) -> String {
  format!("{INTERNAL_RETRY_TOPIC_PREFIX}.{source_topic}")
}

fn is_internal_topic_name(topic: &str) -> bool {
  is_direct_internal_topic_name(topic)
    || is_scheduler_delay_topic_name(topic)
    || topic.starts_with(INTERNAL_RETRY_TOPIC_PREFIX)
      && (topic == INTERNAL_RETRY_TOPIC_PREFIX
        || topic.as_bytes().get(INTERNAL_RETRY_TOPIC_PREFIX.len()) == Some(&b'.')
        || topic.as_bytes().get(INTERNAL_RETRY_TOPIC_PREFIX.len()) == Some(&b'/'))
}

fn retry_origin_from_record(record: &Record, topic: &str, partition: u32) -> Result<RetryOrigin> {
  let origin_topic = header_string(record, RETRY_HEADER_ORIGINAL_TOPIC)
    .unwrap_or_else(|| topic.to_owned());
  let origin_partition = header_u32(record, RETRY_HEADER_ORIGINAL_PARTITION)?.unwrap_or(partition);
  let origin_offset = header_u64(record, RETRY_HEADER_ORIGINAL_OFFSET)?.unwrap_or(record.offset);
  let retry_count = header_u32(record, RETRY_HEADER_RETRY_COUNT)?.unwrap_or(0);
  Ok(RetryOrigin {
    topic: origin_topic,
    partition: origin_partition,
    offset: origin_offset,
    retry_count,
  })
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

fn upsert_header(headers: &mut Vec<RecordHeader>, key: &str, value: String) {
  let encoded = value.into_bytes().into();
  if let Some(existing) = headers.iter_mut().find(|h| h.key == key) {
    existing.value = encoded;
  } else {
    headers.push(RecordHeader {
      key: key.to_owned(),
      value: encoded,
    });
  }
}

fn clone_as_append(record: &Record) -> RecordAppend {
  let mut append = RecordAppend::new(record.payload.clone());
  append.timestamp_ms = Some(record.timestamp_ms);
  append.key = record.key.clone();
  append.headers = record.headers.clone();
  append.attributes = record.attributes;
  append
}

fn build_dlq_append(retry_record: &Record, origin: &RetryOrigin) -> RecordAppend {
  let error_message = header_string(retry_record, RETRY_HEADER_LAST_ERROR)
    .unwrap_or_else(|| "unknown".to_owned());
  let mut append = RecordAppend::new(retry_record.payload.clone());
  append.timestamp_ms = Some(retry_record.timestamp_ms);
  append.key = retry_record.key.clone();
  append.attributes = retry_record.attributes;
  let mut headers = Vec::new();
  upsert_header(&mut headers, DLQ_HEADER_ORIGINAL_TOPIC, origin.topic.clone());
  upsert_header(
    &mut headers,
    DLQ_HEADER_ORIGINAL_PARTITION,
    origin.partition.to_string(),
  );
  upsert_header(
    &mut headers,
    DLQ_HEADER_ORIGINAL_OFFSET,
    origin.offset.to_string(),
  );
  upsert_header(
    &mut headers,
    DLQ_HEADER_RETRY_COUNT,
    origin.retry_count.to_string(),
  );
  upsert_header(
    &mut headers,
    DLQ_HEADER_ERROR_CODE,
    DLQ_ERROR_CODE_RETRY_EXHAUSTED.to_owned(),
  );
  upsert_header(&mut headers, DLQ_HEADER_ERROR_MESSAGE, error_message);
  append.headers = headers;
  append
}
