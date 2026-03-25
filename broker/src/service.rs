use std::sync::Arc;
use std::{collections::BTreeMap, collections::BTreeSet};
use tracing::info;

use direct::{DirectRuntime, FetchInboxResult, SendDirectResult, is_internal_topic_name};
use queue::QueueRuntime;
use store::{
  ConsensusLogStore, ConsensusMetadataStore, ConsumerGroupAssignment, ConsumerGroupMember,
  ConsumerGroupStore, GroupPartitionAssignment, LocalPartitionStateStore, MessageLogStore,
  MutablePartitionLogStore, OffsetStore, Result, StoreError, TopicCatalogStore, TopicConfig,
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
  BrokerIdentity, BrokerRuntimeMode, FetchResult, FetchedRecord, GroupAssignmentSnapshot,
  GroupAssignmentStrategy, GroupCoordinatorOptions, GroupMemberLease, GroupMemberLoad,
  GroupRebalanceExplain,
};

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
    self.create_topic_with_retention(topic, partitions, None)
  }

  pub fn create_topic_with_retention(
    &self,
    topic: impl Into<String>,
    partitions: u32,
    retention_max_bytes: Option<u64>,
  ) -> Result<TopicConfig> {
    let topic = topic.into();
    if partitions == 0 {
      return Err(StoreError::Codec(
        "partitions must be greater than zero".to_owned(),
      ));
    }
    if retention_max_bytes == Some(0) {
      return Err(StoreError::Codec(
        "retention_max_bytes must be greater than zero".to_owned(),
      ));
    }

    let mut config = TopicConfig::new(topic.clone());
    config.partitions = partitions;
    if let Some(retention_max_bytes) = retention_max_bytes {
      config.retention_max_bytes = retention_max_bytes;
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
