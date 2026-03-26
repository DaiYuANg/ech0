use crate::raft::OpenRaftRuntimeConfig;
use store::{
  ConsumerGroupAssignment, ConsumerGroupMember, GroupPartitionAssignment, TopicCleanupPolicy,
  TopicRetryPolicy,
};

#[derive(Debug, Clone)]
pub enum BrokerRuntimeMode {
  Standalone,
  Raft(OpenRaftRuntimeConfig),
}

impl BrokerRuntimeMode {
  pub fn from_raft_runtime(runtime: Option<OpenRaftRuntimeConfig>) -> Self {
    match runtime {
      Some(runtime) => Self::Raft(runtime),
      None => Self::Standalone,
    }
  }

  pub fn is_raft(&self) -> bool {
    matches!(self, Self::Raft(_))
  }

  pub fn label(&self) -> &'static str {
    match self {
      Self::Standalone => "standalone",
      Self::Raft(_) => "raft",
    }
  }
}

impl Default for BrokerRuntimeMode {
  fn default() -> Self {
    Self::Standalone
  }
}

#[derive(Debug, Clone)]
pub struct BrokerIdentity {
  pub node_id: u64,
  pub cluster_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct TopicPolicyOverrides {
  pub retention_max_bytes: Option<u64>,
  pub cleanup_policy: Option<TopicCleanupPolicy>,
  pub max_message_bytes: Option<u32>,
  pub max_batch_bytes: Option<u32>,
  pub retention_ms: Option<u64>,
  pub retry_policy: Option<TopicRetryPolicy>,
  pub dead_letter_topic: Option<String>,
  pub delay_enabled: Option<bool>,
  pub compaction_enabled: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRecord {
  /// Record offset (record ID, inclusive).
  pub offset: u64,
  pub timestamp_ms: u64,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResult {
  pub topic: String,
  pub partition: u32,
  pub records: Vec<FetchedRecord>,
  /// Cursor to pass in the next fetch call.
  pub next_offset: u64,
  /// Largest committed and visible offset (inclusive).
  pub high_watermark: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryResult {
  pub retry_topic: String,
  pub retry_partition: u32,
  pub retry_offset: u64,
  pub retry_next_offset: u64,
  pub retry_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRetryResult {
  pub retry_topic: String,
  pub partition: u32,
  pub moved_to_origin: usize,
  pub moved_to_dead_letter: usize,
  pub committed_next_offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelayScheduleResult {
  pub delay_topic: String,
  pub partition: u32,
  pub offset: u64,
  pub next_offset: u64,
  pub deliver_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMemberLease {
  pub group: String,
  pub member_id: String,
  pub topics: Vec<String>,
  pub session_timeout_ms: u64,
  pub joined_at_ms: u64,
  pub last_heartbeat_ms: u64,
  pub expires_at_ms: u64,
}

impl From<ConsumerGroupMember> for GroupMemberLease {
  fn from(value: ConsumerGroupMember) -> Self {
    let expires_at_ms = value.expires_at_ms();
    Self {
      group: value.group,
      member_id: value.member_id,
      topics: value.topics,
      session_timeout_ms: value.session_timeout_ms,
      joined_at_ms: value.joined_at_ms,
      last_heartbeat_ms: value.last_heartbeat_ms,
      expires_at_ms,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupPartitionOwner {
  pub member_id: String,
  pub topic: String,
  pub partition: u32,
}

impl From<GroupPartitionAssignment> for GroupPartitionOwner {
  fn from(value: GroupPartitionAssignment) -> Self {
    Self {
      member_id: value.member_id,
      topic: value.topic,
      partition: value.partition,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupAssignmentSnapshot {
  pub group: String,
  pub generation: u64,
  pub assignments: Vec<GroupPartitionOwner>,
  pub updated_at_ms: u64,
}

impl From<ConsumerGroupAssignment> for GroupAssignmentSnapshot {
  fn from(value: ConsumerGroupAssignment) -> Self {
    Self {
      group: value.group,
      generation: value.generation,
      assignments: value.assignments.into_iter().map(Into::into).collect(),
      updated_at_ms: value.updated_at_ms,
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupAssignmentStrategy {
  RoundRobin,
  Range,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCoordinatorOptions {
  pub assignment_strategy: GroupAssignmentStrategy,
  pub sticky_assignments: bool,
}

impl Default for GroupCoordinatorOptions {
  fn default() -> Self {
    Self {
      assignment_strategy: GroupAssignmentStrategy::RoundRobin,
      sticky_assignments: true,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMemberLoad {
  pub member_id: String,
  pub partitions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRebalanceExplain {
  pub group: String,
  pub next_generation: u64,
  pub strategy: &'static str,
  pub sticky_assignments: bool,
  pub active_members: usize,
  pub total_assignments: usize,
  pub moved_partitions: u64,
  pub sticky_candidates: u64,
  pub sticky_applied: u64,
  pub member_loads: Vec<GroupMemberLoad>,
}
