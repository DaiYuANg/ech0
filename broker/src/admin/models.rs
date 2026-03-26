use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone)]
pub struct TopicSummary {
  pub name: String,
  pub partitions: u32,
  pub segment_max_bytes: u64,
  pub index_interval_bytes: u64,
  pub retention_max_bytes: u64,
  pub cleanup_policy: String,
  pub retention_ms: Option<u64>,
  pub compaction_tombstone_retention_ms: Option<u64>,
  pub max_message_bytes: u32,
  pub max_batch_bytes: u32,
  pub retry_max_attempts: u32,
  pub dead_letter_topic: Option<String>,
  pub delay_enabled: bool,
  pub compaction_enabled: bool,
  pub produced_records_total: u64,
  pub hottest_partition: Option<u32>,
  pub hottest_partition_records: u64,
  pub last_partitioning: Option<String>,
  pub total_backlog_records: u64,
  pub max_partition_backlog: u64,
}

#[derive(Debug, Clone)]
pub struct TopicMessageSummary {
  pub offset: u64,
  pub timestamp_ms: u64,
  pub payload_size: usize,
  pub payload_utf8_preview: String,
  pub payload_hex_preview: String,
  pub payload_json_preview: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TopicMessagesPageSummary {
  pub topic: String,
  pub partition: u32,
  pub next_offset: u64,
  pub high_watermark: Option<u64>,
  pub records: Vec<TopicMessageSummary>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct GroupMemberSummary {
  pub group: String,
  pub member_id: String,
  pub topics: Vec<String>,
  pub session_timeout_ms: u64,
  pub joined_at_ms: u64,
  pub last_heartbeat_ms: u64,
  pub expires_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct GroupPartitionOwnerSummary {
  pub member_id: String,
  pub topic: String,
  pub partition: u32,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct GroupAssignmentSummary {
  pub group: String,
  pub generation: u64,
  pub assignments: Vec<GroupPartitionOwnerSummary>,
  pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct GroupMemberLoadSummary {
  pub member_id: String,
  pub partitions: usize,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct GroupRebalanceExplainSummary {
  pub group: String,
  pub next_generation: u64,
  pub strategy: String,
  pub sticky_assignments: bool,
  pub active_members: usize,
  pub total_assignments: usize,
  pub moved_partitions: u64,
  pub sticky_candidates: u64,
  pub sticky_applied: u64,
  pub member_loads: Vec<GroupMemberLoadSummary>,
}

#[derive(Debug, Clone)]
pub struct HotPartitionSummary {
  pub topic: String,
  pub partition: u32,
  pub produced_records_total: u64,
  pub last_partitioning: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct GroupPartitionLagSummary {
  pub member_id: String,
  pub topic: String,
  pub partition: u32,
  pub committed_next_offset: u64,
  pub high_watermark: Option<u64>,
  pub backlog_records: u64,
  pub lag_records: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct GroupLagSummary {
  pub group: String,
  pub generation: u64,
  pub total_backlog_records: u64,
  pub total_lag_records: u64,
  pub partitions: Vec<GroupPartitionLagSummary>,
}
