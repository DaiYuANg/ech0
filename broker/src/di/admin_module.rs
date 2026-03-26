use std::sync::Arc;

use serde_json::Value;

use crate::{
  admin::{
    AdminServerConfig, GroupAssignmentSummary, GroupLagSummary, GroupMemberLoadSummary,
    GroupMemberSummary, GroupPartitionLagSummary, GroupPartitionOwnerSummary,
    GroupRebalanceExplainSummary, OpsConfigSummary, TopicMessageSummary,
    TopicMessagesPageSummary, TopicSummary,
  },
  config::AppConfig,
  ops_state, producer_state,
};
use crate::admin::health::build_health_response;

use super::BrokerServiceHandle;

fn cleanup_policy_label(policy: store::TopicCleanupPolicy) -> &'static str {
  match policy {
    store::TopicCleanupPolicy::Delete => "delete",
    store::TopicCleanupPolicy::Compact => "compact",
    store::TopicCleanupPolicy::CompactAndDelete => "compact_and_delete",
  }
}

fn take_chars_with_ellipsis(input: &str, max_chars: usize) -> String {
  let mut preview = String::new();
  for (idx, ch) in input.chars().enumerate() {
    if idx >= max_chars {
      preview.push_str("...");
      return preview;
    }
    preview.push(ch);
  }
  preview
}

fn payload_utf8_preview(bytes: &[u8]) -> String {
  let text = String::from_utf8_lossy(bytes);
  take_chars_with_ellipsis(&text, 160)
}

fn payload_hex_preview(bytes: &[u8]) -> String {
  let max_bytes = 32usize;
  let mut hex = bytes
    .iter()
    .take(max_bytes)
    .map(|byte| format!("{byte:02x}"))
    .collect::<Vec<_>>()
    .join("");
  if bytes.len() > max_bytes {
    hex.push_str("...");
  }
  hex
}

fn payload_json_preview(bytes: &[u8]) -> Option<String> {
  let value = serde_json::from_slice::<Value>(bytes).ok()?;
  let pretty = serde_json::to_string_pretty(&value).ok()?;
  Some(take_chars_with_ellipsis(&pretty, 1000))
}

pub(super) struct AdminModule {
  app: AppConfig,
}

impl AdminModule {
  pub fn new(app: AppConfig) -> Self {
    Self { app }
  }

  pub fn build_admin_config(&self, service: Arc<BrokerServiceHandle>) -> Option<AdminServerConfig> {
    if !self.app.admin.enabled {
      return None;
    }

    let retention_cleanup_enabled = self.app.storage.retention_cleanup_enabled;
    let retention_cleanup_interval_secs = self.app.storage.retention_cleanup_interval_secs;
    let compaction_cleanup_enabled = self.app.storage.compaction_cleanup_enabled;
    let compaction_cleanup_interval_secs = self.app.storage.compaction_cleanup_interval_secs;
    let compaction_sealed_segment_batch = self.app.storage.compaction_sealed_segment_batch;
    let service_for_health = Arc::clone(&service);
    let service_for_metrics = Arc::clone(&service);
    let service_for_admin = Arc::clone(&service);
    let service_for_members = Arc::clone(&service);
    let service_for_assignment = Arc::clone(&service);
    let service_for_group_lag = Arc::clone(&service);
    let service_for_rebalance = Arc::clone(&service);
    let service_for_rebalance_explain = Arc::clone(&service);
    let service_for_topic_messages = Arc::clone(&service);
    Some(AdminServerConfig {
      bind_addr: self.app.admin.bind_addr.clone(),
      ops_config: OpsConfigSummary {
        retention_cleanup_enabled: self.app.storage.retention_cleanup_enabled,
        retention_cleanup_interval_secs: self.app.storage.retention_cleanup_interval_secs,
        compaction_cleanup_enabled: self.app.storage.compaction_cleanup_enabled,
        compaction_cleanup_interval_secs: self.app.storage.compaction_cleanup_interval_secs,
        compaction_sealed_segment_batch: self.app.storage.compaction_sealed_segment_batch,
      },
      health_snapshot: Arc::new(move || {
        service_for_health
          .runtime_health()
          .map(|health| {
            let ops = ops_state::snapshot();
            build_health_response(
              health,
              &OpsConfigSummary {
                retention_cleanup_enabled,
                retention_cleanup_interval_secs,
                compaction_cleanup_enabled,
                compaction_cleanup_interval_secs,
                compaction_sealed_segment_batch,
              },
              &ops,
            )
          })
          .map_err(|err| err.to_string())
      }),
      metrics_refresh: Arc::new(move || {
        service_for_metrics
          .stream_metrics_snapshot()
          .map(|snapshot| crate::metrics::update_stream_metrics(&snapshot))
          .map_err(|err| err.to_string())
      }),
      topic_snapshot: Arc::new(move || {
        let producer_snapshot = producer_state::snapshot();
        service_for_admin
          .list_topics()
          .map(|topics| {
            topics
              .into_iter()
              .map(|topic| {
                let topic_name = topic.name.clone();
                let producer_stats = producer_snapshot.topics.get(&topic_name);
                let backlog = service_for_admin.topic_backlog(&topic_name).ok();
                TopicSummary {
                  name: topic.name,
                  partitions: topic.partitions,
                  segment_max_bytes: topic.segment_max_bytes,
                  index_interval_bytes: topic.index_interval_bytes,
                  retention_max_bytes: topic.retention_max_bytes,
                  cleanup_policy: cleanup_policy_label(topic.cleanup_policy).to_owned(),
                  retention_ms: topic.retention_ms,
                  compaction_tombstone_retention_ms: topic.compaction_tombstone_retention_ms,
                  max_message_bytes: topic.max_message_bytes,
                  max_batch_bytes: topic.max_batch_bytes,
                  retry_max_attempts: topic.retry_policy.max_attempts,
                  dead_letter_topic: topic.dead_letter_topic,
                  delay_enabled: topic.delay_enabled,
                  compaction_enabled: topic.compaction_enabled,
                  produced_records_total: producer_stats
                    .map(|stats| stats.produced_records_total)
                    .unwrap_or(0),
                  hottest_partition: producer_stats.and_then(|stats| stats.hottest_partition),
                  hottest_partition_records: producer_stats
                    .map(|stats| stats.hottest_partition_records)
                    .unwrap_or(0),
                  last_partitioning: producer_stats.and_then(|stats| stats.last_partitioning.clone()),
                  total_backlog_records: backlog
                    .as_ref()
                    .map(|snapshot| snapshot.total_backlog_records)
                    .unwrap_or(0),
                  max_partition_backlog: backlog
                    .as_ref()
                    .map(|snapshot| snapshot.max_partition_backlog)
                    .unwrap_or(0),
                }
              })
              .collect()
          })
          .map_err(|err| err.to_string())
      }),
      topic_messages_snapshot: Arc::new(move |topic, partition, offset, limit| {
        service_for_topic_messages
          .fetch("admin-ui", topic.to_owned(), partition, Some(offset), limit)
          .map(|fetched| TopicMessagesPageSummary {
            topic: fetched.topic,
            partition: fetched.partition,
            next_offset: fetched.next_offset,
            high_watermark: fetched.high_watermark,
            records: fetched
              .records
              .into_iter()
              .map(|record| {
                let payload_size = record.payload.len();
                let payload_utf8_preview = payload_utf8_preview(&record.payload);
                let payload_hex_preview = payload_hex_preview(&record.payload);
                let payload_json_preview = payload_json_preview(&record.payload);
                TopicMessageSummary {
                  offset: record.offset,
                  timestamp_ms: record.timestamp_ms,
                  payload_size,
                  payload_utf8_preview,
                  payload_hex_preview,
                  payload_json_preview,
                }
              })
              .collect(),
          })
          .map_err(|err| err.to_string())
      }),
      group_members_snapshot: Arc::new(move |group| {
        service_for_members
          .list_active_group_members(group)
          .map(|members| {
            members
              .into_iter()
              .map(|member| GroupMemberSummary {
                group: member.group,
                member_id: member.member_id,
                topics: member.topics,
                session_timeout_ms: member.session_timeout_ms,
                joined_at_ms: member.joined_at_ms,
                last_heartbeat_ms: member.last_heartbeat_ms,
                expires_at_ms: member.expires_at_ms,
              })
              .collect()
          })
          .map_err(|err| err.to_string())
      }),
      group_assignment_snapshot: Arc::new(move |group| {
        service_for_assignment
          .load_consumer_group_assignment(group)
          .map(|assignment| {
            assignment.map(|snapshot| GroupAssignmentSummary {
              group: snapshot.group,
              generation: snapshot.generation,
              assignments: snapshot
                .assignments
                .into_iter()
                .map(|item| GroupPartitionOwnerSummary {
                  member_id: item.member_id,
                  topic: item.topic,
                  partition: item.partition,
                })
                .collect(),
              updated_at_ms: snapshot.updated_at_ms,
            })
          })
          .map_err(|err| err.to_string())
      }),
      group_lag_snapshot: Arc::new(move |group| {
        service_for_group_lag
          .consumer_group_lag(group)
          .map(|snapshot| {
            snapshot.map(|lag| GroupLagSummary {
              group: lag.group,
              generation: lag.generation,
              total_backlog_records: lag.total_backlog_records,
              total_lag_records: lag.total_lag_records,
              partitions: lag
                .partitions
                .into_iter()
                .map(|item| GroupPartitionLagSummary {
                  member_id: item.member_id,
                  topic: item.topic,
                  partition: item.partition,
                  committed_next_offset: item.committed_next_offset,
                  high_watermark: item.high_watermark,
                  backlog_records: item.backlog_records,
                  lag_records: item.lag_records,
                })
                .collect(),
            })
          })
          .map_err(|err| err.to_string())
      }),
      rebalance_group: Arc::new(move |group| {
        service_for_rebalance
          .rebalance_consumer_group(group)
          .map(|snapshot| GroupAssignmentSummary {
            group: snapshot.group,
            generation: snapshot.generation,
            assignments: snapshot
              .assignments
              .into_iter()
              .map(|item| GroupPartitionOwnerSummary {
                member_id: item.member_id,
                topic: item.topic,
                partition: item.partition,
              })
              .collect(),
            updated_at_ms: snapshot.updated_at_ms,
          })
          .map_err(|err| err.to_string())
      }),
      explain_rebalance: Arc::new(move |group| {
        service_for_rebalance_explain
          .explain_consumer_group_rebalance(group)
          .map(|explain| GroupRebalanceExplainSummary {
            group: explain.group,
            next_generation: explain.next_generation,
            strategy: explain.strategy.to_owned(),
            sticky_assignments: explain.sticky_assignments,
            active_members: explain.active_members,
            total_assignments: explain.total_assignments,
            moved_partitions: explain.moved_partitions,
            sticky_candidates: explain.sticky_candidates,
            sticky_applied: explain.sticky_applied,
            member_loads: explain
              .member_loads
              .into_iter()
              .map(|load| GroupMemberLoadSummary {
                member_id: load.member_id,
                partitions: load.partitions,
              })
              .collect(),
          })
          .map_err(|err| err.to_string())
      }),
    })
  }
}
