use std::sync::Arc;

use serde_json::Value;

use crate::{
  admin::{
    AdminServerConfig, GroupAssignmentSummary, GroupMemberLoadSummary, GroupMemberSummary,
    GroupPartitionOwnerSummary, GroupRebalanceExplainSummary, TopicMessageSummary,
    TopicMessagesPageSummary, TopicSummary,
  },
  config::AppConfig,
};

use super::BrokerServiceHandle;

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

    let service_for_admin = Arc::clone(&service);
    let service_for_members = Arc::clone(&service);
    let service_for_assignment = Arc::clone(&service);
    let service_for_rebalance = Arc::clone(&service);
    let service_for_rebalance_explain = Arc::clone(&service);
    let service_for_topic_messages = Arc::clone(&service);
    Some(AdminServerConfig {
      bind_addr: self.app.admin.bind_addr.clone(),
      topic_snapshot: Arc::new(move || {
        service_for_admin
          .list_topics()
          .map(|topics| {
            topics
              .into_iter()
              .map(|topic| TopicSummary {
                name: topic.name,
                partitions: topic.partitions,
                segment_max_bytes: topic.segment_max_bytes,
                index_interval_bytes: topic.index_interval_bytes,
                retention_max_bytes: topic.retention_max_bytes,
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
