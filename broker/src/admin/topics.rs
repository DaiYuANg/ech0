use askama::Template;
use axum::{
  extract::{Path, Query, State},
  response::IntoResponse,
};
use serde::Deserialize;

use super::{
  AdminState, TopicMessageSummary, TopicMessagesPageSummary, TopicSummary, render_html,
};

#[derive(Debug, Template)]
#[template(path = "topics.html")]
struct TopicsTemplate {
  topics: Vec<TopicSummary>,
  error: Option<String>,
}

pub(super) async fn ui_topics(State(state): State<AdminState>) -> impl IntoResponse {
  let (topics, error) = match (state.topic_snapshot)() {
    Ok(topics) => (topics, None),
    Err(err) => (Vec::new(), Some(err)),
  };

  let template = TopicsTemplate { topics, error };
  render_html(template.render(), "topics page")
}

#[derive(Debug, Deserialize)]
pub(super) struct TopicMessagesQuery {
  partition: Option<u32>,
  offset: Option<u64>,
  limit: Option<usize>,
}

#[derive(Debug, Template)]
#[template(path = "topic_messages.html")]
struct TopicMessagesTemplate {
  topic: String,
  partition: u32,
  offset: u64,
  limit: usize,
  prev_offset: u64,
  next_offset: u64,
  high_watermark: Option<u64>,
  records: Vec<TopicMessageSummary>,
  error: Option<String>,
}

pub(super) async fn ui_topic_messages(
  State(state): State<AdminState>,
  Path(topic): Path<String>,
  Query(query): Query<TopicMessagesQuery>,
) -> impl IntoResponse {
  let partition = query.partition.unwrap_or(0);
  let offset = query.offset.unwrap_or(0);
  let limit = query.limit.unwrap_or(50).clamp(1, 200);

  let result = (state.topic_messages_snapshot)(&topic, partition, offset, limit);
  let (records, next_offset, high_watermark, error) = match result {
    Ok(TopicMessagesPageSummary {
      topic: page_topic,
      partition: page_partition,
      next_offset,
      high_watermark,
      records,
    }) => (
      records,
      next_offset,
      high_watermark,
      if page_topic == topic && page_partition == partition {
        None
      } else {
        Some("topic snapshot mismatch".to_owned())
      },
    ),
    Err(err) => (Vec::new(), offset, None, Some(err)),
  };

  let template = TopicMessagesTemplate {
    topic,
    partition,
    offset,
    limit,
    prev_offset: offset.saturating_sub(limit as u64),
    next_offset,
    high_watermark,
    records,
    error,
  };

  render_html(template.render(), "topic messages page")
}
