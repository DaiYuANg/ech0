use std::sync::OnceLock;
use std::time::Instant;
use std::{io, sync::Arc};

use askama::Template;
use axum::{
  Router,
  extract::{Path, Query, State},
  http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE},
  response::{IntoResponse, Redirect},
  routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::info;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::metrics;

#[derive(Clone)]
pub struct AdminServerConfig {
  pub bind_addr: String,
  pub topic_snapshot: TopicSnapshotFn,
  pub topic_messages_snapshot: TopicMessagesSnapshotFn,
  pub group_members_snapshot: GroupMembersSnapshotFn,
  pub group_assignment_snapshot: GroupAssignmentSnapshotFn,
  pub rebalance_group: GroupRebalanceFn,
  pub explain_rebalance: GroupRebalanceExplainFn,
}

static STARTED_AT: OnceLock<Instant> = OnceLock::new();

pub type TopicSnapshotFn = Arc<dyn Fn() -> Result<Vec<TopicSummary>, String> + Send + Sync>;
pub type TopicMessagesSnapshotFn =
  Arc<dyn Fn(&str, u32, u64, usize) -> Result<TopicMessagesPageSummary, String> + Send + Sync>;
pub type GroupMembersSnapshotFn =
  Arc<dyn Fn(&str) -> Result<Vec<GroupMemberSummary>, String> + Send + Sync>;
pub type GroupAssignmentSnapshotFn =
  Arc<dyn Fn(&str) -> Result<Option<GroupAssignmentSummary>, String> + Send + Sync>;
pub type GroupRebalanceFn =
  Arc<dyn Fn(&str) -> Result<GroupAssignmentSummary, String> + Send + Sync>;
pub type GroupRebalanceExplainFn =
  Arc<dyn Fn(&str) -> Result<GroupRebalanceExplainSummary, String> + Send + Sync>;

#[derive(Debug, Clone)]
pub struct TopicSummary {
  pub name: String,
  pub partitions: u32,
  pub segment_max_bytes: u64,
  pub index_interval_bytes: u64,
  pub retention_max_bytes: u64,
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

#[derive(Clone)]
struct AdminState {
  topic_snapshot: TopicSnapshotFn,
  topic_messages_snapshot: TopicMessagesSnapshotFn,
  group_members_snapshot: GroupMembersSnapshotFn,
  group_assignment_snapshot: GroupAssignmentSnapshotFn,
  rebalance_group: GroupRebalanceFn,
  explain_rebalance: GroupRebalanceExplainFn,
}

#[derive(OpenApi)]
#[openapi(
  paths(
    healthz,
    api_group_members,
    api_group_assignment,
    api_group_rebalance,
    api_group_rebalance_explain
  ),
  components(
    schemas(
      HealthResponse,
      GroupMemberSummary,
      GroupAssignmentSummary,
      GroupPartitionOwnerSummary,
      GroupRebalanceExplainSummary,
      GroupMemberLoadSummary
    )
  ),
  tags(
    (name = "admin", description = "ech0 admin and group operations")
  )
)]
struct AdminApiDoc;

pub async fn run(config: AdminServerConfig) -> io::Result<()> {
  STARTED_AT.get_or_init(Instant::now);
  let state = AdminState {
    topic_snapshot: Arc::clone(&config.topic_snapshot),
    topic_messages_snapshot: Arc::clone(&config.topic_messages_snapshot),
    group_members_snapshot: Arc::clone(&config.group_members_snapshot),
    group_assignment_snapshot: Arc::clone(&config.group_assignment_snapshot),
    rebalance_group: Arc::clone(&config.rebalance_group),
    explain_rebalance: Arc::clone(&config.explain_rebalance),
  };
  let app = Router::new()
    .route("/", get(root))
    .route("/ui", get(ui_dashboard))
    .route("/ui/topics", get(ui_topics))
    .route("/ui/topics/{topic}/messages", get(ui_topic_messages))
    .route("/ui/groups/{group}", get(ui_group))
    .route("/healthz", get(healthz))
    .route("/metrics", get(metrics_handler))
    .route("/api/groups/{group}/members", get(api_group_members))
    .route("/api/groups/{group}/assignment", get(api_group_assignment))
    .route("/api/groups/{group}/rebalance", post(api_group_rebalance))
    .route(
      "/api/groups/{group}/rebalance-explain",
      get(api_group_rebalance_explain),
    )
    .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", AdminApiDoc::openapi()))
    .with_state(state);
  let listener = TcpListener::bind(&config.bind_addr).await?;
  info!(bind_addr = %config.bind_addr, "admin http server listening");
  axum::serve(listener, app).await
}

#[derive(Debug, Serialize, ToSchema)]
struct HealthResponse {
  status: &'static str,
}

#[utoipa::path(
  get,
  path = "/healthz",
  tag = "admin",
  responses(
    (status = 200, description = "Health status", body = HealthResponse)
  )
)]
async fn healthz() -> impl IntoResponse {
  axum::Json(HealthResponse { status: "ok" })
}

async fn root() -> impl IntoResponse {
  Redirect::to("/ui")
}

#[derive(Debug, Template)]
#[template(path = "dashboard.html")]
struct DashboardTemplate {
  uptime_seconds: u64,
  tcp_connections_total: u64,
  commands_total: u64,
  command_errors_total: u64,
  command_error_rate_percent: String,
  topic_count: usize,
  topic_error: Option<String>,
}

async fn ui_dashboard(State(state): State<AdminState>) -> impl IntoResponse {
  let snapshot = metrics::snapshot();
  let uptime_seconds = STARTED_AT
    .get()
    .map(|started| started.elapsed().as_secs())
    .unwrap_or(0);
  let command_error_rate_percent = if snapshot.commands_total == 0 {
    0.0
  } else {
    (snapshot.command_errors_total as f64 / snapshot.commands_total as f64) * 100.0
  };
  let (topic_count, topic_error) = match (state.topic_snapshot)() {
    Ok(topics) => (topics.len(), None),
    Err(err) => (0, Some(err)),
  };

  let template = DashboardTemplate {
    uptime_seconds,
    tcp_connections_total: snapshot.tcp_connections_total,
    commands_total: snapshot.commands_total,
    command_errors_total: snapshot.command_errors_total,
    command_error_rate_percent: format!("{command_error_rate_percent:.2}"),
    topic_count,
    topic_error,
  };

  match template.render() {
    Ok(html) => (
      StatusCode::OK,
      [(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
      )],
      html,
    )
      .into_response(),
    Err(err) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      format!("failed to render dashboard: {err}"),
    )
      .into_response(),
  }
}

#[derive(Debug, Template)]
#[template(path = "topics.html")]
struct TopicsTemplate {
  topics: Vec<TopicSummary>,
  error: Option<String>,
}

async fn ui_topics(State(state): State<AdminState>) -> impl IntoResponse {
  let (topics, error) = match (state.topic_snapshot)() {
    Ok(topics) => (topics, None),
    Err(err) => (Vec::new(), Some(err)),
  };

  let template = TopicsTemplate { topics, error };
  match template.render() {
    Ok(html) => (
      StatusCode::OK,
      [(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
      )],
      html,
    )
      .into_response(),
    Err(err) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      format!("failed to render topics page: {err}"),
    )
      .into_response(),
  }
}

#[derive(Debug, Deserialize)]
struct TopicMessagesQuery {
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

async fn ui_topic_messages(
  State(state): State<AdminState>,
  Path(topic): Path<String>,
  Query(query): Query<TopicMessagesQuery>,
) -> impl IntoResponse {
  let partition = query.partition.unwrap_or(0);
  let offset = query.offset.unwrap_or(0);
  let limit = query.limit.unwrap_or(50).clamp(1, 200);

  let result = (state.topic_messages_snapshot)(&topic, partition, offset, limit);
  let (records, next_offset, high_watermark, error) = match result {
    Ok(page) => (
      page.records,
      page.next_offset,
      page.high_watermark,
      if page.topic == topic && page.partition == partition {
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

  match template.render() {
    Ok(html) => (
      StatusCode::OK,
      [(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
      )],
      html,
    )
      .into_response(),
    Err(err) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      format!("failed to render topic messages page: {err}"),
    )
      .into_response(),
  }
}

#[derive(Debug, Template)]
#[template(path = "groups.html")]
struct GroupTemplate {
  group: String,
  explain: Option<GroupRebalanceExplainSummary>,
  members: Vec<GroupMemberSummary>,
  assignment: Option<GroupAssignmentSummary>,
  error: Option<String>,
}

async fn ui_group(State(state): State<AdminState>, Path(group): Path<String>) -> impl IntoResponse {
  let explain_result = (state.explain_rebalance)(&group);
  let members_result = (state.group_members_snapshot)(&group);
  let assignment_result = (state.group_assignment_snapshot)(&group);

  let mut errors = Vec::new();
  let explain = match explain_result {
    Ok(value) => Some(value),
    Err(err) => {
      errors.push(format!("explain query failed: {err}"));
      None
    }
  };
  let members = match members_result {
    Ok(value) => value,
    Err(err) => {
      errors.push(format!("members query failed: {err}"));
      Vec::new()
    }
  };
  let assignment = match assignment_result {
    Ok(value) => value,
    Err(err) => {
      errors.push(format!("assignment query failed: {err}"));
      None
    }
  };

  let template = GroupTemplate {
    group,
    explain,
    members,
    assignment,
    error: if errors.is_empty() {
      None
    } else {
      Some(errors.join("; "))
    },
  };

  match template.render() {
    Ok(html) => (
      StatusCode::OK,
      [(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
      )],
      html,
    )
      .into_response(),
    Err(err) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      format!("failed to render groups page: {err}"),
    )
      .into_response(),
  }
}

async fn metrics_handler() -> impl IntoResponse {
  match metrics::render() {
    Ok(body) => {
      let mut headers = HeaderMap::new();
      headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
      );
      (StatusCode::OK, headers, body).into_response()
    }
    Err(message) => (StatusCode::INTERNAL_SERVER_ERROR, message).into_response(),
  }
}

#[utoipa::path(
  get,
  path = "/api/groups/{group}/members",
  tag = "admin",
  params(
    ("group" = String, Path, description = "Consumer group name")
  ),
  responses(
    (status = 200, description = "Active group members", body = [GroupMemberSummary]),
    (status = 500, description = "Server error")
  )
)]
async fn api_group_members(
  State(state): State<AdminState>,
  Path(group): Path<String>,
) -> impl IntoResponse {
  match (state.group_members_snapshot)(&group) {
    Ok(members) => (StatusCode::OK, axum::Json(members)).into_response(),
    Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
  }
}

#[utoipa::path(
  get,
  path = "/api/groups/{group}/assignment",
  tag = "admin",
  params(
    ("group" = String, Path, description = "Consumer group name")
  ),
  responses(
    (status = 200, description = "Latest assignment snapshot", body = Option<GroupAssignmentSummary>),
    (status = 500, description = "Server error")
  )
)]
async fn api_group_assignment(
  State(state): State<AdminState>,
  Path(group): Path<String>,
) -> impl IntoResponse {
  match (state.group_assignment_snapshot)(&group) {
    Ok(assignment) => (StatusCode::OK, axum::Json(assignment)).into_response(),
    Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
  }
}

#[utoipa::path(
  post,
  path = "/api/groups/{group}/rebalance",
  tag = "admin",
  params(
    ("group" = String, Path, description = "Consumer group name")
  ),
  responses(
    (status = 200, description = "Rebalance result snapshot", body = GroupAssignmentSummary),
    (status = 500, description = "Server error")
  )
)]
async fn api_group_rebalance(
  State(state): State<AdminState>,
  Path(group): Path<String>,
) -> impl IntoResponse {
  match (state.rebalance_group)(&group) {
    Ok(assignment) => (StatusCode::OK, axum::Json(assignment)).into_response(),
    Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
  }
}

#[utoipa::path(
  get,
  path = "/api/groups/{group}/rebalance-explain",
  tag = "admin",
  params(
    ("group" = String, Path, description = "Consumer group name")
  ),
  responses(
    (status = 200, description = "Rebalance explain diagnostics", body = GroupRebalanceExplainSummary),
    (status = 500, description = "Server error")
  )
)]
async fn api_group_rebalance_explain(
  State(state): State<AdminState>,
  Path(group): Path<String>,
) -> impl IntoResponse {
  match (state.explain_rebalance)(&group) {
    Ok(explain) => (StatusCode::OK, axum::Json(explain)).into_response(),
    Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
  }
}
