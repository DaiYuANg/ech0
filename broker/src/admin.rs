use std::sync::OnceLock;
use std::time::Instant;
use std::{io, sync::Arc};

use axum::{
  Router,
  extract::State,
  http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE},
  response::{IntoResponse, Redirect, Response},
  routing::{get, post},
};
use tokio::net::TcpListener;
use tracing::info;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::metrics;

mod api;
mod dashboard;
mod groups;
pub(crate) mod health;
mod models;
mod topics;

pub use health::{
  BackgroundWorkersHealthResponse, CompactionCleanupHealthResponse, HealthResponse,
  OpsConfigSummary, RaftHealthResponse, RetentionCleanupHealthResponse,
};

use self::api::{
  api_group_assignment, api_group_lag, api_group_members, api_group_rebalance,
  api_group_rebalance_explain,
};
use self::dashboard::ui_dashboard;
use self::groups::ui_group;
pub(crate) use self::health::error_health_response;
pub use self::models::{
  GroupAssignmentSummary, GroupLagSummary, GroupMemberLoadSummary, GroupMemberSummary,
  GroupPartitionLagSummary, GroupPartitionOwnerSummary, GroupRebalanceExplainSummary,
  HotPartitionSummary, TopicMessageSummary, TopicMessagesPageSummary, TopicSummary,
};
use self::topics::{ui_topic_messages, ui_topics};

#[derive(Clone)]
pub struct AdminServerConfig {
  pub bind_addr: String,
  pub ops_config: OpsConfigSummary,
  pub health_snapshot: HealthSnapshotFn,
  pub metrics_refresh: MetricsRefreshFn,
  pub topic_snapshot: TopicSnapshotFn,
  pub topic_messages_snapshot: TopicMessagesSnapshotFn,
  pub group_members_snapshot: GroupMembersSnapshotFn,
  pub group_assignment_snapshot: GroupAssignmentSnapshotFn,
  pub group_lag_snapshot: GroupLagSnapshotFn,
  pub rebalance_group: GroupRebalanceFn,
  pub explain_rebalance: GroupRebalanceExplainFn,
}

static STARTED_AT: OnceLock<Instant> = OnceLock::new();

pub type HealthSnapshotFn = Arc<dyn Fn() -> Result<HealthResponse, String> + Send + Sync>;
pub type MetricsRefreshFn = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;
pub type TopicSnapshotFn = Arc<dyn Fn() -> Result<Vec<TopicSummary>, String> + Send + Sync>;
pub type TopicMessagesSnapshotFn =
  Arc<dyn Fn(&str, u32, u64, usize) -> Result<TopicMessagesPageSummary, String> + Send + Sync>;
pub type GroupMembersSnapshotFn =
  Arc<dyn Fn(&str) -> Result<Vec<GroupMemberSummary>, String> + Send + Sync>;
pub type GroupAssignmentSnapshotFn =
  Arc<dyn Fn(&str) -> Result<Option<GroupAssignmentSummary>, String> + Send + Sync>;
pub type GroupLagSnapshotFn =
  Arc<dyn Fn(&str) -> Result<Option<GroupLagSummary>, String> + Send + Sync>;
pub type GroupRebalanceFn =
  Arc<dyn Fn(&str) -> Result<GroupAssignmentSummary, String> + Send + Sync>;
pub type GroupRebalanceExplainFn =
  Arc<dyn Fn(&str) -> Result<GroupRebalanceExplainSummary, String> + Send + Sync>;


#[derive(Clone)]
pub(crate) struct AdminState {
  ops_config: OpsConfigSummary,
  health_snapshot: HealthSnapshotFn,
  metrics_refresh: MetricsRefreshFn,
  topic_snapshot: TopicSnapshotFn,
  topic_messages_snapshot: TopicMessagesSnapshotFn,
  group_members_snapshot: GroupMembersSnapshotFn,
  group_assignment_snapshot: GroupAssignmentSnapshotFn,
  group_lag_snapshot: GroupLagSnapshotFn,
  rebalance_group: GroupRebalanceFn,
  explain_rebalance: GroupRebalanceExplainFn,
}

#[derive(OpenApi)]
#[openapi(
  paths(
    healthz,
    api::api_group_members,
    api::api_group_assignment,
    api::api_group_lag,
    api::api_group_rebalance,
    api::api_group_rebalance_explain
  ),
    components(
      schemas(
        HealthResponse,
        RaftHealthResponse,
        RetentionCleanupHealthResponse,
        CompactionCleanupHealthResponse,
        BackgroundWorkersHealthResponse,
        GroupMemberSummary,
        GroupAssignmentSummary,
        GroupLagSummary,
        GroupPartitionLagSummary,
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
    ops_config: config.ops_config,
    health_snapshot: Arc::clone(&config.health_snapshot),
    metrics_refresh: Arc::clone(&config.metrics_refresh),
    topic_snapshot: Arc::clone(&config.topic_snapshot),
    topic_messages_snapshot: Arc::clone(&config.topic_messages_snapshot),
    group_members_snapshot: Arc::clone(&config.group_members_snapshot),
    group_assignment_snapshot: Arc::clone(&config.group_assignment_snapshot),
    group_lag_snapshot: Arc::clone(&config.group_lag_snapshot),
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
    .route("/api/groups/{group}/lag", get(api_group_lag))
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

#[utoipa::path(
  get,
  path = "/healthz",
  tag = "admin",
  responses(
    (status = 200, description = "Healthy broker status", body = HealthResponse),
    (status = 503, description = "Degraded or unavailable broker status", body = HealthResponse)
  )
)]
async fn healthz(State(state): State<AdminState>) -> impl IntoResponse {
  match (state.health_snapshot)() {
    Ok(health) => {
      let status = if health.status == "ok" {
        StatusCode::OK
      } else {
        StatusCode::SERVICE_UNAVAILABLE
      };
      (status, axum::Json(health)).into_response()
    }
    Err(err) => (
      StatusCode::SERVICE_UNAVAILABLE,
      axum::Json(error_health_response(&state.ops_config, err)),
    )
      .into_response(),
  }
}

async fn root() -> impl IntoResponse {
  Redirect::to("/ui")
}

pub(crate) fn uptime_seconds() -> u64 {
  STARTED_AT
    .get()
    .map(|started| started.elapsed().as_secs())
    .unwrap_or(0)
}

pub(crate) fn render_html(rendered: Result<String, askama::Error>, page: &str) -> Response {
  match rendered {
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
      format!("failed to render {page}: {err}"),
    )
      .into_response(),
  }
}

async fn metrics_handler(State(state): State<AdminState>) -> impl IntoResponse {
  if let Err(message) = (state.metrics_refresh)() {
    return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
  }
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
