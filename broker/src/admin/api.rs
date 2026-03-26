use axum::{
  extract::{Path, State},
  response::IntoResponse,
  http::StatusCode,
};

use super::{
  AdminState, GroupAssignmentSummary, GroupLagSummary, GroupMemberSummary,
  GroupRebalanceExplainSummary,
};

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
pub(super) async fn api_group_members(
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
pub(super) async fn api_group_assignment(
  State(state): State<AdminState>,
  Path(group): Path<String>,
) -> impl IntoResponse {
  match (state.group_assignment_snapshot)(&group) {
    Ok(assignment) => (StatusCode::OK, axum::Json(assignment)).into_response(),
    Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
  }
}

#[utoipa::path(
  get,
  path = "/api/groups/{group}/lag",
  tag = "admin",
  params(
    ("group" = String, Path, description = "Consumer group name")
  ),
  responses(
    (status = 200, description = "Current group lag snapshot", body = Option<GroupLagSummary>),
    (status = 500, description = "Server error")
  )
)]
pub(super) async fn api_group_lag(
  State(state): State<AdminState>,
  Path(group): Path<String>,
) -> impl IntoResponse {
  match (state.group_lag_snapshot)(&group) {
    Ok(lag) => (StatusCode::OK, axum::Json(lag)).into_response(),
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
pub(super) async fn api_group_rebalance(
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
pub(super) async fn api_group_rebalance_explain(
  State(state): State<AdminState>,
  Path(group): Path<String>,
) -> impl IntoResponse {
  match (state.explain_rebalance)(&group) {
    Ok(explain) => (StatusCode::OK, axum::Json(explain)).into_response(),
    Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
  }
}
