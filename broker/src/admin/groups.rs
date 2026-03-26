use askama::Template;
use axum::{
  extract::{Path, State},
  response::IntoResponse,
};

use super::{
  AdminState, GroupAssignmentSummary, GroupLagSummary, GroupMemberSummary,
  GroupRebalanceExplainSummary,
  render_html,
};

#[derive(Debug, Template)]
#[template(path = "groups.html")]
struct GroupTemplate {
  group: String,
  explain: Option<GroupRebalanceExplainSummary>,
  members: Vec<GroupMemberSummary>,
  assignment: Option<GroupAssignmentSummary>,
  lag: Option<GroupLagSummary>,
  error: Option<String>,
}

pub(super) async fn ui_group(
  State(state): State<AdminState>,
  Path(group): Path<String>,
) -> impl IntoResponse {
  let explain_result = (state.explain_rebalance)(&group);
  let members_result = (state.group_members_snapshot)(&group);
  let assignment_result = (state.group_assignment_snapshot)(&group);
  let lag_result = (state.group_lag_snapshot)(&group);

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
  let lag = match lag_result {
    Ok(value) => value,
    Err(err) => {
      errors.push(format!("lag query failed: {err}"));
      None
    }
  };

  let template = GroupTemplate {
    group,
    explain,
    members,
    assignment,
    lag,
    error: if errors.is_empty() {
      None
    } else {
      Some(errors.join("; "))
    },
  };

  render_html(template.render(), "groups page")
}
