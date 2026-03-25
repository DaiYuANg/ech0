use std::sync::Arc;

use fluxdi::Injector;

use crate::di::BrokerServiceHandle;

pub(super) fn resolve_service(
  injector: &Injector,
) -> std::result::Result<Arc<BrokerServiceHandle>, String> {
  injector
    .try_resolve::<BrokerServiceHandle>()
    .map_err(|err| format!("failed to resolve BrokerService: {err}"))
}
