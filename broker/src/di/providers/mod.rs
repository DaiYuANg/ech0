use std::sync::Arc;

use fluxdi::Injector;

use crate::{admin::AdminServerConfig, config::AppConfig, server::TcpServerConfig};

use super::BrokerServiceHandle;

mod admin_provider;
mod runtime_provider;
mod tcp_provider;

pub(super) fn resolve_service(
  injector: &Injector,
) -> std::result::Result<Arc<BrokerServiceHandle>, String> {
  runtime_provider::resolve_service(injector)
}

pub(super) fn resolve_tcp_config(
  injector: &Injector,
) -> std::result::Result<TcpServerConfig, String> {
  tcp_provider::resolve_tcp_config(injector)
}

pub(super) fn build_admin_config(
  app: &AppConfig,
  service: Arc<BrokerServiceHandle>,
) -> Option<AdminServerConfig> {
  admin_provider::build_admin_config(app, service)
}
