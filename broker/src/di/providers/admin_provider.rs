use std::sync::Arc;

use crate::{admin::AdminServerConfig, config::AppConfig};

use super::super::{BrokerServiceHandle, admin_module::AdminModule};

pub(super) fn build_admin_config(
  app: &AppConfig,
  service: Arc<BrokerServiceHandle>,
) -> Option<AdminServerConfig> {
  AdminModule::new(app.clone()).build_admin_config(service)
}
