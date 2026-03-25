use std::sync::Arc;

use fluxdi::Injector;

use crate::{
  admin::AdminServerConfig, config::AppConfig, server::TcpServerConfig, service::BrokerService,
};
use store::{RedbMetadataStore, SegmentLog};

mod admin_module;
mod bootstrap_module;
mod config_module;
mod lifecycle_module;
mod network_module;
mod providers;
mod runtime_module;
#[cfg(test)]
mod tests;

use config_module::ConfigModule;
use network_module::NetworkModule;
use runtime_module::RuntimeModule;

pub use bootstrap_module::BootstrapModule;
pub use lifecycle_module::LifecycleModule;

pub type BrokerServiceHandle = BrokerService<Arc<SegmentLog>, Arc<RedbMetadataStore>>;

pub struct RuntimeWiring {
  pub service: Arc<BrokerServiceHandle>,
  pub tcp_config: TcpServerConfig,
  pub admin_config: Option<AdminServerConfig>,
}

pub(crate) trait DiModule {
  fn register(&self, injector: &Injector);
}

pub fn wire_runtime(
  app: &AppConfig,
  service: Arc<BrokerServiceHandle>,
) -> Result<RuntimeWiring, String> {
  let injector = Injector::root();
  let modules: Vec<Box<dyn DiModule>> = vec![
    Box::new(ConfigModule::new(app.clone())),
    Box::new(RuntimeModule::new(service)),
    Box::new(NetworkModule::new(TcpServerConfig {
      bind_addr: app.broker.bind_addr.clone(),
      max_frame_body_bytes: app.broker.max_frame_body_bytes,
    })),
  ];
  for module in modules {
    module.register(&injector);
  }

  let service_resolved = providers::resolve_service(&injector)?;
  let tcp_resolved = providers::resolve_tcp_config(&injector)?;
  let admin_config = providers::build_admin_config(app, Arc::clone(&service_resolved));

  Ok(RuntimeWiring {
    service: service_resolved,
    tcp_config: tcp_resolved,
    admin_config,
  })
}
