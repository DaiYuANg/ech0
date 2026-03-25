use fluxdi::{Injector, Provider, Shared};

use crate::server::TcpServerConfig;

use super::DiModule;

pub(super) struct NetworkModule {
  tcp_config: TcpServerConfig,
}

impl NetworkModule {
  pub fn new(tcp_config: TcpServerConfig) -> Self {
    Self { tcp_config }
  }
}

impl DiModule for NetworkModule {
  fn register(&self, injector: &Injector) {
    let tcp_cfg = self.tcp_config.clone();
    injector.provide::<TcpServerConfig>(Provider::root(move |_| Shared::new(tcp_cfg.clone())));
  }
}
