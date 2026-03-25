use std::sync::Arc;

use fluxdi::{Injector, Provider};

use super::{BrokerServiceHandle, DiModule};

pub(super) struct RuntimeModule {
  service: Arc<BrokerServiceHandle>,
}

impl RuntimeModule {
  pub fn new(service: Arc<BrokerServiceHandle>) -> Self {
    Self { service }
  }
}

impl DiModule for RuntimeModule {
  fn register(&self, injector: &Injector) {
    let service_for_provider = Arc::clone(&self.service);
    injector
      .provide::<BrokerServiceHandle>(Provider::root(move |_| Arc::clone(&service_for_provider)));
  }
}
