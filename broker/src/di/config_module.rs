use fluxdi::{Injector, Provider, Shared};

use crate::config::AppConfig;

use super::DiModule;

pub(super) struct ConfigModule {
  app: AppConfig,
}

impl ConfigModule {
  pub fn new(app: AppConfig) -> Self {
    Self { app }
  }
}

impl DiModule for ConfigModule {
  fn register(&self, injector: &Injector) {
    let app_cfg = self.app.clone();
    injector.provide::<AppConfig>(Provider::root(move |_| Shared::new(app_cfg.clone())));
  }
}
