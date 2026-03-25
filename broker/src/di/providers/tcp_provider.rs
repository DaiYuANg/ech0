use fluxdi::Injector;

use crate::server::TcpServerConfig;

pub(super) fn resolve_tcp_config(
  injector: &Injector,
) -> std::result::Result<TcpServerConfig, String> {
  injector
    .try_resolve::<TcpServerConfig>()
    .map(|resolved| (*resolved).clone())
    .map_err(|err| format!("failed to resolve TcpServerConfig: {err}"))
}
