use crate::raft::OpenRaftRuntimeConfig;

#[derive(Debug, Clone)]
pub enum BrokerRuntimeMode {
  Standalone,
  Raft(OpenRaftRuntimeConfig),
}

impl BrokerRuntimeMode {
  pub fn from_raft_runtime(runtime: Option<OpenRaftRuntimeConfig>) -> Self {
    match runtime {
      Some(runtime) => Self::Raft(runtime),
      None => Self::Standalone,
    }
  }

  pub fn is_raft(&self) -> bool {
    matches!(self, Self::Raft(_))
  }

  pub fn label(&self) -> &'static str {
    match self {
      Self::Standalone => "standalone",
      Self::Raft(_) => "raft",
    }
  }
}

impl Default for BrokerRuntimeMode {
  fn default() -> Self {
    Self::Standalone
  }
}

#[derive(Debug, Clone)]
pub struct BrokerIdentity {
  pub node_id: u64,
  pub cluster_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRecord {
  pub offset: u64,
  pub timestamp_ms: u64,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResult {
  pub topic: String,
  pub partition: u32,
  pub records: Vec<FetchedRecord>,
  pub next_offset: u64,
  pub high_watermark: Option<u64>,
}
