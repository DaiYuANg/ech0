use std::{future::Future, sync::Arc};

use openraft::{
  BasicNode, Raft,
  error::{ClientWriteError, InitializeError, RaftError},
};
use store::{
  ApplyResult, ConsensusLogStore, ConsensusMetadataStore, LocalPartitionStateStore,
  MessageLogStore, MutablePartitionLogStore, OffsetStore, ReplicatedPartitionCommandEnvelope,
  Result as StoreResult, StoreError, TopicCatalogStore, TopicConfig, TopicPartition,
};
use tracing::warn;

use super::{
  BrokerRaftRequest, BrokerRaftResponse, EchoRaftTypeConfig, OpenRaftRuntimeConfig,
  network::{TcpRaftNetworkFactory, run_raft_rpc_server, send_client_write_request},
  storage::{BrokerRaftLogStore, BrokerRaftStateMachine},
};

#[derive(Clone)]
pub struct BrokerRaftRuntime<L, M> {
  config: OpenRaftRuntimeConfig,
  executor: BrokerRaftExecutor,
  raft: Raft<EchoRaftTypeConfig>,
  _marker: std::marker::PhantomData<(L, M)>,
}

impl<L, M> std::fmt::Debug for BrokerRaftRuntime<L, M> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("BrokerRaftRuntime")
      .field("node_id", &self.config.node_id)
      .field("bind_addr", &self.config.bind_addr)
      .field("read_policy", &self.config.read_policy)
      .finish()
  }
}

#[derive(Clone)]
enum BrokerRaftExecutor {
  Owned(Arc<tokio::runtime::Runtime>),
  Handle(tokio::runtime::Handle),
}

impl<L, M> BrokerRaftRuntime<L, M>
where
  L: MessageLogStore + MutablePartitionLogStore + Send + Sync + 'static,
  M: OffsetStore
    + TopicCatalogStore
    + LocalPartitionStateStore
    + ConsensusLogStore
    + ConsensusMetadataStore
    + Send
    + Sync
    + 'static,
{
  pub fn new(config: OpenRaftRuntimeConfig, log: Arc<L>, meta: Arc<M>) -> StoreResult<Self> {
    let executor = match tokio::runtime::Handle::try_current() {
      Ok(handle) => BrokerRaftExecutor::Handle(handle),
      Err(_) => BrokerRaftExecutor::Owned(Arc::new(
        tokio::runtime::Builder::new_multi_thread()
          .worker_threads(2)
          .enable_all()
          .build()
          .map_err(StoreError::Io)?,
      )),
    };

    let runtime_config = config.clone();
    let init = async move {
      let consensus_group = runtime_config.consensus_group();
      let log_store = BrokerRaftLogStore::new(consensus_group.clone(), Arc::clone(&meta));
      let state_machine = BrokerRaftStateMachine::new(log, meta, consensus_group)?;
      let raft = Raft::<EchoRaftTypeConfig>::new(
        runtime_config.node_id,
        Arc::clone(&runtime_config.config),
        TcpRaftNetworkFactory,
        log_store,
        state_machine,
      )
      .await
      .map_err(|err| StoreError::Codec(format!("failed to start openraft runtime: {err}")))?;

      if runtime_config.network_enabled() {
        let server_addr = runtime_config.bind_addr.clone();
        let server_raft = raft.clone();
        tokio::spawn(async move {
          if let Err(err) = run_raft_rpc_server(server_addr.clone(), server_raft).await {
            warn!(bind_addr = %server_addr, error = %err, "raft rpc server stopped");
          }
        });
      }

      initialize_cluster_if_needed(&raft, &runtime_config).await?;

      Ok::<_, StoreError>(raft)
    };
    let raft = run_with_executor(&executor, init)?;

    Ok(Self {
      config,
      executor,
      raft,
      _marker: std::marker::PhantomData,
    })
  }

  pub fn config(&self) -> &OpenRaftRuntimeConfig {
    &self.config
  }

  pub fn current_leader(&self) -> StoreResult<Option<u64>> {
    Ok(self.block_on(self.raft.current_leader()))
  }

  pub fn ensure_linearizable_read(&self) -> StoreResult<()> {
    match self.block_on(self.raft.ensure_linearizable()) {
      Ok(_) => Ok(()),
      Err(err) => match leader_id_from_forward(err.forward_to_leader(), &self.config) {
        Some(leader_id) => Err(StoreError::NotLeader {
          leader_id: Some(leader_id),
        }),
        None => Err(StoreError::Codec(format!(
          "failed to ensure linearizable read: {err}"
        ))),
      },
    }
  }

  pub fn ensure_topic(&self, topic: TopicConfig) -> StoreResult<()> {
    match self.submit(BrokerRaftRequest::EnsureTopic { topic })? {
      BrokerRaftResponse::TopicEnsured => Ok(()),
      other => Err(StoreError::Corruption(format!(
        "unexpected raft response for ensure_topic: {other:?}"
      ))),
    }
  }

  pub fn apply_partition(
    &self,
    envelope: ReplicatedPartitionCommandEnvelope,
  ) -> StoreResult<ApplyResult> {
    match self.submit(BrokerRaftRequest::ApplyPartition { envelope })? {
      BrokerRaftResponse::PartitionApplied { result } => Ok(result),
      other => Err(StoreError::Corruption(format!(
        "unexpected raft response for partition apply: {other:?}"
      ))),
    }
  }

  pub fn save_consumer_offset(
    &self,
    consumer: &str,
    topic_partition: &TopicPartition,
    next_offset: u64,
  ) -> StoreResult<()> {
    match self.submit(BrokerRaftRequest::SaveConsumerOffset {
      consumer: consumer.to_owned(),
      topic_partition: topic_partition.clone(),
      next_offset,
    })? {
      BrokerRaftResponse::ConsumerOffsetSaved => Ok(()),
      other => Err(StoreError::Corruption(format!(
        "unexpected raft response for offset save: {other:?}"
      ))),
    }
  }

  pub fn submit(&self, request: BrokerRaftRequest) -> StoreResult<BrokerRaftResponse> {
    self.block_on(self.submit_async(request))
  }

  fn block_on<F, T>(&self, future: F) -> T
  where
    F: Future<Output = T>,
  {
    run_with_executor(&self.executor, future)
  }

  async fn submit_async(&self, request: BrokerRaftRequest) -> StoreResult<BrokerRaftResponse> {
    let mut redirect: Option<BasicNode> = None;

    for _ in 0..5 {
      let response = match redirect.take() {
        Some(node) => send_client_write_request(&node.addr, &request).await,
        None => self.raft.client_write(request.clone()).await,
      };

      match response {
        Ok(response) => return Ok(response.data),
        Err(err) => {
          if let Some(node) = leader_node_from_write_error(&err, &self.config) {
            redirect = Some(node);
            continue;
          }
          return Err(StoreError::Codec(format!(
            "raft client write failed: {err}"
          )));
        }
      }
    }

    Err(StoreError::Codec(
      "raft client write exceeded forwarding limit".to_owned(),
    ))
  }
}

fn run_with_executor<F, T>(executor: &BrokerRaftExecutor, future: F) -> T
where
  F: Future<Output = T>,
{
  match executor {
    BrokerRaftExecutor::Owned(runtime) => {
      if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| runtime.block_on(future))
      } else {
        runtime.block_on(future)
      }
    }
    BrokerRaftExecutor::Handle(handle) => {
      if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| handle.block_on(future))
      } else {
        handle.block_on(future)
      }
    }
  }
}

fn leader_node_from_write_error(
  err: &RaftError<u64, ClientWriteError<u64, BasicNode>>,
  config: &OpenRaftRuntimeConfig,
) -> Option<BasicNode> {
  leader_node_from_forward(err.forward_to_leader(), config)
}

fn leader_node_from_forward(
  forward: Option<&openraft::error::ForwardToLeader<u64, BasicNode>>,
  config: &OpenRaftRuntimeConfig,
) -> Option<BasicNode> {
  forward.and_then(|forward| {
    forward.leader_node.clone().or_else(|| {
      forward
        .leader_id
        .and_then(|node_id| config.known_nodes.get(&node_id).cloned())
    })
  })
}

fn leader_id_from_forward(
  forward: Option<&openraft::error::ForwardToLeader<u64, BasicNode>>,
  config: &OpenRaftRuntimeConfig,
) -> Option<u64> {
  forward.and_then(|forward| {
    forward.leader_id.or_else(|| {
      forward.leader_node.as_ref().and_then(|leader_node| {
        config
          .known_nodes
          .iter()
          .find_map(|(node_id, known_node)| (known_node == leader_node).then_some(*node_id))
      })
    })
  })
}

async fn initialize_cluster_if_needed(
  raft: &Raft<EchoRaftTypeConfig>,
  config: &OpenRaftRuntimeConfig,
) -> StoreResult<()> {
  if config.known_nodes.is_empty() {
    return Ok(());
  }

  if !config.known_nodes.contains_key(&config.node_id) {
    return Err(StoreError::Codec(format!(
      "raft node {} is missing from configured cluster membership",
      config.node_id
    )));
  }

  if raft
    .is_initialized()
    .await
    .map_err(|err| StoreError::Codec(format!("failed to query raft init state: {err}")))?
  {
    return Ok(());
  }

  match raft.initialize(config.known_nodes.clone()).await {
    Ok(()) => Ok(()),
    Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
    Err(err) => Err(StoreError::Codec(format!(
      "failed to initialize raft cluster: {err}"
    ))),
  }
}
