use std::io;

use openraft::{
  BasicNode, Raft, RaftNetwork, RaftNetworkFactory,
  error::{ClientWriteError, RPCError, RaftError, RemoteError, Unreachable},
};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};
use transport::{Frame, read_frame, write_frame};

use super::{BrokerRaftRequest, EchoRaftTypeConfig};

const RAFT_RPC_VERSION: u8 = 1;
const CMD_RAFT_APPEND_ENTRIES: u16 = 201;
const CMD_RAFT_VOTE: u16 = 202;
const CMD_RAFT_INSTALL_SNAPSHOT: u16 = 203;
const CMD_RAFT_CLIENT_WRITE: u16 = 204;

#[derive(Debug, Clone, Copy)]
pub(super) struct TcpRaftNetworkFactory;

impl RaftNetworkFactory<EchoRaftTypeConfig> for TcpRaftNetworkFactory {
  type Network = TcpRaftNetwork;

  async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
    TcpRaftNetwork {
      target,
      node: node.clone(),
    }
  }
}

#[derive(Debug, Clone)]
pub(super) struct TcpRaftNetwork {
  target: u64,
  node: BasicNode,
}

impl RaftNetwork<EchoRaftTypeConfig> for TcpRaftNetwork {
  async fn append_entries(
    &mut self,
    rpc: openraft::raft::AppendEntriesRequest<EchoRaftTypeConfig>,
    _option: openraft::network::RPCOption,
  ) -> Result<openraft::raft::AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
  {
    let response = send_raft_rpc::<
      _,
      Result<openraft::raft::AppendEntriesResponse<u64>, RaftError<u64>>,
    >(&self.node.addr, CMD_RAFT_APPEND_ENTRIES, &rpc)
    .await
    .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?;

    response.map_err(|err| {
      RPCError::RemoteError(RemoteError::new_with_node(
        self.target,
        self.node.clone(),
        err,
      ))
    })
  }

  async fn install_snapshot(
    &mut self,
    rpc: openraft::raft::InstallSnapshotRequest<EchoRaftTypeConfig>,
    _option: openraft::network::RPCOption,
  ) -> Result<
    openraft::raft::InstallSnapshotResponse<u64>,
    RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
  > {
    let response = send_raft_rpc::<
      _,
      Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RaftError<u64, openraft::error::InstallSnapshotError>,
      >,
    >(&self.node.addr, CMD_RAFT_INSTALL_SNAPSHOT, &rpc)
    .await
    .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?;

    response.map_err(|err| {
      RPCError::RemoteError(RemoteError::new_with_node(
        self.target,
        self.node.clone(),
        err,
      ))
    })
  }

  async fn vote(
    &mut self,
    rpc: openraft::raft::VoteRequest<u64>,
    _option: openraft::network::RPCOption,
  ) -> Result<openraft::raft::VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
    let response = send_raft_rpc::<_, Result<openraft::raft::VoteResponse<u64>, RaftError<u64>>>(
      &self.node.addr,
      CMD_RAFT_VOTE,
      &rpc,
    )
    .await
    .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?;

    response.map_err(|err| {
      RPCError::RemoteError(RemoteError::new_with_node(
        self.target,
        self.node.clone(),
        err,
      ))
    })
  }
}

pub(super) async fn run_raft_rpc_server(
  bind_addr: String,
  raft: Raft<EchoRaftTypeConfig>,
) -> io::Result<()> {
  let listener = TcpListener::bind(&bind_addr).await?;
  info!(bind_addr = %bind_addr, "raft rpc server listening");

  loop {
    let (stream, remote_addr) = listener.accept().await?;
    let peer = remote_addr.to_string();
    let raft = raft.clone();
    tokio::spawn(async move {
      if let Err(err) = handle_raft_rpc_connection(peer.clone(), stream, raft).await {
        warn!(peer = %peer, error = %err, "raft rpc connection closed with error");
      }
    });
  }
}

async fn handle_raft_rpc_connection(
  peer: String,
  mut stream: TcpStream,
  raft: Raft<EchoRaftTypeConfig>,
) -> io::Result<()> {
  let request = read_frame(&mut stream).await?;
  let response = match request.header.command {
    CMD_RAFT_APPEND_ENTRIES => {
      let rpc: openraft::raft::AppendEntriesRequest<EchoRaftTypeConfig> =
        decode_raft_request(&request)?;
      let result = raft.append_entries(rpc).await;
      debug!(peer = %peer, "handled raft append_entries rpc");
      encode_raft_response(CMD_RAFT_APPEND_ENTRIES, &result)?
    }
    CMD_RAFT_VOTE => {
      let rpc: openraft::raft::VoteRequest<u64> = decode_raft_request(&request)?;
      let result = raft.vote(rpc).await;
      debug!(peer = %peer, "handled raft vote rpc");
      encode_raft_response(CMD_RAFT_VOTE, &result)?
    }
    CMD_RAFT_INSTALL_SNAPSHOT => {
      let rpc: openraft::raft::InstallSnapshotRequest<EchoRaftTypeConfig> =
        decode_raft_request(&request)?;
      let result = raft.install_snapshot(rpc).await;
      debug!(peer = %peer, "handled raft install_snapshot rpc");
      encode_raft_response(CMD_RAFT_INSTALL_SNAPSHOT, &result)?
    }
    CMD_RAFT_CLIENT_WRITE => {
      let rpc: BrokerRaftRequest = decode_raft_request(&request)?;
      let result = raft.client_write(rpc).await;
      debug!(peer = %peer, "handled raft client_write rpc");
      encode_raft_response(CMD_RAFT_CLIENT_WRITE, &result)?
    }
    command => {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unknown raft rpc command {command}"),
      ));
    }
  };

  write_frame(&mut stream, &response).await
}

pub(super) async fn send_client_write_request(
  addr: &str,
  request: &BrokerRaftRequest,
) -> Result<
  openraft::raft::ClientWriteResponse<EchoRaftTypeConfig>,
  RaftError<u64, ClientWriteError<u64, BasicNode>>,
> {
  let response = send_raft_rpc::<
    _,
    Result<
      openraft::raft::ClientWriteResponse<EchoRaftTypeConfig>,
      RaftError<u64, ClientWriteError<u64, BasicNode>>,
    >,
  >(addr, CMD_RAFT_CLIENT_WRITE, request)
  .await
  .map_err(|_| RaftError::Fatal(openraft::error::Fatal::Panicked))?;

  response
}

async fn send_raft_rpc<Req, Resp>(addr: &str, command: u16, request: &Req) -> io::Result<Resp>
where
  Req: Serialize,
  Resp: for<'de> Deserialize<'de>,
{
  let body = serde_json::to_vec(request).map_err(|err| {
    io::Error::new(
      io::ErrorKind::InvalidData,
      format!("failed to encode raft request: {err}"),
    )
  })?;

  let mut stream = TcpStream::connect(addr).await?;
  let frame = Frame::new(RAFT_RPC_VERSION, command, body)?;
  write_frame(&mut stream, &frame).await?;
  let response = read_frame(&mut stream).await?;
  if response.header.version != RAFT_RPC_VERSION {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      format!(
        "unsupported raft rpc version {}, expected {}",
        response.header.version, RAFT_RPC_VERSION
      ),
    ));
  }

  serde_json::from_slice(&response.body).map_err(|err| {
    io::Error::new(
      io::ErrorKind::InvalidData,
      format!("failed to decode raft response: {err}"),
    )
  })
}

fn decode_raft_request<T>(frame: &Frame) -> io::Result<T>
where
  T: for<'de> Deserialize<'de>,
{
  if frame.header.version != RAFT_RPC_VERSION {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      format!(
        "unsupported raft rpc version {}, expected {}",
        frame.header.version, RAFT_RPC_VERSION
      ),
    ));
  }

  serde_json::from_slice(&frame.body).map_err(|err| {
    io::Error::new(
      io::ErrorKind::InvalidData,
      format!("failed to decode raft request: {err}"),
    )
  })
}

fn encode_raft_response<T>(command: u16, response: &T) -> io::Result<Frame>
where
  T: Serialize,
{
  let body = serde_json::to_vec(response).map_err(|err| {
    io::Error::new(
      io::ErrorKind::InvalidData,
      format!("failed to encode raft response: {err}"),
    )
  })?;
  Frame::new(RAFT_RPC_VERSION, command, body)
}
