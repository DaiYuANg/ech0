use serde::{Deserialize, Serialize};
use store::{ConsensusMetadataStore, Result as StoreResult, StoreError};

pub(super) const CONSENSUS_VOTE_KEY: &str = "vote";
pub(super) const CONSENSUS_COMMITTED_KEY: &str = "committed";
pub(super) const CONSENSUS_LAST_PURGED_LOG_ID_KEY: &str = "last_purged_log_id";
pub(super) const CONSENSUS_LAST_APPLIED_LOG_ID_KEY: &str = "state_machine.last_applied_log_id";
pub(super) const CONSENSUS_LAST_MEMBERSHIP_KEY: &str = "state_machine.last_membership";
pub(super) const CONSENSUS_CURRENT_SNAPSHOT_KEY: &str = "state_machine.current_snapshot";

pub(super) fn encode_json_value<T>(label: &str, value: &T) -> StoreResult<Vec<u8>>
where
  T: Serialize,
{
  serde_json::to_vec(value)
    .map_err(|err| StoreError::Codec(format!("failed to encode {label}: {err}")))
}

pub(super) fn decode_json_value<T>(label: &str, payload: &[u8]) -> StoreResult<T>
where
  T: for<'de> Deserialize<'de>,
{
  serde_json::from_slice(payload)
    .map_err(|err| StoreError::Codec(format!("failed to decode {label}: {err}")))
}

pub(super) fn save_consensus_json_value<T, M>(
  store: &M,
  group: &str,
  key: &str,
  value: &T,
) -> StoreResult<()>
where
  T: Serialize,
  M: ConsensusMetadataStore,
{
  let payload = encode_json_value(key, value)?;
  store.save_consensus_value(group, key, &payload)
}

pub(super) fn load_consensus_json_value<T, M>(
  store: &M,
  group: &str,
  key: &str,
) -> StoreResult<Option<T>>
where
  T: for<'de> Deserialize<'de>,
  M: ConsensusMetadataStore,
{
  let Some(payload) = store.load_consensus_value(group, key)? else {
    return Ok(None);
  };
  decode_json_value(key, &payload).map(Some)
}
