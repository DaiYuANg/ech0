use std::{fmt::Debug, marker::PhantomData};

use serde::{Serialize, de::DeserializeOwned};

use crate::{Result, StoreError};

pub trait Codec<T>: Send + Sync + Debug {
  fn encode(&self, value: &T) -> Result<Vec<u8>>;
  fn decode(&self, bytes: &[u8]) -> Result<T>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct JsonCodec<T> {
  _marker: PhantomData<T>,
}

impl<T> JsonCodec<T> {
  pub fn new() -> Self {
    Self {
      _marker: PhantomData,
    }
  }
}

impl<T> Codec<T> for JsonCodec<T>
where
  T: Serialize + DeserializeOwned + Send + Sync + Debug,
{
  fn encode(&self, value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|err| StoreError::Codec(format!("json encode failed: {err}")))
  }

  fn decode(&self, bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes)
      .map_err(|err| StoreError::Codec(format!("json decode failed: {err}")))
  }
}
