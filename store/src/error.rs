use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
  #[error("topic already exists: {0}")]
  TopicAlreadyExists(String),
  #[error("topic not found: {0}")]
  TopicNotFound(String),
  #[error("partition {partition} not found for topic {topic}")]
  PartitionNotFound { topic: String, partition: u32 },
  #[error("invalid offset {offset} for topic {topic} partition {partition}")]
  InvalidOffset {
    topic: String,
    partition: u32,
    offset: u64,
  },
  #[error("topic {topic} is unavailable: {reason}")]
  TopicUnavailable { topic: String, reason: String },
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
  #[error("redb error: {0}")]
  Redb(#[from] redb::Error),
  #[error("transaction error: {0}")]
  RedbTransaction(#[from] redb::TransactionError),
  #[error("database error: {0}")]
  RedbDatabase(#[from] redb::DatabaseError),
  #[error("table error: {0}")]
  RedbTable(#[from] redb::TableError),
  #[error("storage error: {0}")]
  RedbStorage(#[from] redb::StorageError),
  #[error("commit error: {0}")]
  RedbCommit(#[from] redb::CommitError),
  #[error("storage corruption: {0}")]
  Corruption(String),
  #[error("codec error: {0}")]
  Codec(String),
  #[error("unsupported operation: {0}")]
  Unsupported(&'static str),
}

pub type Result<T> = std::result::Result<T, StoreError>;
