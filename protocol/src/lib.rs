#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientCommand {
  CreateTopic {
    topic: String,
  },
  Publish {
    topic: String,
    payload: Vec<u8>,
  },
  Poll {
    consumer: String,
    topic: String,
    max_records: usize,
  },
  Ack {
    consumer: String,
    topic: String,
    next_offset: u64,
  },
  Ping,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerResponse {
  Ok,
  Records {
    offsets: Vec<u64>,
    payloads: Vec<Vec<u8>>,
  },
  Error {
    message: String,
  },
  Pong,
}
