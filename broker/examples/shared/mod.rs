use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use protocol::{CMD_ERROR_RESPONSE, ErrorResponse, VERSION_1, decode_json, encode_json};
use tokio::net::TcpStream;
use transport::{Frame, read_frame, write_frame};

pub async fn request_json<TReq, TResp>(
  stream: &mut TcpStream,
  request_command: u16,
  response_command: u16,
  payload: &TReq,
) -> io::Result<TResp>
where
  TReq: serde::Serialize,
  TResp: for<'de> serde::Deserialize<'de>,
{
  let body = encode_json(payload)
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
  let frame = Frame::new(VERSION_1, request_command, body)?;
  write_frame(stream, &frame).await?;
  let response = read_frame(stream).await?;

  if response.header.command == CMD_ERROR_RESPONSE {
    let error: ErrorResponse = decode_json(&response.body)
      .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    return Err(io::Error::other(format!(
      "broker error: code={} message={}",
      error.code, error.message
    )));
  }

  if response.header.command != response_command {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      format!(
        "unexpected response command {}, expected {}",
        response.header.command, response_command
      ),
    ));
  }

  decode_json(&response.body)
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
}

pub fn unix_timestamp_ms() -> u128 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|duration| duration.as_millis())
    .unwrap_or(0)
}
