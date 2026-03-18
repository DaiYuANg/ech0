use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const HEADER_LEN: usize = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
  pub version: u8,
  pub command: u16,
  pub body_len: u32,
}

impl FrameHeader {
  pub fn new(version: u8, command: u16, body_len: u32) -> Self {
    Self {
      version,
      command,
      body_len,
    }
  }

  pub fn encode(self) -> [u8; HEADER_LEN] {
    let mut bytes = [0_u8; HEADER_LEN];
    bytes[0] = self.version;
    bytes[1..3].copy_from_slice(&self.command.to_be_bytes());
    bytes[3..7].copy_from_slice(&self.body_len.to_be_bytes());
    bytes
  }

  pub fn decode(bytes: [u8; HEADER_LEN]) -> Self {
    Self {
      version: bytes[0],
      command: u16::from_be_bytes([bytes[1], bytes[2]]),
      body_len: u32::from_be_bytes([bytes[3], bytes[4], bytes[5], bytes[6]]),
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
  pub header: FrameHeader,
  pub body: Vec<u8>,
}

impl Frame {
  pub fn new(version: u8, command: u16, body: Vec<u8>) -> io::Result<Self> {
    let body_len = u32::try_from(body.len())
      .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame body too large"))?;
    Ok(Self {
      header: FrameHeader::new(version, command, body_len),
      body,
    })
  }
}

pub async fn read_frame<R>(reader: &mut R) -> io::Result<Frame>
where
  R: AsyncRead + Unpin,
{
  let mut header_buf = [0_u8; HEADER_LEN];
  reader.read_exact(&mut header_buf).await?;
  let header = FrameHeader::decode(header_buf);

  let mut body = vec![0_u8; header.body_len as usize];
  reader.read_exact(&mut body).await?;

  Ok(Frame { header, body })
}

pub async fn write_frame<W>(writer: &mut W, frame: &Frame) -> io::Result<()>
where
  W: AsyncWrite + Unpin,
{
  writer.write_all(&frame.header.encode()).await?;
  if !frame.body.is_empty() {
    writer.write_all(&frame.body).await?;
  }
  writer.flush().await
}

#[cfg(test)]
mod tests {
  use super::{Frame, FrameHeader, HEADER_LEN};

  #[test]
  fn header_roundtrip() {
    let header = FrameHeader::new(1, 42, 1024);
    let encoded = header.encode();
    assert_eq!(encoded.len(), HEADER_LEN);
    assert_eq!(FrameHeader::decode(encoded), header);
  }

  #[test]
  fn frame_new_sets_header_length() {
    let frame = Frame::new(1, 7, vec![1, 2, 3]).unwrap();
    assert_eq!(frame.header.body_len, 3);
  }
}
