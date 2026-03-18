#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
  pub version: u8,
  pub command: u16,
  pub body_len: u32,
}
