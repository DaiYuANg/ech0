# Wire Protocol

ech0 uses a custom binary protocol over TCP. The goal is portability without code generation: non-Go clients can implement a small frame reader/writer and command-specific body codecs by hand.

## Transport Frame

Every TCP message is one frame:

| Offset | Size | Field | Description |
| --- | ---: | --- | --- |
| 0 | 4 | magic | `ECH0`, encoded as `0x45434830`. |
| 4 | 1 | version | Current protocol version. |
| 5 | 1 | header_len | Currently `28`. |
| 6 | 2 | flags | Reserved for future frame flags. |
| 8 | 2 | command | Command ID from `protocol`. |
| 10 | 2 | status | `0` for OK, `1` for error. |
| 12 | 8 | request_id | Correlates a response frame to a request frame. |
| 20 | 4 | body_len | Body byte length. |
| 24 | 4 | reserved | Reserved for future use. |

All numeric fields are big-endian. The frame header is implemented in `transport.FrameHeader`; frame IO lives in `transport.ReadFrameWithLimit` and `transport.WriteFrame`.

## Body Encoding

The frame body is command-specific. There is no JSON, MessagePack, Protobuf, or generated schema layer in the TCP protocol.

Primitive encoding:

| Type | Encoding |
| --- | --- |
| `u8` | 1 byte. |
| `u16` | 2 bytes, big-endian. |
| `u32` | 4 bytes, big-endian. |
| `u64` | 8 bytes, big-endian. |
| `bool` | `0` or `1`. |
| `string` | `u16` length followed by UTF-8 bytes. |
| `bytes` | `u32` length followed by raw bytes. |
| optional value | `bool` presence flag followed by the value when present. |
| repeated value | `u16` or `u32` count followed by each value, depending on the field. |

Message headers are repeated `{ key string, value bytes }` pairs.

## Codec Registry

`protocol.EncodeBody` and `protocol.DecodeBody` dispatch through a collectionx-backed registry:

- Commands are defined in `protocol/protocol.go`.
- Codec entries are listed in `protocol/binary_registry_entries.go`.
- Encoders and decoders are stored in `collectionx/mapping`.
- The command list is stored and returned through `collectionx/list`.

Repeated fields are decoded into `collectionx/list` before returning ordinary slices to public message structs. The byte cursor itself remains a low-level reader/writer because it is protocol state, not a collection abstraction.

## Command Groups

Command IDs are grouped by feature:

- `1-2`: handshake and ping.
- `10-32`: topics, produce/fetch, offsets, nack, retry, and delay.
- `40-42`: direct inbox messaging.
- `50-54`: request/reply.
- `60-66`: consumer group membership, assignment, fetch, and commit.
- `1001+`: responses.
- `1500`: error response.

The response command usually mirrors the request command by adding `1000`.

## Error Responses

When a server-side handler returns an application error, the server writes an error frame with:

- `StatusError`.
- `CmdErrorResponse`.
- A binary `ErrorResponse` body containing a code and message.
- The same `request_id` as the request frame.

Transport-level failures such as invalid magic, unsupported header length, or oversized bodies fail before command dispatch.

## Compatibility Rules

The current protocol is versioned by `protocol.Version`. Since compatibility with older pre-binary formats is not required, version `2` defines the custom binary contract.

Future changes should follow these rules:

- Add new commands instead of changing existing command field order.
- Append optional fields only when older clients can safely stop reading at the old body length.
- Bump `protocol.Version` for incompatible body changes.
- Keep `HeaderLen` stable unless transport framing itself changes.
- Update `protocol.CommandIDs()` tests when adding commands.

## Client Implementation Notes

A non-Go client needs only:

1. A 28-byte frame header encoder/decoder.
2. A body codec for the commands it uses.
3. Request ID generation.
4. Error frame handling.

Clients do not need to hand-write internal broker envelopes for request/reply. Request/reply is exposed as protocol commands.

