# Wire Protocol

ech0 uses a custom binary protocol over TCP. The goal is portability without code generation: non-Go clients can implement a small frame reader/writer and command-specific body codecs by hand.

## Transport Frame

Every TCP message is one frame:

| Offset | Size | Field | Description |
| --- | ---: | --- | --- |
| 0 | 4 | magic | `ECH0`, encoded as `0x45434830`. |
| 4 | 1 | version | Current protocol version, currently `1`. |
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

## Handshake And Capabilities

The first command a TCP client should send is `CmdHandshakeRequest`.

Handshake request body order:

| Field | Encoding |
| --- | --- |
| `client_id` | `string` |
| `tenant` | `string` |
| `namespace` | `string` |
| `principal` | `string` |
| `auth_token` | `string` |
| `capabilities` | `u32` count followed by capability strings |

Handshake response body order:

| Field | Encoding |
| --- | --- |
| `server_id` | `string` |
| `protocol_version` | `u8` |
| `tenant` | `string` |
| `namespace` | `string` |
| `principal` | `string` |
| `capabilities` | `u32` count followed by negotiated capability strings |

If the request sends an empty capability list, the server returns every capability supported by this protocol version. If the request sends one or more capability strings, the server returns the supported intersection in server-preferred order and ignores unknown capabilities.

Current capability strings:

| Capability | Meaning |
| --- | --- |
| `compression.zstd` | zstd payload or batch compression support. |
| `produce.batch` | Single-topic batch produce command support. |
| `produce.batches` | Multi-topic/multi-partition batch produce command support. |
| `produce.fanout` | Fanout produce command support across all partitions of a topic. |
| `fetch.batch` | Batch fetch command support. |
| `fetch.wait` | Fetch requests can use min-records and max-wait fields. |
| `transactions` | Transaction begin, publish, offset commit, commit, and abort commands. |
| `idempotent.produce` | Producer ID, epoch, and sequence based dedupe. |
| `direct` | Direct inbox send, fetch, and ack commands. |
| `request.reply` | Request/reply command set. |
| `request.reply.many` | Multi-replier request/reply collection commands. |
| `consumer.groups` | Consumer group membership, rebalance, fetch, and commit commands. |
| `retry.delay` | Nack, retry processing, and delayed schedule commands. |
| `routing.key` | Routing-key based partition selection. |
| `topic.ordering` | Topic-level key or routing-key ordering policies on create-topic. |
| `subject.wildcards` | Wildcard subject fetch support. |
| `schema.headers` | Schema hints are carried through message headers such as `content-type`, `schema-id`, and `encoding`. |

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
- `50-55`: request/reply.
- `60-66`: consumer group membership, assignment, fetch, and commit.
- `70-75`: transactions.
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

The current protocol is versioned by `protocol.Version`. The project has not shipped a stable public release yet, so the custom binary contract remains protocol version `1` and incompatible protocol changes are folded into that version.

Future changes should follow these rules:

- Add new commands when that keeps the command surface clearer.
- Field order may still change before the first stable release when it makes the client contract simpler.
- Keep `protocol.Version` at `1` until there is a released compatibility promise.
- Keep `HeaderLen` stable unless transport framing itself changes.
- Update `protocol.CommandIDs()` tests when adding commands.

## Client Implementation Notes

A non-Go client needs only:

1. A 28-byte frame header encoder/decoder.
2. A body codec for the commands it uses.
3. Request ID generation.
4. Error frame handling.
5. Capability negotiation during handshake.

Clients do not need to hand-write internal broker envelopes for request/reply. Request/reply is exposed as protocol commands.
