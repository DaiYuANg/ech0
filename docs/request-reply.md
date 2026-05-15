# Request Reply

ech0 supports request/reply over the broker for services that do not know each other's network address. The design is close to NATS-style request/reply, but it is implemented on top of ech0 topics and direct inboxes.

## Goals

- A requester can send a request to a subject without knowing responder addresses.
- Multiple responder replicas can share a subject through normal consumption.
- Replies return to the originating requester instance, not another replica of the requester service.
- The protocol exposes request/reply commands directly, so non-Go clients do not need to hand-write internal envelopes.
- The overall model remains asynchronous while keeping reply polling intervals low.

## Routing Model

Request flow:

1. A requester calls `StartRequest` or sends `CmdStartRequestRequest`.
2. The broker generates a correlation ID.
3. The request is published to the subject topic.
4. The request payload includes reply routing metadata.
5. A responder consumes from the subject through `FetchRequests`.
6. The responder calls `Reply` or `ReplyError`.
7. The reply is sent as a direct message to the requester instance's reply inbox.
8. The requester calls `AwaitReply`, which polls its reply inbox for the matching correlation ID.

The reply inbox name is derived from the requester instance ID:

```text
__reply/{instance_id}
```

Because the inbox contains the instance ID, a reply for `A1` is not consumed by `A2`.
Each pending request uses its own internal consumer cursor on that stable inbox, keyed by correlation ID. This allows the same requester instance to wait on multiple concurrent requests without creating a new reply inbox topic for every request.

## Replica Example

Assume:

- Service A has replicas `A1` and `A2`.
- Service B has replicas `B1`, `B2`, and `B3`.
- `A1` sends a request on subject `payments.authorize`.
- `B2` fetches and handles that request.

The request carries `reply_to = __reply/A1`. `B2` replies to that direct recipient with the correlation ID in the reply envelope. `A1` awaits the reply on its own direct inbox. `A2` is not polling that inbox and will not receive the reply.

## Timeouts

Each request has an expiration timestamp. Responders should avoid replying to expired requests; the broker also rejects replies when the request is already expired.

Requester wait behavior:

- `Timeout` defaults to five seconds.
- `PollInterval` defaults to five milliseconds.
- `AwaitReply` returns when a matching correlation ID is found or the context/deadline expires.

This keeps the transport asynchronous while allowing low-latency request/reply behavior for local or fast network deployments.

## Protocol Commands

Request/reply commands are first-class wire protocol commands:

| Command | Purpose |
| --- | --- |
| `CmdStartRequestRequest` / `CmdStartRequestResponse` | Publish a request and return pending request metadata. |
| `CmdFetchRequestsRequest` / `CmdFetchRequestsResponse` | Fetch request records from a subject. |
| `CmdReplyRequest` / `CmdReplyResponse` | Send a successful reply. |
| `CmdReplyErrorRequest` / `CmdReplyErrorResponse` | Send an error reply. |
| `CmdAwaitReplyRequest` / `CmdAwaitReplyResponse` | Wait for a reply by pending request metadata. |

The broker still uses internal request and reply payload encoders, but those are server-side details. Protocol clients interact with typed binary commands.

## Delivery Semantics

Requests are normal topic records and follow topic partitioning, offsets, and consumer behavior.

Replies are direct inbox messages:

- The direct inbox has its own offset.
- `AwaitReply` uses a per-correlation consumer cursor and acks that cursor after it reads the matching correlation ID.
- If the requester crashes before acking, the reply can remain in the inbox and be fetched again.

The current implementation is pull-based. It does not maintain long-lived push subscriptions or server-side response streams yet.
