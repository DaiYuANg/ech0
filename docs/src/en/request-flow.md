# Core Request Flow

[中文版本](../zh-CN/request-flow.md)

For queue publish/fetch, the request pipeline is:

1. client sends a protocol frame over TCP
2. `transport` decodes it into command + payload
3. `broker` handlers call `BrokerService`
4. `BrokerService` dispatches to `QueueRuntime`
5. `QueueRuntime` uses `store` traits for persistence/access
6. result is encoded by `transport` and sent back

## Publish Path

- entry: `BrokerService::publish`
- core: `QueueRuntime::publish`
- persistence: `SegmentLog` appends record and advances partition offset

## Fetch Path

- entry: `BrokerService::fetch`
- pre-check: read access policy validated by runtime mode
- core: `QueueRuntime::fetch`
- storage read: scan segment data by topic/partition + offset

## Direct Path

Direct and queue are distinct at API level, but share storage abstractions:

- `send_direct`: write to recipient inbox partition
- `fetch_inbox`: read inbox records
- `ack_direct`: commit inbox consumption progress
