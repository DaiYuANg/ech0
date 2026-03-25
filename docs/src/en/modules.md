# Workspace and Module Responsibilities

[中文版本](../zh-CN/modules.md)

## `broker`

`broker` is the application entrypoint, responsible for:

- lifecycle orchestration (startup/run/shutdown)
- TCP request handling and dispatch to `BrokerService`
- Admin HTTP endpoints (health, metrics, management APIs, UI)
- runtime mode adaptation (standalone/raft)

## `protocol`

Defines command IDs and payload structures shared by clients and server.

## `transport`

Implements frame-level IO to convert TCP byte streams into business messages.

## `store`

`store` is the persistent state backbone, including:

- `segment`: append log segments and indexes by topic/partition
- `meta`: metadata persistence (topics, offsets, members, assignments)
- `state_machine` and `command`: local partition command application model
- `traits`: storage interfaces used by upper runtimes

## `queue`

Implements topic/partition queue semantics:

- create topics and partition configs
- append records and advance offsets
- fetch records by offsets
- commit consumer progress

## `direct`

Implements point-to-point messaging:

- send direct messages to recipient inboxes
- fetch inbox messages
- ack inbox progress
