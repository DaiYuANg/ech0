# Broker 设计与实现

[English version](../en/broker-design.md)

本文描述当前代码状态下 `broker` crate 的设计与实现。它不是面向外部协议的 API 手册，而是面向维护者的实现说明；当 broker 的启动流程、运行模式、后台任务或管理面发生变化时，这份文档也应同步更新。

## 1. Broker 在 workspace 中的位置

`ech0` 的 workspace 里，`broker` 是把协议、运行时、存储和管理面装配成一个进程的那一层。

- `protocol` 定义命令号和请求/响应结构。
- `transport` 负责帧协议编解码。
- `queue` 和 `direct` 提供两条业务 runtime。
- `store` 提供消息日志、元数据、状态机和一致性命令执行能力。
- `broker` 把这些组件连成一个可运行的 TCP broker，并额外提供 Admin HTTP、后台 worker 和可观测性。

因此，`broker` 本身更像“进程运行时与编排层”，而不是单一业务模块。

## 2. 启动与进程装配

主入口在 `broker/src/lib.rs`：

1. 加载 `AppConfig`
2. 初始化日志和 Prometheus 指标
3. 打开 `SegmentLog` 与 `RedbMetadataStore`
4. 在 standalone 模式下确保 bootstrap topic 存在，并校验 catalog / log 的 topic 一致性
5. 根据配置决定 `BrokerRuntimeMode::Standalone` 或 `BrokerRuntimeMode::Raft`
6. 构造 `BrokerService`
7. 通过 `di` 模块生成 TCP server 和 Admin HTTP wiring
8. 启动后台 worker
9. 进入 lifecycle，等待 shutdown future

这里有几个当前实现上的关键点：

- `SegmentLogOptions` 会接收 `storage.compaction_sealed_segment_batch`，用于限制单轮 compaction 的 sealed segment 批次。
- broker 启动时会把 broker state 写入 metadata store。
- `BootstrapModule` 除了初始化 observability，还会把协议限制和后台 worker 配置同步到运行态。

## 3. 网络入口与命令分发

TCP 入口在 `broker/src/server`，真正的命令分发在 `broker/src/server/handler.rs`。

当前 handler 的职责很明确：

- 校验 protocol version
- 记录 command 指标
- 做 frame 级和 request 级保护
- 把请求转换为 `BrokerService` 调用
- 把 service 结果映射回 wire response

当前已经实现的主要命令族包括：

- queue：`create_topic`、`produce`、`produce_batch`、`fetch`、`fetch_batch`、`commit_offset`
- direct：`send_direct`、`fetch_inbox`、`ack_direct`
- retry / delay：`nack`、`process_retry`、`schedule_delay`
- group：`join_consumer_group`、`heartbeat_consumer_group`、`rebalance_consumer_group`、`get_consumer_group_assignment`
- 基础：`handshake`、`ping`、`list_topics`

handler 还承担一层关键保护：

- `max_payload_bytes`
- `max_batch_payload_bytes`
- `max_fetch_records`

这些限制来自 `BootstrapModule::configure_runtime_limits()`，而不是散落在各个命令实现里。

## 4. BrokerService：统一业务门面

`BrokerService` 是 broker 的核心业务门面，定义在 `broker/src/service.rs`。

它当前聚合了几类能力：

- topic 创建与 topic policy 校验
- queue publish / fetch / commit
- direct inbox send / fetch / ack
- retry / DLQ / delay 调度相关能力
- consumer group 成员管理与 rebalance
- runtime health 与 read policy 约束

从结构上看，`BrokerService` 不是直接操作裸存储，而是持有：

- `QueueRuntime`
- `DirectRuntime`
- mode-aware log / metadata store 包装
- 可选的 `BrokerRaftRuntime`

这种设计的目的，是让上层业务 API 在 standalone 和 raft 两种模式下保持一致。

## 5. Mode-Aware 读写路径

当前 broker 的一个核心设计是“业务层不关心底层是否走本地写还是 Raft 写”。

### 5.1 写路径

写路径通过 `broker/src/service/mode/appenders.rs` 抽象成 `PartitionAppender`：

- `StandalonePartitionAppender`
- `RaftPartitionAppender`

standalone 下直接写本地日志和本地元数据。

raft 下会把写请求封装为一致性命令，通过 `BrokerRaftRuntime` 提交到状态机，再由状态机应用到 `store`。

当前实现还做了一个重要优化：

- `ApplyResult::Appended` 直接返回追加出来的 records
- broker 不需要在 apply 成功后再做一次 read-back

这让 standalone 和 raft 的 append / batch append 路径都收敛到了同一种结果消费方式。

### 5.2 读路径

读路径由 `BrokerService::ensure_read_allowed()` 统一约束。

Raft 模式下支持三种读策略：

- `Local`
- `Leader`
- `Linearizable`

其中：

- `Leader` 要求当前节点就是 leader
- `Linearizable` 在 leader 基础上再做线性一致性校验

这套限制只作用于读；对外 API 语义不变，只改变允许读取的条件。

## 6. Topic 与消息模型

当前 broker 支持的消息模型不止是“普通 topic + payload”。

### 6.1 Queue

queue 路径采用 topic / partition / offset 模型。

- `offset` 是 record ID
- fetch 请求里的 `offset` 是 inclusive 起始位置
- 响应里的 `next_offset` 是下一次应继续读取的位置
- `high_watermark` 表示当前可稳定读取的最大 offset

### 6.2 Direct

direct 路径本质上仍复用底层存储，但对外暴露为收件箱模型。

- 每个 recipient 对应隐藏 inbox topic
- `send_direct` 写入收件箱
- `fetch_inbox` 读取收件箱
- `ack_direct` 提交收件箱消费进度

### 6.3 内部 topic

broker 当前会生成并依赖几类内部 topic：

- direct inbox topic
- retry topic
- delay topic

对用户侧 `create_topic`，这些名字属于保留名称，service 会显式拒绝。

### 6.4 Keyed record 与 tombstone

当前实现已经支持：

- record `key`
- tombstone 语义
- keyed batch produce
- fetch / fetch_batch 回传 key

这为 compact topic 提供了基础。

### 6.5 Topic policy

当前 topic policy 校验已经集中在 `BrokerService::create_topic_with_policies()` 和相关 helper 中，主要包括：

- `max_message_bytes <= max_batch_bytes`
- retry backoff 参数合法
- internal topic 名称禁止用户创建
- `compaction_tombstone_retention_ms` 需要 `compaction_enabled`
- `cleanup_policy` 与 `compaction_enabled` 必须一致
- dead-letter topic 名称合法且不能和源 topic 相同

## 7. 存储维护与后台 worker

当前 broker 进程内有四类后台任务：

- retention cleanup
- compaction cleanup
- delay scheduler
- retry worker

### 7.1 Retention cleanup

retention cleanup 周期性调用 `SegmentLog::enforce_retention_once()`。

它当前负责：

- 基于 retention policy 删除旧 segment
- 控制磁盘增长
- 上报最近运行状态和累计删除 segment 指标

### 7.2 Compaction cleanup

compaction cleanup 周期性调用 `SegmentLog::compact_once()`。

当前实现特征：

- 只处理启用 compaction 的 topic
- 保留 keyed record 的最新版本
- 支持 tombstone retention window
- compact-only topic 不会走 delete retention 清理
- 单轮只 compact 一个 sealed prefix，而不是整分区全量重写
- `compaction_sealed_segment_batch` 控制单轮最多处理多少 sealed segments

当前还存在一个明确边界：

- active segment 不会被就地 compact
- 某些 tombstone 的物理清理要等 active segment seal 之后才发生

### 7.3 Delay scheduler 与 retry worker

- delay scheduler 负责把到期的 delayed records 转回原 topic
- retry worker 负责扫描 retry topics，把记录重投回 origin topic 或送入 DLQ

这两个 worker 仍然属于 broker 进程的一部分，而不是独立服务。

## 8. Admin、健康检查与可观测性

broker 当前同时提供 Admin HTTP：

- `/healthz`
- `/metrics`
- UI 页面
- group 管理 API

### 8.1 当前管理面结构

当前 `admin` 模块已按职责拆分为：

- `admin/health.rs`
- `admin/dashboard.rs`
- `admin/topics.rs`
- `admin/groups.rs`
- `admin/api.rs`
- `admin/models.rs`

这样做的原因很直接：管理面的实现已经不再是“一个简单 handler 文件”，而是完整的子系统。

### 8.2 Health 语义

当前 `/healthz` 不只报告 runtime mode / raft leader，还会把后台 worker 的 readiness 放进去。

已落地的运行态包括：

- retention cleanup 最近一次 started / finished / success / error
- compaction cleanup 最近一次 started / finished / success / error
- 后台 worker 的 readiness

当前 readiness 判定采用“最近成功时间是否超过 `3 x interval`”的规则；如果 worker 长时间没有成功运行，broker health 会从 `ok` 变成 `degraded`。

### 8.3 Metrics

当前 broker 暴露的指标至少包括：

- TCP 连接数
- command 总量与错误总量
- rebalance 指标
- raft client write retry / failure
- retention cleanup runs / removed segments
- compaction cleanup runs / compacted partitions / removed records

这些指标同时被 dashboard 和 Prometheus 采集接口复用。

## 9. 当前实现边界

为了维护预期，下面这些点应当被视为“当前实现的边界”，而不是未来承诺：

- admin 目前是单节点视图优先，不是完整 cluster control plane
- compaction 已可用，但还是基于 segment prefix 的重建式实现，不是完全增量的后台压实引擎
- Raft 读写路径已经打通，但 cluster 运维和治理能力还比较初步
- direct、retry、delay、DLQ 都复用同一套存储基础，这提升了一致性，但也意味着后台 worker 的健康度会直接影响 broker 整体状态

## 10. 维护这份文档时优先同步的文件

如果 broker 发生较大变化，优先检查这些文件并更新本文：

- `broker/src/lib.rs`
- `broker/src/server/handler.rs`
- `broker/src/service.rs`
- `broker/src/service/mode/appenders.rs`
- `broker/src/di/bootstrap_module.rs`
- `broker/src/admin/health.rs`
- `broker/src/admin/*.rs`
- `store/src/segment/partition.rs`
- `store/src/command/*`
- `broker/src/raft/*`

这份文档的目标不是穷尽所有细节，而是保证维护者能快速回答三个问题：

1. broker 进程是怎么被装起来的？
2. 一条请求在当前实现里是怎么走到存储层的？
3. 当前版本已经支持什么，以及还明确不支持什么？

## 11. Durable Stream Core 当前状态

当前 broker 已经不只是“提供原始日志读写”的阶段，durable stream 这一层已经开始收敛成明确的数据面语义。

### 11.1 Consumer Group 数据面

consumer group 不再只是控制面元数据。

当前 broker 已经提供显式的 group-aware fetch / commit 命令，请求里会带上：

- `group`
- `member_id`
- `generation`

这些请求在返回数据或推进 offset 之前，会先按当前 assignment snapshot 做校验。

当前行为包括：

- rebalance 之后会对 stale member 做 fencing
- member 没有拥有目标 partition 时，fetch / commit 会被拒绝
- 消费进度按 group 级 cursor 跟踪，而不是绑在单个 member 身上

这意味着 consumer group 现在已经开始真正约束消费数据面，而不只是保存 rebalance 结果。

### 11.2 Producer 分区选择

producer 现在已经支持 broker-side partitioning，而不再只依赖客户端显式指定 partition。

当前支持三种模式：

- `explicit`
- `round_robin`
- `key_hash`

当前规则是：

- `explicit` 必须显式给出 partition
- `round_robin` 由 broker 在 topic 内做轮转选择
- `key_hash` 需要 keyed record，并保证同 key 稳定落到同一个 partition
- batch 在 `key_hash` 模式下要求整批 records 使用一致且非空的 key

这一点已经作为破坏性协议更新落地，应当视为当前 producer 行为基线。

### 11.3 Fetch 等待语义

fetch 已经不再只是短轮询接口。

当前 fetch 家族支持：

- `max_wait_ms`
- `min_records`

覆盖的命令包括：

- `fetch`
- `fetch_batch`
- `fetch_consumer_group`
- `fetch_consumer_group_batch`

当前行为是：

- 如果 `max_wait_ms` 缺省或为 `0`，broker 立即返回
- 否则 broker 会持续轮询，直到拿到足够记录数或超时
- `min_records` 默认是 `1`
- batch fetch 按整批返回结果中的总记录数判断是否满足阈值

这让当前实现已经具备一版基础的 Kafka 风格 long polling 数据路径。

### 11.4 Stream 可观测性

broker 现在已经暴露出比早期更多的 durable stream 运行态。

当前覆盖包括：

- topic backlog snapshot
- consumer group lag snapshot
- 通过 `/metrics` 刷新的低基数 stream 聚合指标
- producer partitioning 总量和热点 partition 摘要

因此当前 dashboard 和 admin API 已经不只是节点健康视图，也开始承担 stream 压力和消费进度的观测职责。

### 11.5 当前 Stream 边界

即使做完这些增强，当前 durable stream core 仍然有明确边界：

- fetch 等待语义目前基于 `min_records`，还不是 `min_bytes`
- 还没有 transactional producer 模型
- 还没有 producer idempotence 状态
- admin 视角仍然以单节点运维为主
- work queue 这类投递语义仍属于后续工作，不是当前 stream core 的一部分
