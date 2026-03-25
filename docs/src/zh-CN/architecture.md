# 系统架构

[English version](../en/architecture.md)

从分层视角看，`ech0` 可分为四层：

1. 接入层：TCP 连接、请求解码、响应编码
2. 业务层：`BrokerService` 聚合 queue/direct/group 等能力
3. 运行时层：`QueueRuntime` / `DirectRuntime`
4. 存储层：`SegmentLog` + `RedbMetadataStore`

## 关键装配点

`broker/src/main.rs` 完成系统启动装配：

- 加载配置并初始化日志/指标
- 打开分段日志存储与元数据存储
- 根据配置决定 `Standalone` 或 `Raft` 运行模式
- 创建 `BrokerService`
- 启动 TCP 服务与 Admin HTTP 服务
- 启动后台任务（例如日志保留清理）

## 运行时组合

`BrokerService` 并不是“单一队列服务”，而是组合了两条业务 runtime：

- `queue` 路径：topic/partition 发布与拉取、offset 提交、消费组分配
- `direct` 路径：发送私信、拉取收件箱、ack 收件箱消息

同时，`BrokerService` 内部使用 mode-aware 包装，在不同模式下复用同一套业务 API：

- standalone：本地存储直接读写
- raft：写操作通过一致性路径提交，读操作按策略约束（local/leader/linearizable）
