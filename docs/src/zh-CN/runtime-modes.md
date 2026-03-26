# 运行模式与一致性

[English version](../en/runtime-modes.md)

`ech0` 支持两类运行模式：

- `Standalone`：单节点本地读写，路径最短，适合本地开发和轻量部署
- `Raft`：通过一致性复制保障多节点状态收敛

## Mode-Aware 设计

业务层通过 mode-aware store 进行统一访问：

- 对业务 API 透明，不同模式复用同一接口
- 写路径可切换为本地直接写或一致性提交
- 读路径由读策略约束

## 读策略

Raft 模式下支持不同读语义：

- `Local`：允许本地读取，延迟最低
- `Leader`：要求在 leader 节点读取
- `Linearizable`：在 leader 基础上做线性一致性保障

策略越强，一致性越高，通常也意味着更高读取成本。

## 消费组分配

消费组支持：

- `round_robin` 与 `range` 两种分配策略
- 可选 sticky 行为，尽量复用历史 owner，减少迁移
- 记录重平衡指标，便于观察分配抖动与迁移量

## Offset / Watermark 语义规范

为避免后续复制、主从切换和读策略演进时语义漂移，`ech0` 统一采用以下定义。

### 1) `offset`

- `offset` 是 **record 在某个 `topic-partition` 内的唯一序号**（从 `0` 开始，单调递增，不复用）
- `offset` 是“这条记录本身的 ID”，不是“下一条要读的位置”

### 2) Fetch 请求里的 `offset`

- Fetch 请求参数 `offset` 定义为 **起始读取位置（inclusive）**
- 也就是“从这个 offset 开始尝试返回可见记录”
- 因此它在语义上等价于“consumer 当前的 next offset”

### 3) Fetch 响应里的 `next_offset`

- `next_offset` 定义为 **客户端下一次应当传入的 offset**
- 计算规则：
  - 若返回了记录：`next_offset = last_returned_record.offset + 1`
  - 若未返回记录：`next_offset = request.offset`
- 客户端/消费组提交位点时，提交的是 `next_offset`，而不是“最后消费到的 offset”

### 4) `high_watermark`

- `high_watermark` 定义为 **当前可见且已提交（committed）的最大 record offset（inclusive）**
- 这是“读上界”，表示最多可以稳定读取到哪里
- 当没有任何可见记录时，可为 `None`

### 5) Standalone 与 Raft 的一致性约束

两种模式必须保持同一 API 语义，只允许“内部来源”不同：

- `Standalone`：`committed == appended`，因此 `high_watermark == last_appended_offset`
- `Raft`：`high_watermark` 由复制提交进度决定（已提交并可见），不是简单的本地追加末尾

换言之：

- **对客户端而言，`offset/next_offset/high_watermark` 语义完全一致**
- **对实现而言，差异只在 high watermark 的计算来源**

### 6) 为什么这是 P0

如果不先固定这套语义，后续会出现典型问题：

- ACK 提交时“提交最后一条”还是“提交下一条”混用，导致重复消费或跳读
- leader/follower 读到不同上界，排障时无法判断是复制延迟还是位点错误
- 批量 fetch、重平衡、重试/延迟队列难以复用统一游标逻辑

因此本规范作为后续复制、读策略和客户端 SDK 的统一基线。
