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
