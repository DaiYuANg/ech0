# Workspace 与模块职责

[English version](../en/modules.md)

## `broker`

`broker` 是应用入口，负责：

- 生命周期管理（启动、运行、关闭）
- TCP 请求处理与转发到 `BrokerService`
- Admin HTTP（健康检查、指标、管理 API、UI）
- 运行模式适配（standalone/raft）

它依赖其余 crate，并把它们装配成一个可运行节点。

## `protocol`

定义协议命令号与消息体结构，确保客户端与服务端对请求语义保持一致。

## `transport`

实现帧协议读写逻辑（头 + body），将网络字节流转换为业务可处理帧。

## `store`

这是系统“状态根”，主要包含：

- `segment`：按 topic/partition 管理追加日志段与索引
- `meta`：元数据持久化（topic 配置、offset、组成员与分配等）
- `state_machine` / `command`：用于局部分区命令与应用状态推进
- `traits`：定义存储抽象边界，供上层 runtime 解耦

## `queue`

基于 topic/partition 的队列语义：

- 创建主题与分区配置
- 追加消息并维护 offset
- 按 offset 拉取消息
- 提交消费进度

## `direct`

点对点消息语义：

- 发送 direct 消息到接收者收件箱
- 拉取收件箱消息
- ack 消息推进收件箱 offset
