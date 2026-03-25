# 核心请求链路

[English version](./en/request-flow.md)

以 queue 的发布与拉取为例，请求链路可以抽象为：

1. 客户端通过 TCP 发送协议帧
2. `transport` 解码为命令 + payload
3. `broker` handler 调用 `BrokerService`
4. `BrokerService` 分发到 `QueueRuntime`
5. `QueueRuntime` 调用 `store` trait 完成落盘/读取
6. 结果经 `transport` 编码后返回客户端

## Publish 路径

- 入口：`BrokerService::publish`
- 核心：`QueueRuntime::publish`
- 落地：`SegmentLog` 追加 record，更新分区 next offset

## Fetch 路径

- 入口：`BrokerService::fetch`
- 读前校验：按运行模式执行读策略约束
- 核心：`QueueRuntime::fetch`
- 读取：根据 topic/partition + offset 从段文件扫描并返回 records

## Direct 路径

direct 与 queue 在对外命令上独立，但底层仍复用存储抽象：

- `send_direct`：写入接收者收件箱分区
- `fetch_inbox`：读取收件箱数据
- `ack_direct`：提交收件箱消费进度

这使系统可以在同一存储基础上同时支持“主题广播式消费”和“点对点收件箱”两类模型。
