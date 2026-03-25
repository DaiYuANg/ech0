# 总览

[English version](../en/overview.md)

`ech0` 是一个基于 Rust 的 TCP 消息代理系统，目标是提供两种消息路径：

- queue（主题/分区 + offset 消费）
- direct（点对点收件箱）

工程采用 Cargo workspace，核心 crate 如下：

- `broker`：进程入口、网络服务、管理面、运行时装配
- `store`：日志分段存储、元数据、状态机与抽象 trait
- `protocol`：命令号与请求/响应结构
- `transport`：帧协议编解码
- `queue`：队列投递与消费运行时
- `direct`：直连消息运行时

默认情况下，`broker` 同时暴露：

- TCP 服务端口（业务协议）
- Admin HTTP 端口（健康检查、指标、UI、API）

本书重点描述系统的结构设计与关键数据流，不覆盖每个命令的接口细节。
# 简体中文
