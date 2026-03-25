# 运维与可观测性

[English version](../en/operations.md)

## 服务端口

默认提供两个入口：

- broker TCP：业务协议请求
- admin HTTP：健康、指标、UI、管理 API

## 配置加载

配置按层叠顺序加载（后者覆盖前者）：

1. `./ech0.toml`
2. `./config/ech0.toml`
3. `./config/ech0.local.toml`
4. `ECH0_` 前缀环境变量

环境变量使用 `__` 表示嵌套字段，便于容器化部署覆盖局部配置。

## 运行保护

系统提供多类防护：

- 最大帧体积限制，防止异常大包
- 最大 payload 限制，约束单条消息大小
- fetch 最大记录数限制，防止单请求过大
- retention 清理控制磁盘增长

## 日常命令

- 构建：`cargo build --workspace`
- 测试：`cargo test --workspace`
- 检查：`cargo fmt --all -- --check`
- lint：`cargo clippy --workspace --all-targets`

## 文档命令（mdBook）

- 本地预览：`mdbook serve docs`
- 生成静态文档：`mdbook build docs`

生成结果在 `docs/book` 目录，可直接静态托管。
