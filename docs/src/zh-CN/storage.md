# 存储设计

[English version](../en/storage.md)

## 段日志（SegmentLog）

`SegmentLog` 负责消息主体数据，采用“分区 + 多段文件”模型：

- 每个 topic 的每个 partition 对应一组 segment 文件
- append-only 写入，按 `segment_max_bytes` 滚动新段
- 配套 index 文件支持更高效定位
- 通过 checkpoint 记录 next offset

这种设计兼顾了顺序写吞吐与分区隔离。

## 元数据（RedbMetadataStore）

元数据与消息主体分离，主要保存：

- topic 配置
- consumer offset
- consumer group 成员与分配结果
- broker 状态与一致性相关元数据

元数据由 `redb` 持久化，便于原子更新与快速恢复。

## 保留策略（Retention）

系统支持按 partition 执行基于字节上限的保留清理：

- 当累计段大小超过 `retention_max_bytes`
- 删除最旧 segment（至少保留一个活动段）
- 保证磁盘占用可控

## 截断（Truncate）

在特定恢复/一致性场景下可按 offset 截断分区：

- 保留截断点之前记录
- 重建分区目录与段文件
- 更新 checkpoint 和运行时状态

这为“回滚到某 offset”类操作提供了基础能力。
