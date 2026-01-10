package ech0.model;

import io.soabase.recordbuilder.core.RecordBuilder;

// 网络协议 Header
@RecordBuilder
public record NetHeader(
  byte magic,       // 协议标识
  byte version,     // 协议版本
  byte msgType,     // 消息类型: 1-PUBLISH, 2-SUBSCRIBE, 3-ACK, ...
  byte flags,       // 扩展标志位
  short topicLen,   // Topic 名长度
  int partition,    // Partition ID
  int payloadLen,   // Payload 长度（Fory/自定义序列化）
  int reserved      // 预留字段
) {
}
