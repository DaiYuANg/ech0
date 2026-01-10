package ech0.model;

import io.soabase.recordbuilder.core.RecordBuilder;

// Header record
@RecordBuilder
public record MessageHeader(
  byte magic,
  byte version,
  byte msgType,
  byte flags,
  short topicLen,
  int partition,
  short msgCount,
  int payloadLen,
  int reserved
) {
}
