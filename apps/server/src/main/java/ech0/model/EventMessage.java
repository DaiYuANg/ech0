package ech0.model;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record EventMessage(
  long msgId,
  String key,
  String value,
  long timestamp
) {
}
