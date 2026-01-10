package ech0.model;

import io.soabase.recordbuilder.core.RecordBuilder;
import org.github.gestalt.config.annotations.ConfigPrefix;

@RecordBuilder
//@ConfigPrefix(prefix = "server")
public record ServerConfig(
  int port,
  boolean preferNativeTransport
) {
}
