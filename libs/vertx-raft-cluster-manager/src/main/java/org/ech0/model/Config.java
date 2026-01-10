package org.ech0.model;

import lombok.Data;

@Data
public class Config {
  private String path = "raft.mv";
  private boolean enableSnapshot = true;
  private long snapshotIntervalMs = 60_000; // 默认 1 分钟
}
