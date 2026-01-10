package ech0.arch;

public interface Lifecycle {
  void start();
  void stop();
  default void init() {} // 可选初始化
}
