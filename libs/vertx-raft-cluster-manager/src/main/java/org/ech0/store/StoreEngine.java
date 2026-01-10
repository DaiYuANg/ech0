package org.ech0.store;

import java.util.Map;

public interface StoreEngine extends AutoCloseable {

  <K, V> Map<K, V> openMap(String name);

  void commit();

  void flush();

  @Override
  void close();
}
