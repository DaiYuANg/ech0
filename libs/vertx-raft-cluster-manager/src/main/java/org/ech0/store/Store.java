package org.ech0.store;

import io.vertx.core.Promise;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.core.spi.cluster.RegistrationInfo;

import java.util.List;
import java.util.Map;

public interface Store {
  <K, V> AsyncMap<K, V> getAsyncMap(String name);
  <K, V> Map<K, V> getSyncMap(String name);
  Lock getLock(String name);
  Counter getCounter(String name);

  String getNodeId();
  List<String> getNodes();
  void setNodeListener(NodeListener listener);

  void setNodeInfo(NodeInfo info, Promise<Void> promise);
  NodeInfo getNodeInfo();
  void getNodeInfo(String nodeId, Promise<NodeInfo> promise);

  void join(Promise<Void> promise);
  void leave(Promise<Void> promise);
  boolean isActive();

  void addRegistration(String name, RegistrationInfo info, Promise<Void> promise);
  void removeRegistration(String name, RegistrationInfo info, Promise<Void> promise);
  void getRegistrations(String name, Promise<List<RegistrationInfo>> promise);
}
