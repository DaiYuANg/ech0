package org.ech0;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.*;
import lombok.extern.slf4j.Slf4j;
import org.ech0.model.Config;
import org.ech0.store.MVStoreStore;
import org.ech0.store.Store;
import org.h2.mvstore.MVStore;

import java.util.List;
import java.util.Map;

@Slf4j
public class RaftClusterManager implements ClusterManager {

  private final Store store;

  // 构造函数，可传配置
  public RaftClusterManager() {
    this(new Config());
  }

  public RaftClusterManager(Config config) {
    this.store = new MVStoreStore(config);
  }
  @Override
  public void init(Vertx vertx, NodeSelector nodeSelector) {
  }

  @Override
  public <K, V> void getAsyncMap(String s, Promise<AsyncMap<K, V>> promise) {

  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String s) {
    return Map.of();
  }

  @Override
  public void getLockWithTimeout(String s, long l, Promise<Lock> promise) {

  }

  @Override
  public void getCounter(String s, Promise<Counter> promise) {

  }

  @Override
  public String getNodeId() {
    return "";
  }

  @Override
  public List<String> getNodes() {
    return List.of();
  }

  @Override
  public void nodeListener(NodeListener nodeListener) {

  }

  @Override
  public void setNodeInfo(NodeInfo nodeInfo, Promise<Void> promise) {

  }

  @Override
  public NodeInfo getNodeInfo() {
    return null;
  }

  @Override
  public void getNodeInfo(String s, Promise<NodeInfo> promise) {

  }

  @Override
  public void join(Promise<Void> promise) {

  }

  @Override
  public void leave(Promise<Void> promise) {

  }

  @Override
  public boolean isActive() {
    return false;
  }

  @Override
  public void addRegistration(String s, RegistrationInfo registrationInfo, Promise<Void> promise) {

  }

  @Override
  public void removeRegistration(String s, RegistrationInfo registrationInfo, Promise<Void> promise) {

  }

  @Override
  public void getRegistrations(String s, Promise<List<RegistrationInfo>> promise) {

  }
}
