package org.ech0.store;

import io.vertx.core.Promise;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.core.spi.cluster.RegistrationInfo;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.ech0.model.Config;
import org.h2.mvstore.MVStore;

import java.util.List;
import java.util.Map;

@Slf4j
public class MVStoreStore implements Store {

  private final MVStore store;

  public MVStoreStore(@NonNull Config config) {
    store = new MVStore.Builder()
      .fileName(config.getPath())
      .open();
    log.atInfo().log("MVStore opened at {}", config.getPath());
  }

  @Override
  public <K, V> AsyncMap<K, V> getAsyncMap(String name) {
    return null;
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return Map.of();
  }

  @Override
  public Lock getLock(String name) {
    return null;
  }

  @Override
  public Counter getCounter(String name) {
    return null;
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
  public void setNodeListener(NodeListener listener) {

  }

  @Override
  public void setNodeInfo(NodeInfo info, Promise<Void> promise) {

  }

  @Override
  public NodeInfo getNodeInfo() {
    return null;
  }

  @Override
  public void getNodeInfo(String nodeId, Promise<NodeInfo> promise) {

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
  public void addRegistration(String name, RegistrationInfo info, Promise<Void> promise) {

  }

  @Override
  public void removeRegistration(String name, RegistrationInfo info, Promise<Void> promise) {

  }

  @Override
  public void getRegistrations(String name, Promise<List<RegistrationInfo>> promise) {

  }
}
