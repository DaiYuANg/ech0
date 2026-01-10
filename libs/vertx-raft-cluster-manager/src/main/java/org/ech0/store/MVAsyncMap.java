package org.ech0.store;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.shareddata.AsyncMap;
import lombok.extern.slf4j.Slf4j;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class MVAsyncMap<K, V> implements AsyncMap<K, V> {

  private final MVStore store;
  private final MVMap<K, V> map;

  public MVAsyncMap(MVStore store, MVMap<K, V> map) {
    this.store = store;
    this.map = map;
  }

  @Override
  public Future<V> get(K k) {
    return null;
  }

  @Override
  public Future<Void> put(K k, V v) {
    return null;
  }

  @Override
  public Future<Void> put(K k, V v, long l) {
    return null;
  }

  @Override
  public Future<V> putIfAbsent(K k, V v) {
    return null;
  }

  @Override
  public Future<V> putIfAbsent(K k, V v, long l) {
    return null;
  }

  @Override
  public Future<V> remove(K k) {
    return null;
  }

  @Override
  public Future<Boolean> removeIfPresent(K k, V v) {
    return null;
  }

  @Override
  public Future<V> replace(K k, V v) {
    return null;
  }

  @Override
  public Future<Boolean> replaceIfPresent(K k, V v, V v1) {
    return null;
  }

  @Override
  public Future<Void> clear() {
    return null;
  }

  @Override
  public Future<Integer> size() {
    return null;
  }

  @Override
  public Future<Set<K>> keys() {
    return null;
  }

  @Override
  public Future<List<V>> values() {
    return null;
  }

  @Override
  public Future<Map<K, V>> entries() {
    return null;
  }
}
