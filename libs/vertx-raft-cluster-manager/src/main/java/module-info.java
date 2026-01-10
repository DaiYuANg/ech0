module ech0.vertx.raft.cluster.manager {
  requires io.vertx.core;
  requires static lombok;
  requires org.slf4j;
  requires com.h2database.mvstore;

  exports org.ech0;
  exports org.ech0.store;
  exports org.ech0.model;
}
