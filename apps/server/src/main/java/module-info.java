module ech0 {
  requires io.avaje.inject;
  requires io.smallrye.mutiny;
  requires io.smallrye.mutiny.vertx.core;
  requires io.smallrye.mutiny.vertx.runtime;
  requires io.soabase.recordbuilder.core;
  requires io.vertx.core;
  requires jakarta.inject;
  requires org.slf4j;
  requires jul.to.slf4j;
  requires java.compiler;
  requires java.logging;
  requires static lombok;
  requires org.github.gestalt.core;
  requires org.jspecify;
  requires ech0.vertx.raft.cluster.manager;
  provides io.avaje.inject.spi.InjectExtension with ech0.Ech0Module;
}
