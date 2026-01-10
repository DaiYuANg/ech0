package ech0.beans;

import io.avaje.inject.Bean;
import io.avaje.inject.Factory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.net.NetServer;
import lombok.val;
import org.ech0.RaftClusterManager;
import org.ech0.model.Config;
import org.jspecify.annotations.NonNull;

@Factory
public class VertxFactory {

  @Bean
  ClusterManager clusterManager() {
    val config = new Config();
    config.setPath("data.mv");
    return new RaftClusterManager(config);
  }

  @Bean
  Vertx vertx(ClusterManager clusterManager) {
//    return Vertx.builder().withClusterManager(clusterManager).buildClusteredAndAwait();
    return Vertx.builder().build();
  }

  @Bean
  NetServer netServer(@NonNull Vertx vertx) {
    return vertx.createNetServer();
  }
}
