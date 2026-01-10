package ech0;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class TestMainVerticle {

  @BeforeEach
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
//    vertx.deployVerticle(new MainVerticle()).onComplete(testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  void verticle_deployed(Vertx vertx, @NonNull VertxTestContext testContext) throws Throwable {
    testContext.completeNow();
  }
}
