package ech0;

import ech0.arch.Lifecycle;
import ech0.codec.ProtocolCodec;
import ech0.instance.DI;
import ech0.model.ServerConfig;
import ech0.queue.SimpleQueue;
import ech0.server.TcpMessageServer;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.net.NetServer;
import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.github.gestalt.config.Gestalt;

@Slf4j
@Singleton
public class Ech0 implements Lifecycle {

  private final SimpleQueue<ProtocolCodec.DecodedMessage> queue = new SimpleQueue<>(this::processMessage);

  private final TcpMessageServer server;

  private final ServerConfig serverConfig;

  @SneakyThrows
  public Ech0(NetServer netServer, ServerConfig serverConfig) {
    this.serverConfig = serverConfig;
    val queue = new SimpleQueue<>(this::processMessage);
    this.server = new TcpMessageServer(netServer, queue);
  }

  public void start() {
    log.atInfo().log("Start");
    server.start(serverConfig.port());
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    log.info("Application started, PID={}", ProcessHandle.current().pid());
  }

  @Override
  public void stop() {
    log.info("Stopping application...");
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Shutdown signal received...");
      stop();
    }));
    log.info("Application stopped");
  }

  private Uni<Void> processMessage(ProtocolCodec.DecodedMessage msg) {
    log.info("Processing message: {}", msg);
    // TODO: 可以分发给不同 Handler
    return Uni.createFrom().voidItem();
  }
}
