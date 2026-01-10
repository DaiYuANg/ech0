package ech0.server;

import ech0.codec.ProtocolCodec;
import ech0.model.ServerConfig;
import ech0.queue.SimpleQueue;
import io.vertx.mutiny.core.net.NetServer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@RequiredArgsConstructor
public class TcpMessageServer {

  private final NetServer server;
  private final SimpleQueue<ProtocolCodec.DecodedMessage> queue;

  public void start(int port) {
    server.connectHandler(socket ->
        socket.handler(buffer -> {
          val msg = ProtocolCodec.decode(buffer);
          log.atInfo().log("Received message: {}", msg);
          queue.push(msg); // 推入队列
        })
      )
      .listen(port)
      .subscribe()
      .with(
        server -> log.atInfo().log("TCP Server started on {}", server.actualPort()),
        (t) -> log.atError().log(t.getMessage(), t));
    queue.start();
    log.atInfo().log("TCP Server listening on {}", port);
  }

  public void stop() {
    server.closeAndAwait();
    queue.stop();
  }
}
