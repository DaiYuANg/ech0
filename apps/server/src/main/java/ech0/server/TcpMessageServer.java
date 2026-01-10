package ech0.server;

import ech0.codec.ProtocolCodec;
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
    server.connectHandler(socket -> {
        socket.handler(buffer -> {
          val msg = ProtocolCodec.decode(buffer);
          log.info("Received message: {}", msg);
          queue.push(msg); // 推入队列
        });
      })
      .listen(port)
      .await()
      .indefinitely();
    queue.start();
    log.info("TCP Server listening on {}", port);
  }

  public void stop() {
    server.closeAndAwait();
    queue.stop();
  }
}
