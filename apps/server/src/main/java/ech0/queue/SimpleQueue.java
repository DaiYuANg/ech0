package ech0.queue;


import ech0.arch.MessageHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class SimpleQueue<T> {

  private final BlockingQueue<T> queue = new LinkedBlockingQueue<>();
  private final MessageHandler<T> handler;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private volatile boolean running = true;

  public SimpleQueue(MessageHandler<T> handler) {
    this.handler = handler;
  }

  public void start() {
    executor.submit(() -> {
      while (running) {
        try {
          T msg = queue.take();
          handler.handle(msg);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          log.error("Message processing failed", e);
        }
      }
    });
  }

  public void stop() {
    running = false;
    executor.shutdownNow();
  }

  public void push(T msg) {
    queue.offer(msg);
  }
}
