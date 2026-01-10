package ech0.arch;

import io.smallrye.mutiny.Uni;

public interface MessageHandler<T> {
  Uni<Void> handle(T message);
}
