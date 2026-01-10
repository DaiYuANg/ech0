package ech0.instance;

import io.avaje.inject.BeanScope;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.logging.LogManager;


@Getter
public enum DI {
  INSTANCE;

  @Delegate
  private final BeanScope beanScope;

  DI() {
    LogManager.getLogManager().reset();
    SLF4JBridgeHandler.install();
    beanScope = BeanScope.builder().shutdownHook(true).build();
  }
}
