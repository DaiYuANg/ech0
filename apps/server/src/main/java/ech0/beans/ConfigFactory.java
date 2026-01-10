package ech0.beans;

import io.avaje.inject.Bean;
import io.avaje.inject.Factory;
import jakarta.inject.Named;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.github.gestalt.config.Gestalt;
import org.github.gestalt.config.builder.GestaltBuilder;
import org.github.gestalt.config.source.*;
import org.jspecify.annotations.NonNull;

import java.util.Set;

@Factory
@Slf4j
public class ConfigFactory {

  @SneakyThrows
  @Bean
  @Named
  ConfigSourcePackage classPathConfigSource() {
    return ClassPathConfigSourceBuilder.builder().setResource("/default.properties").build();
  }

  @SneakyThrows
  @Bean
  @Named
  ConfigSourcePackage environmentConfigSource() {
    return EnvironmentConfigSourceBuilder.builder().setPrefix("ECH0").build();
  }

  @SneakyThrows
  @Bean
  Gestalt gestalt(@NonNull Set<ConfigSourcePackage> configSources) {
    val builder = new GestaltBuilder();
    configSources.forEach(builder::addSource);
    return builder.build();
  }
}
