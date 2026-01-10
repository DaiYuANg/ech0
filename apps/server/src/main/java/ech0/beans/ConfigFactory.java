package ech0.beans;

import ech0.model.ServerConfig;
import io.avaje.inject.Bean;
import io.avaje.inject.Factory;
import io.github.cdimascio.dotenv.Dotenv;
import jakarta.inject.Named;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.github.gestalt.config.Gestalt;
import org.github.gestalt.config.builder.GestaltBuilder;
import org.github.gestalt.config.json.JsonLoader;
import org.github.gestalt.config.loader.ConfigLoader;
import org.github.gestalt.config.loader.EnvironmentVarsLoader;
import org.github.gestalt.config.source.*;
import org.github.gestalt.config.tag.Tags;
import org.github.gestalt.config.toml.TomlLoader;
import org.github.gestalt.config.yaml.YamlLoader;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Set;

@Factory
@Slf4j
public class ConfigFactory {

  @SneakyThrows
  @Bean
  @Named
  ConfigSourcePackage classPathConfigSource() {
    return ClassPathConfigSourceBuilder.builder()
      .setResource("default.yaml")
      .build();
  }


  @SneakyThrows
  @Bean
  @Named
  ConfigSourcePackage environmentConfigSource() {
    return EnvironmentConfigSourceBuilder.builder()
      .setPrefix("ECH0_")
      .build();
  }

  @Bean
  @Named
  ConfigLoader yamlLoader() {
    return new YamlLoader();
  }

  @Bean
  @Named
  ConfigLoader tomlLoader() {
    return new TomlLoader();
  }

  @Bean
  @Named
  ConfigLoader jsonLoader() {
    return new JsonLoader();
  }

  @Bean
  @Named
  ConfigLoader environmentVarsLoader() {
    return new EnvironmentVarsLoader();
  }

  @SneakyThrows
  @Bean
  ConfigSourcePackage dotenvConfigSource() {
    return new ConfigSourcePackage(new DotenvConfigSource(), List.of(), Tags.of());
  }

  @SneakyThrows
  @Bean
  Gestalt gestalt(
    @NonNull List<ConfigSourcePackage> configSources,
    @NonNull List<ConfigLoader> configLoaders
  ) {
    val builder = new GestaltBuilder();
    builder.addSources(configSources);
    builder.addConfigLoaders(configLoaders);
    val instance = builder.build();
    instance.loadConfigs();
    log.atInfo().log("Config:{}", instance.debugPrint());
    return instance;
  }

  @SneakyThrows
  @Bean
  ServerConfig serverConfig(@NonNull Gestalt gestalt) {
    return gestalt.getConfig("server", ServerConfig.class);
  }
}
