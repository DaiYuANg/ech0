import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  application
  alias(libs.plugins.shadow)
}

dependencies {
  implementation(libs.vertx.core)
  implementation(libs.vertx.opentelemetry)
  implementation(libs.vertx.json.schema)
  implementation(libs.vertx.health.check)
  implementation(libs.vertx.micrometer.metrics)
  implementation(libs.vertx.tcp.eventbus.bridge)

  // Mutiny Vert.x bindings
  implementation(libs.vertx.mutiny.core)
  implementation(libs.vertx.mutiny.web)
  // DI
  implementation(libs.avaje.inject)
  annotationProcessor(libs.avaje.inject.processor)

  // Config
  implementation(libs.bundles.config)

  implementation(libs.slf4j.jul.bridge)
  implementation(libs.logback.classic)
  implementation(libs.logback.core)
  // Utils
  implementation(libs.agrona)
  implementation(libs.fastutil)
  implementation(libs.guava)

  implementation(libs.mapstruct)
  annotationProcessor(libs.mapstruct.annotation.processor)

  implementation(libs.record.builder.core)
  annotationProcessor(libs.record.builder.processor)

  implementation(projects.libs.vertxRaftClusterManager)

  // Testing
  testImplementation(libs.vertx.junit5)
  testImplementation("org.junit.jupiter:junit-jupiter:${libs.versions.junit.get()}")
}


tasks.withType<ShadowJar> {
  archiveClassifier.set("fat")
  mergeServiceFiles()
}
