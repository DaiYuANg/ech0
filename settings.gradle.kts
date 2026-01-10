pluginManagement {
  repositories {
    mavenLocal()
    mavenCentral()
    gradlePluginPortal()
    google()
  }
}

plugins {
  id("org.gradle.toolchains.foojay-resolver-convention") version "0.10.0"
  id("org.danilopianini.gradle-pre-commit-git-hooks") version "2.0.23"
}

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

buildCache {
  local {
    isEnabled = true
    directory = File(rootProject.projectDir, ".gradle/build-cache")
  }
}


rootProject.name = "ech0"

include("libs:vertx-raft-cluster-manager")
include("apps:server")
