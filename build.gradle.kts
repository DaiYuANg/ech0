import name.remal.gradle_plugins.lombok.LombokPlugin
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
  java
  alias(libs.plugins.lombok) apply false
}

group = "org.ech0"
version = "1.0.0-SNAPSHOT"

allprojects {
  repositories {
    mavenLocal()
    mavenCentral()
    google()
    gradlePluginPortal()
  }
}

subprojects {
  apply<JavaLibraryPlugin>()
  apply<LombokPlugin>()

  dependencies {
    implementation(platform(rootProject.libs.vertx.stack.depchain))
    implementation(rootProject.libs.slf4j.api)
  }

  java {
    sourceCompatibility = JavaVersion.VERSION_25
    targetCompatibility = JavaVersion.VERSION_25
  }

  tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
      events = setOf(PASSED, SKIPPED, FAILED)
    }
  }
}


