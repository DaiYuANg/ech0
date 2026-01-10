dependencies {
  implementation(platform(libs.vertx.stack.depchain))
  implementation(libs.vertx.core)
  implementation("com.h2database:h2-mvstore:2.4.240")
}
