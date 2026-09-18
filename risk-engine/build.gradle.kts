plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

val confluentVersion = providers.gradleProperty("confluentVersion").get()
val testcontainersVersion = providers.gradleProperty("testcontainersVersion").get()

// Run from the repo root, like IntelliJ does, so the relative state directory (./state)
// resolves to the same place however the engine is started. Before this, bootRun put it in
// risk-engine/state and IntelliJ in ./state - and a reset that cleaned one left the other.
tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    workingDir = rootProject.projectDir
}

dependencies {
    implementation(project(":common-avro"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    // /actuator/prometheus: decision and rule counters, DLQ count, and every Kafka Streams
    // metric (lag, process latency, state store sizes) in a format Prometheus scrapes.
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.apache.kafka:kafka-streams")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.apache.kafka:kafka-streams-test-utils")
    // MockSchemaRegistry, so TopologyTestDriver tests need no broker and no registry.
    testImplementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    testImplementation(platform("org.testcontainers:testcontainers-bom:$testcontainersVersion"))
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:kafka")
    // Gradle 9 no longer puts the launcher on the test runtime classpath for you.
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
