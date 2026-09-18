plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

val confluentVersion = providers.gradleProperty("confluentVersion").get()
val testcontainersVersion = providers.gradleProperty("testcontainersVersion").get()

dependencies {
    implementation(project(":common-avro"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
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
