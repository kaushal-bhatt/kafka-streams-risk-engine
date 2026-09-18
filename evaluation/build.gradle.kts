plugins {
    application
    id("io.spring.dependency-management")
}

// The engine's dependency versions - with one deliberate exception.
//
// Kafka Streams 3.8.1 instead of the engine's 3.7.1. In 3.7 the foreign-key join's internal
// subscription store is hard-coded to RocksDB (KTableImpl calls
// Stores.persistentTimestampedKeyValueStore directly; no config can change it). Under
// TopologyTestDriver, which commits after every record, that one disk store forced a
// checkpoint write and fsync per transaction: ~250 transactions/s, over two hours for the
// dataset. 3.8 builds that store through SubscriptionStoreFactory, which honours the
// in-memory DSL store setting. The topology code is identical; only the library hosting it
// differs. Revisit when the engine itself moves to 3.8.
dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:3.3.5") {
            bomProperty("kafka.version", "3.8.1")
        }
    }
}

val confluentVersion = providers.gradleProperty("confluentVersion").get()

dependencies {
    implementation(project(":common-avro"))
    // The production topology: EnrichmentTopology, DecisionTopology, RiskEvaluator.
    implementation(project(":risk-engine"))
    // SparkovMapping - the same CSV-to-record mapping the live replay uses. Its logging
    // backend is excluded so only the engine's (logback) is on the classpath.
    implementation(project(":traffic-generator")) {
        exclude(group = "org.slf4j", module = "slf4j-simple")
    }

    implementation("org.apache.kafka:kafka-streams")
    implementation("org.apache.kafka:kafka-streams-test-utils")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    implementation("io.micrometer:micrometer-core")
    implementation("org.apache.commons:commons-csv:1.11.0")

    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "com.kaushal.riskengine.evaluation.Evaluate"
    // RocksDB state for 1,000 cards is small; the heap mostly holds Avro schemas and buffers.
    applicationDefaultJvmArgs = listOf("-Xmx2g")
}

// ./gradlew :evaluation:run   (reads data/, writes docs/EVALUATION.md)
tasks.named<JavaExec>("run") {
    workingDir = rootProject.projectDir
}
