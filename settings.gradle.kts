plugins {
    // Lets Gradle download the Java 21 toolchain automatically, so the build does not
    // depend on which JDK happens to be on PATH.
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}

rootProject.name = "kafka-streams-risk-engine"

include("common-avro")
include("risk-engine")
include("traffic-generator")
include("evaluation")
