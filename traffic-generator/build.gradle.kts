plugins {
    application
}

val confluentVersion = providers.gradleProperty("confluentVersion").get()

dependencies {
    implementation(project(":common-avro"))
    implementation("org.apache.kafka:kafka-clients:3.7.1")
    implementation("org.apache.commons:commons-csv:1.11.0")
    implementation("org.slf4j:slf4j-simple:2.0.16")

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "com.kaushal.trafficgen.TrafficGenerator"
}

// ./gradlew :traffic-generator:run --args="card-testing"
tasks.named<JavaExec>("run") {
    standardInput = System.`in`
    // Resolve --data-dir=data against the repo root, where scripts/fetch-data.sh puts it,
    // rather than against this module's directory.
    workingDir = rootProject.projectDir
}
