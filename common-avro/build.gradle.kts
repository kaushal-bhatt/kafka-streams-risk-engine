// Avro code generation without the Gradle Avro plugin.
//
// The usual choice, com.github.davidmc24.gradle.plugin.avro, was archived in October 2023
// and was last tested against Gradle 7.6. This project is on Gradle 9.x, so rather than
// gamble on an unmaintained plugin we drive avro-tools directly. Two steps, both cacheable:
//
//   risk.avdl --(idl)--> risk.avpr --(compile protocol)--> generated Java

plugins {
    `java-library`
}

val avroVersion = providers.gradleProperty("avroVersion").get()
val confluentVersion = providers.gradleProperty("confluentVersion").get()

// Isolated classpath: avro-tools is a shaded fat jar and must not leak into compile.
val avroTools: Configuration by configurations.creating

dependencies {
    avroTools("org.apache.avro:avro-tools:$avroVersion")

    // Consumers of this module get the Avro runtime and the Confluent serdes transitively.
    api("org.apache.avro:avro:$avroVersion")
    api("io.confluent:kafka-streams-avro-serde:$confluentVersion")
}

val idlFile = layout.projectDirectory.file("src/main/avro/risk.avdl")
val protocolFile = layout.buildDirectory.file("avro/risk.avpr")
val generatedJavaDir = layout.buildDirectory.dir("generated/avro/java")
val generatedSchemaDir = layout.buildDirectory.dir("generated/avro/schema")

val compileAvroIdl by tasks.registering(JavaExec::class) {
    group = "avro"
    description = "Compiles the Avro IDL into a protocol (.avpr) file."

    inputs.file(idlFile)
    outputs.file(protocolFile)
    outputs.cacheIf { true }

    classpath = avroTools
    mainClass = "org.apache.avro.tool.Main"
    // avro-tools writes and reads these files with the platform default charset. On
    // Windows that is windows-1252, so a single non-ASCII character in a doc comment
    // gets written as a byte that Jackson then rejects as invalid UTF-8.
    defaultCharacterEncoding = "UTF-8"
    argumentProviders.add(CommandLineArgumentProvider {
        listOf("idl", idlFile.asFile.absolutePath, protocolFile.get().asFile.absolutePath)
    })

    doFirst { protocolFile.get().asFile.parentFile.mkdirs() }
}

val generateAvroJava by tasks.registering(JavaExec::class) {
    group = "avro"
    description = "Generates Java classes from the Avro protocol."
    dependsOn(compileAvroIdl)

    inputs.file(protocolFile)
    outputs.dir(generatedJavaDir)
    outputs.cacheIf { true }

    classpath = avroTools
    mainClass = "org.apache.avro.tool.Main"
    // avro-tools writes and reads these files with the platform default charset. On
    // Windows that is windows-1252, so a single non-ASCII character in a doc comment
    // gets written as a byte that Jackson then rejects as invalid UTF-8.
    defaultCharacterEncoding = "UTF-8"
    argumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "compile",
            // Generate java.lang.String rather than CharSequence. Without this every
            // string field comes back as a Utf8 and every comparison is a trap.
            "-string",
            "protocol",
            protocolFile.get().asFile.absolutePath,
            generatedJavaDir.get().asFile.absolutePath,
        )
    })

    doFirst { generatedJavaDir.get().asFile.mkdirs() }
}

// Not on the compile path — run it by hand when you want .avsc files to register or diff.
val generateAvroSchemas by tasks.registering(JavaExec::class) {
    group = "avro"
    description = "Emits one .avsc per record, for schema registration and review."

    inputs.file(idlFile)
    outputs.dir(generatedSchemaDir)

    classpath = avroTools
    mainClass = "org.apache.avro.tool.Main"
    // avro-tools writes and reads these files with the platform default charset. On
    // Windows that is windows-1252, so a single non-ASCII character in a doc comment
    // gets written as a byte that Jackson then rejects as invalid UTF-8.
    defaultCharacterEncoding = "UTF-8"
    argumentProviders.add(CommandLineArgumentProvider {
        listOf("idl2schemata", idlFile.asFile.absolutePath, generatedSchemaDir.get().asFile.absolutePath)
    })

    doFirst { generatedSchemaDir.get().asFile.mkdirs() }
}

sourceSets.main {
    java.srcDir(generateAvroJava)
}
