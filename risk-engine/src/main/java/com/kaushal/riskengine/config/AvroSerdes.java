package com.kaushal.riskengine.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Builds {@link SpecificAvroSerde} instances pointed at the configured Schema Registry.
 *
 * <p>Serdes are deliberately created per-use rather than shared as singletons: a serde
 * carries its own {@code isKey} flag, and reusing one configured for values as a key serde
 * is a subtle and very annoying bug.
 *
 * <p>In tests the registry URL is {@code mock://...}, which the Confluent client resolves to
 * an in-JVM {@code MockSchemaRegistry}. That is what lets the TopologyTestDriver tests run
 * with no broker and no registry container.
 */
@Component
public class AvroSerdes {

    private final String schemaRegistryUrl;

    public AvroSerdes(RiskEngineProperties properties) {
        this.schemaRegistryUrl = properties.schemaRegistryUrl();
    }

    public <T extends SpecificRecord> SpecificAvroSerde<T> value() {
        return build(false);
    }

    public <T extends SpecificRecord> SpecificAvroSerde<T> key() {
        return build(true);
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> build(boolean isKey) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(
                Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                        // Fine for a demo. A real deployment registers schemas from CI and
                        // sets this false, so a bad deploy cannot quietly evolve a subject.
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true
                ),
                isKey
        );
        return serde;
    }
}
