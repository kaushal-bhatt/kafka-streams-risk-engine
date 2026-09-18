package com.kaushal.riskengine.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class StreamsConfiguration {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kafkaStreamsConfiguration(RiskEngineProperties properties) {
        Map<String, Object> config = new HashMap<>();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, properties.applicationId());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, properties.stateDir());

        // Keys are always the entity id as a String: cardId, merchantId, customerId.
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, properties.schemaRegistryUrl());

        // Value serdes are always supplied explicitly at each node rather than defaulted.
        // A topology with a default value serde is one rename away from silently
        // deserializing the wrong type.

        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, properties.numStandbyReplicas());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, properties.processingGuarantee());

        // Needed from stage 3 so queryMetadataForKey can tell a caller which instance owns
        // a key. Harmless before then.
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, properties.applicationServer());

        // Stage 6 replaces this with a handler that routes the failing bytes to the DLQ
        // topic. Continuing rather than dying is already the right call: one malformed
        // record must not stall a partition.
        config.put(
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class
        );

        return new KafkaStreamsConfiguration(config);
    }

    /**
     * One bad thread must not take the instance down. Kafka Streams will start a
     * replacement and carry on from the last committed offset.
     */
    @Bean
    StreamsBuilderFactoryBeanConfigurer uncaughtExceptionHandler() {
        return factoryBean -> factoryBean.setStreamsUncaughtExceptionHandler(
                exception -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD
        );
    }
}
