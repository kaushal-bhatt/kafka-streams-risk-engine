package com.kaushal.riskengine.config;

import com.kaushal.riskengine.errors.DeadLetterPublisher;
import com.kaushal.riskengine.errors.DeadLetterQueueHandler;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
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
    KafkaStreamsConfiguration kafkaStreamsConfiguration(RiskEngineProperties properties,
                                                        DeadLetterPublisher deadLetterPublisher) {
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

        // A warm copy of every store on another instance. It doesn't make Kafka notice a dead
        // instance any faster - that's the session timeout. What it buys is that the survivor
        // (a) can answer queries for the dead instance's cards immediately, as stale reads,
        // and (b) doesn't have to replay changelogs from scratch when it takes over.
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, properties.numStandbyReplicas());

        // exactly_once_v2: the input offset, every state store change and the output
        // decision commit together as one Kafka transaction. A crash can't produce a
        // decision twice, or count one authorisation twice toward velocity. The cost -
        // measured, see the README - is that output only becomes visible to read_committed
        // consumers when the transaction commits, every 100 ms under EOS.
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, properties.processingGuarantee());

        // Lets queryMetadataForKey tell a caller which instance owns a key.
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, properties.applicationServer());

        // Poison pills go to the dead-letter topic with the reason attached, and the partition
        // keeps moving. The handler is instantiated by Kafka, so it gets its publisher through
        // the config map rather than from Spring.
        config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DeadLetterQueueHandler.class);
        config.put(DeadLetterQueueHandler.PUBLISHER_CONFIG, deadLetterPublisher);

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
