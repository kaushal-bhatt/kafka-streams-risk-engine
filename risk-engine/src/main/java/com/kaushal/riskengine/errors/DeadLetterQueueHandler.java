package com.kaushal.riskengine.errors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A record that can't be deserialized goes to the dead-letter topic, and the partition keeps
 * moving.
 *
 * <p>Without a handler, Kafka Streams fails on a poison pill. The thread dies, is replaced,
 * re-reads the same record and dies again, and that partition is stuck until someone
 * intervenes. {@code LogAndContinueExceptionHandler} avoids the loop but throws the record
 * away with a log line nobody reads. This keeps the partition moving <em>and</em> keeps the
 * evidence.
 *
 * <p>Kafka Streams creates this handler itself, so Spring can't inject into it. The publisher
 * arrives through the Streams config map instead, under {@link #PUBLISHER_CONFIG}. Config
 * values don't have to be strings, and {@code configure()} receives the original objects.
 */
public class DeadLetterQueueHandler implements DeserializationExceptionHandler {

    public static final String PUBLISHER_CONFIG = "risk.dlq.publisher";

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueHandler.class);

    private DeadLetterPublisher publisher;

    @Override
    public void configure(Map<String, ?> configs) {
        if (!(configs.get(PUBLISHER_CONFIG) instanceof DeadLetterPublisher configured)) {
            throw new ConfigException(PUBLISHER_CONFIG, configs.get(PUBLISHER_CONFIG),
                    "must be a DeadLetterPublisher instance");
        }
        this.publisher = configured;
    }

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {
        try {
            publisher.publish(record, exception, context.taskId().toString());
            log.warn("dead-lettered {}-{}@{}: {}", record.topic(), record.partition(), record.offset(),
                    exception.getMessage());
            return DeserializationHandlerResponse.CONTINUE;
        } catch (Exception dlqFailure) {
            // If the evidence can't be saved, stop. Skipping the record anyway would lose a
            // transaction without a trace, which is the one outcome this handler exists to
            // prevent.
            log.error("could not dead-letter {}-{}@{}; failing instead of dropping it",
                    record.topic(), record.partition(), record.offset(), dlqFailure);
            return DeserializationHandlerResponse.FAIL;
        }
    }
}
