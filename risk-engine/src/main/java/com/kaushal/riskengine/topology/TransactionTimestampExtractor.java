package com.kaushal.riskengine.topology;

import com.kaushal.riskengine.avro.Transaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes a transaction's Kafka timestamp its {@code eventTime}: when the terminal saw it,
 * not when the broker received it.
 *
 * <p>Everything time-based downstream depends on this. The velocity window, the daily
 * spend reset and the impossible-travel speed are all computed on event time, and the
 * velocity store expires old entries relative to stream time, which is derived from these
 * timestamps. A transaction that reaches Kafka two minutes late is still counted in the
 * minute it actually happened.
 *
 * <p>Falls back to the record's own timestamp, and then to the partition's current time,
 * rather than failing: a missing event time is a data-quality problem to log, not a reason
 * to stall the partition.
 */
public class TransactionTimestampExtractor implements TimestampExtractor {

    private static final Logger log = LoggerFactory.getLogger(TransactionTimestampExtractor.class);

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof Transaction transaction && transaction.getEventTime() != null) {
            long eventTime = transaction.getEventTime().toEpochMilli();
            if (eventTime > 0) {
                return eventTime;
            }
        }

        log.warn("transaction at {}-{}@{} has no usable eventTime; falling back to record time",
                record.topic(), record.partition(), record.offset());
        return record.timestamp() >= 0 ? record.timestamp() : partitionTime;
    }
}
