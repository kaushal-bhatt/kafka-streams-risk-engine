package com.kaushal.riskengine.health;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/**
 * Reports the Streams state on {@code /actuator/health}.
 *
 * <p>Spring Boot's own health check says the HTTP port is open, which tells you nothing
 * about whether the topology is running. An instance in ERROR, or stuck in REBALANCING,
 * serves requests perfectly well and answers every query wrongly.
 */
@Component
public class StreamsHealthIndicator implements HealthIndicator {

    private final StreamsBuilderFactoryBean factoryBean;

    public StreamsHealthIndicator(StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    @Override
    public Health health() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            return Health.down().withDetail("streams", "not started").build();
        }

        KafkaStreams.State state = streams.state();
        Health.Builder builder = state == KafkaStreams.State.RUNNING
                ? Health.up()
                // REBALANCING is not an error, but it is not ready to answer queries either.
                : Health.status(state == KafkaStreams.State.REBALANCING ? "REBALANCING" : "DOWN");

        return builder
                .withDetail("state", state.name())
                .withDetail("threads", streams.metadataForLocalThreads().size())
                .build();
    }
}
