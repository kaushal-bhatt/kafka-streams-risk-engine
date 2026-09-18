package com.kaushal.riskengine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Everything tunable about the Streams application, bound from {@code risk.*}.
 */
@ConfigurationProperties(prefix = "risk")
public record RiskEngineProperties(

        @DefaultValue("risk-engine") String applicationId,

        @DefaultValue("localhost:9092") String bootstrapServers,

        @DefaultValue("http://localhost:8081") String schemaRegistryUrl,

        @DefaultValue("./state") String stateDir,

        /**
         * Advertised host:port of this instance, used by Interactive Queries from stage 3 to
         * route a request to whichever instance owns the key's partition.
         */
        @DefaultValue("localhost:8088") String applicationServer,

        /**
         * One warm copy of every store on another instance, so queries survive an instance
         * dying (as stale reads) and takeover skips the full changelog replay.
         */
        @DefaultValue("1") int numStandbyReplicas,

        /**
         * {@code exactly_once_v2} by default. Set {@code at_least_once} to measure the
         * difference with the latency probe; see the README.
         */
        @DefaultValue("exactly_once_v2") String processingGuarantee
) {
}
