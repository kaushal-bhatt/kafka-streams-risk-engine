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
         * Stage 6 raises this to 1 so a second instance keeps a warm copy of every store and
         * the query API survives an instance dying.
         */
        @DefaultValue("0") int numStandbyReplicas,

        /**
         * Stage 6 switches this to {@code exactly_once_v2}. Left at-least-once for now so the
         * cost of EOS can be measured as a deliberate before/after rather than assumed.
         */
        @DefaultValue("at_least_once") String processingGuarantee
) {
}
