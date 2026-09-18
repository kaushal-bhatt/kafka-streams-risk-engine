package com.kaushal.riskengine.topology;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.EnrichedTransaction;
import com.kaushal.riskengine.config.AvroSerdes;
import com.kaushal.riskengine.decision.RiskEvaluatorSupplier;
import com.kaushal.riskengine.decision.RiskPolicy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * Stage 2 - the decision path.
 *
 * <pre>
 *   enriched transactions -&gt; RiskEvaluator (Processor API, 3 state stores) -&gt; payments.decisions.v1
 * </pre>
 *
 * <p>No repartition anywhere on this path: the stream is still keyed by cardId, and every
 * store the evaluator owns is keyed by cardId too, so each card's whole history lives in the
 * same task as its transactions.
 */
@Component
public class DecisionTopology {

    private final AvroSerdes avroSerdes;
    private final MeterRegistry meterRegistry;
    private final boolean inMemoryStores;
    private final RiskPolicy policy;

    @Autowired
    public DecisionTopology(AvroSerdes avroSerdes, MeterRegistry meterRegistry) {
        this(avroSerdes, meterRegistry, false, RiskPolicy.DEFAULT);
    }

    /**
     * For the offline evaluation: in-memory stores (see RiskEvaluatorSupplier), and a policy
     * other than the default, so variants can be compared without editing code.
     */
    public DecisionTopology(AvroSerdes avroSerdes, MeterRegistry meterRegistry, boolean inMemoryStores,
                            RiskPolicy policy) {
        this.avroSerdes = avroSerdes;
        this.meterRegistry = meterRegistry;
        this.inMemoryStores = inMemoryStores;
        this.policy = policy;
    }

    @Bean
    public KStream<String, Decision> decisionStream(KStream<String, EnrichedTransaction> enrichmentStream) {
        SpecificAvroSerde<Decision> decisionSerde = avroSerdes.value();

        KStream<String, Decision> decisions = enrichmentStream.process(
                new RiskEvaluatorSupplier(avroSerdes, meterRegistry, inMemoryStores, policy),
                Named.as("risk-evaluator"));

        decisions.to(Topics.DECISIONS, Produced.with(Serdes.String(), decisionSerde).withName("decisions-sink"));

        // The latest decision per card, as a table, so Interactive Queries can answer "what
        // happened last on this card?" from local state. Same key, same partitioning, so no
        // repartition - it lives in the same task as the card's other stores.
        decisions.toTable(
                Named.as("last-decision"),
                Materialized.<String, Decision, KeyValueStore<Bytes, byte[]>>as(Topics.LAST_DECISION_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(decisionSerde));

        return decisions;
    }
}
