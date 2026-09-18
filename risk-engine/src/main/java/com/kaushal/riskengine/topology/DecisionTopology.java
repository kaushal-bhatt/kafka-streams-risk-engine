package com.kaushal.riskengine.topology;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.EnrichedTransaction;
import com.kaushal.riskengine.config.AvroSerdes;
import com.kaushal.riskengine.decision.RiskEvaluatorSupplier;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
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

    public DecisionTopology(AvroSerdes avroSerdes) {
        this.avroSerdes = avroSerdes;
    }

    @Bean
    public KStream<String, Decision> decisionStream(KStream<String, EnrichedTransaction> enrichmentStream) {
        SpecificAvroSerde<Decision> decisionSerde = avroSerdes.value();

        KStream<String, Decision> decisions = enrichmentStream.process(
                new RiskEvaluatorSupplier(avroSerdes),
                Named.as("risk-evaluator"));

        decisions.to(Topics.DECISIONS, Produced.with(Serdes.String(), decisionSerde).withName("decisions-sink"));

        return decisions;
    }
}
