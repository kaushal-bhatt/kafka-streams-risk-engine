package com.kaushal.riskengine.query;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.config.RiskEngineProperties;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.time.Clock;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Answers "what does the engine know about this card?" from whichever instance owns it.
 *
 * <p>Each card's state lives on exactly one instance - the one assigned the card's partition.
 * A naive Interactive Queries endpoint reads its local stores and returns 404 for the other
 * half of the cards. This one routes:
 *
 * <ol>
 *   <li>{@code queryMetadataForKey} hashes the card id exactly as the producer did and
 *       returns the instance that owns that partition, as advertised through each instance's
 *       {@code application.server} setting.</li>
 *   <li>If that is this instance, read the local stores.</li>
 *   <li>If not, forward the request over HTTP to the owner, with {@code local=true} so it
 *       cannot bounce back.</li>
 *   <li>If the owner is unreachable, answer from a <em>standby</em> replica - on this instance
 *       if it holds one, otherwise on the instance that does - and mark the answer stale.</li>
 * </ol>
 *
 * <p>Step 4 is why standby replicas matter for reads. When an instance dies, Kafka needs its
 * session timeout (tens of seconds) to notice and move its partitions. Without standbys, every
 * query for those cards fails for that whole window. With them, the survivor already holds a
 * warm copy and answers immediately. The answer may trail the dead owner's last writes, and is
 * labelled as such rather than passed off as fresh.
 *
 * <p>One metadata lookup routes all five stores, because they are all keyed by card id and
 * all live in the same sub-topology, so every one of them has the same owner for a given card.
 */
@Service
public class CardRiskQueryService {

    private final StreamsBuilderFactoryBean factoryBean;
    private final LocalCardStores localStores;
    private final RestClient restClient;
    private final HostInfo self;
    private final Clock clock;

    @Autowired
    public CardRiskQueryService(StreamsBuilderFactoryBean factoryBean, LocalCardStores localStores,
                                RestClient.Builder restClientBuilder, RiskEngineProperties properties) {
        this(factoryBean, localStores, restClientBuilder.requestFactory(timeouts()).build(),
                HostInfo.buildFromEndpoint(properties.applicationServer()), Clock.systemUTC());
    }

    CardRiskQueryService(StreamsBuilderFactoryBean factoryBean, LocalCardStores localStores,
                         RestClient restClient, HostInfo self, Clock clock) {
        this.factoryBean = factoryBean;
        this.localStores = localStores;
        this.restClient = restClient;
        this.self = self;
        this.clock = clock;
    }

    /**
     * @param forwarded  set on requests forwarded from another instance: answer from local
     *                   state or fail, never forward again
     * @param allowStale set by a caller falling back to this instance as a standby
     */
    public Optional<CardRiskView> find(String cardId, boolean forwarded, boolean allowStale) {
        KafkaStreams streams = streams();

        KeyQueryMetadata owner = streams.queryMetadataForKey(
                Topics.CARD_PROFILE_STORE, cardId, Serdes.String().serializer());
        if (owner == null || KeyQueryMetadata.NOT_AVAILABLE.equals(owner) || owner.activeHost().port() < 0) {
            throw new StoreNotReadyException("no owner for this card's partition yet - the group is rebalancing");
        }

        if (self.equals(owner.activeHost())) {
            return local(cardId, owner.partition(), false);
        }

        if (forwarded) {
            if (allowStale && owner.standbyHosts().contains(self)) {
                return local(cardId, owner.partition(), true);
            }
            // The partition moved between the caller's lookup and ours. Let the caller retry
            // rather than bouncing the request around the cluster.
            throw new StoreNotReadyException("partition " + owner.partition() + " is now owned by "
                    + endpoint(owner.activeHost()));
        }

        try {
            return forward(owner.activeHost(), cardId, false).map(view -> view.withRoutedVia(endpoint(self)));
        } catch (StoreNotReadyException ownerDown) {
            return fromStandby(cardId, owner, ownerDown);
        }
    }

    /**
     * The owner can't answer. Serve the standby copy instead: this instance's own if it has
     * one, since that costs no network hop, otherwise the first standby that responds.
     */
    private Optional<CardRiskView> fromStandby(String cardId, KeyQueryMetadata owner,
                                               StoreNotReadyException ownerDown) {
        if (owner.standbyHosts().contains(self)) {
            return local(cardId, owner.partition(), true);
        }
        for (HostInfo standby : owner.standbyHosts()) {
            try {
                return forward(standby, cardId, true).map(view -> view.withRoutedVia(endpoint(self)));
            } catch (StoreNotReadyException standbyDown) {
                // Try the next one.
            }
        }
        throw ownerDown;
    }

    private Optional<CardRiskView> local(String cardId, int partition, boolean stale) {
        return CardRiskAssembler.assemble(cardId, partition, endpoint(self), clock.instant(),
                        localStores.forPartition(partition, stale))
                .map(view -> stale ? view.asStale() : view);
    }

    /** Which instance owns which partitions, active and standby. */
    public List<InstanceView> instances() {
        return streams().metadataForAllStreamsClients().stream()
                .map(m -> new InstanceView(
                        endpoint(m.hostInfo()),
                        m.hostInfo().equals(self),
                        partitionsOf(m.topicPartitions()),
                        partitionsOf(m.standbyTopicPartitions())))
                .sorted(Comparator.comparing(InstanceView::host))
                .toList();
    }

    public String self() {
        return endpoint(self);
    }

    private Optional<CardRiskView> forward(HostInfo owner, String cardId, boolean stale) {
        try {
            return restClient.get()
                    .uri("http://{host}:{port}/risk/cards/{cardId}?local=true&stale={stale}",
                            owner.host(), owner.port(), cardId, stale)
                    .exchange((request, response) -> {
                        int status = response.getStatusCode().value();
                        if (status == 404) {
                            return Optional.<CardRiskView>empty();
                        }
                        if (status == 503) {
                            throw new StoreNotReadyException("owner " + endpoint(owner) + " is not ready");
                        }
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            throw new IllegalStateException("owner " + endpoint(owner) + " answered " + status);
                        }
                        return Optional.ofNullable(response.bodyTo(CardRiskView.class));
                    });
        } catch (ResourceAccessException e) {
            // The owner is down. Kafka will notice and move its partitions once its session
            // times out; until then the honest answer is "try again shortly".
            throw new StoreNotReadyException("owner " + endpoint(owner) + " is unreachable", e);
        }
    }

    private KafkaStreams streams() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            throw new StoreNotReadyException("Kafka Streams has not started");
        }
        return streams;
    }

    private static List<Integer> partitionsOf(Set<TopicPartition> partitions) {
        return partitions.stream()
                .filter(tp -> tp.topic().equals(Topics.TRANSACTIONS))
                .map(TopicPartition::partition)
                .sorted()
                .toList();
    }

    private static String endpoint(HostInfo host) {
        return host.host() + ":" + host.port();
    }

    private static SimpleClientHttpRequestFactory timeouts() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2_000);
        factory.setReadTimeout(3_000);
        return factory;
    }

    public record InstanceView(String host, boolean self, List<Integer> activePartitions,
                               List<Integer> standbyPartitions) {
    }
}
