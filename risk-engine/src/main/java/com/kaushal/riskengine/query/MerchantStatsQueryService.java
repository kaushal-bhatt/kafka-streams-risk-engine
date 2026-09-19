package com.kaushal.riskengine.query;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.DecisionCounts;
import com.kaushal.riskengine.config.RiskEngineProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.time.Clock;
import java.util.Optional;

/**
 * "How has this merchant been doing?" - answered from the analytics window store on whichever
 * instance owns the merchant.
 *
 * <p>The owner of a merchant is usually a <em>different</em> instance from the owner of any
 * one of its cards: the card stores are partitioned by card, this store by merchant, because the
 * analytics path repartitioned by merchant. The same routing idea, applied per store, sends each
 * query to the right place.
 *
 * <p>Unlike card queries, there's no standby fallback here. A dashboard statistic can wait a
 * few seconds out a rebalance; a card's risk profile is what a live decision may need.
 */
@Service
public class MerchantStatsQueryService {

    private final StreamsBuilderFactoryBean factoryBean;
    private final RemoteQueries remote;
    private final HostInfo self;
    private final Clock clock;

    @Autowired
    public MerchantStatsQueryService(StreamsBuilderFactoryBean factoryBean, RestClient.Builder restClientBuilder,
                                     RiskEngineProperties properties) {
        this.factoryBean = factoryBean;
        this.remote = new RemoteQueries(restClientBuilder.requestFactory(CardRiskQueryService.timeouts()).build());
        this.self = HostInfo.buildFromEndpoint(properties.applicationServer());
        this.clock = Clock.systemUTC();
    }

    public Optional<MerchantStatsView> find(String merchantId, boolean forwarded) {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            throw new StoreNotReadyException("Kafka Streams has not started");
        }

        KeyQueryMetadata owner = streams.queryMetadataForKey(
                Topics.MERCHANT_STATS_STORE, merchantId, Serdes.String().serializer());
        if (owner == null || KeyQueryMetadata.NOT_AVAILABLE.equals(owner) || owner.activeHost().port() < 0) {
            throw new StoreNotReadyException("no owner for this merchant's partition yet - the group is rebalancing");
        }

        if (self.equals(owner.activeHost())) {
            try {
                ReadOnlyWindowStore<String, DecisionCounts> store = streams.store(StoreQueryParameters
                        .fromNameAndType(Topics.MERCHANT_STATS_STORE, QueryableStoreTypes.<String, DecisionCounts>windowStore())
                        .withPartition(owner.partition()));
                return MerchantStatsAssembler.assemble(merchantId, owner.partition(), endpoint(self), clock.instant(), store);
            } catch (InvalidStateStoreException e) {
                throw new StoreNotReadyException("merchant stats for partition " + owner.partition() + " are not queryable yet", e);
            }
        }

        if (forwarded) {
            throw new StoreNotReadyException("partition " + owner.partition() + " is now owned by " + endpoint(owner.activeHost()));
        }

        return remote.get(owner.activeHost(), "/risk/merchants/{merchantId}/stats?local=true",
                        MerchantStatsView.class, merchantId)
                .map(view -> view.withRoutedVia(endpoint(self)));
    }

    private static String endpoint(HostInfo host) {
        return host.host() + ":" + host.port();
    }
}
