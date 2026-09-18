package com.kaushal.riskengine.query;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.CardProfile;
import com.kaushal.riskengine.avro.CardStatus;
import com.kaushal.riskengine.avro.DailySpend;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.LastSeen;
import com.kaushal.riskengine.avro.RiskTier;
import com.kaushal.riskengine.avro.VelocityEntry;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withException;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * The routing half of Interactive Queries: given Kafka Streams' view of who owns which
 * partition, does each request end up in the right place?
 */
class CardRiskQueryServiceTest {

    private static final HostInfo SELF = new HostInfo("localhost", 8088);
    private static final HostInfo OTHER = new HostInfo("localhost", 8087);
    private static final Instant NOW = Instant.parse("2026-09-18T10:00:00Z");

    private KafkaStreams streams;
    private LocalCardStores localStores;
    private MockRestServiceServer otherInstance;
    private CardRiskQueryService service;

    @BeforeEach
    void setUp() {
        streams = mock(KafkaStreams.class);
        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(streams);
        localStores = mock(LocalCardStores.class);

        RestClient.Builder builder = RestClient.builder();
        otherInstance = MockRestServiceServer.bindTo(builder).build();

        service = new CardRiskQueryService(factoryBean, localStores, builder.build(), SELF,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("a card this instance owns is answered from local state, with no HTTP call")
    void servesLocally() {
        ownedBy(SELF, 1);
        // Built before stubbing: Mockito rejects stubbing another mock inside thenReturn(...).
        CardStores stores = storesWithProfile();
        when(localStores.forPartition(1, false)).thenReturn(stores);

        CardRiskView view = service.find("CARD-7", false, false).orElseThrow();

        assertThat(view.servedBy()).isEqualTo("localhost:8088");
        assertThat(view.routedVia()).isNull();
        assertThat(view.partition()).isEqualTo(1);
        assertThat(view.stale()).isFalse();
        otherInstance.verify(); // no request was expected, none was made
    }

    @Test
    @DisplayName("a card another instance owns is forwarded there, and the answer says so")
    void forwardsToOwner() {
        ownedBy(OTHER, 3);
        otherInstance.expect(requestTo("http://localhost:8087/risk/cards/CARD-7?local=true&stale=false"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("""
                        {"cardId":"CARD-7","servedBy":"localhost:8087","partition":3,
                         "spendToday":{"spentMinor":1200,"remainingMinor":98800}}
                        """, MediaType.APPLICATION_JSON));

        CardRiskView view = service.find("CARD-7", false, false).orElseThrow();

        assertThat(view.servedBy()).isEqualTo("localhost:8087");
        assertThat(view.routedVia()).isEqualTo("localhost:8088");
        assertThat(view.partition()).isEqualTo(3);
        assertThat(view.spendToday().spentMinor()).isEqualTo(1200);
        verify(localStores, never()).forPartition(anyInt(), anyBoolean());
        otherInstance.verify();
    }

    @Test
    @DisplayName("a forwarded request is never forwarded again, even if ownership moved meanwhile")
    void neverForwardsTwice() {
        ownedBy(OTHER, 3);

        assertThatThrownBy(() -> service.find("CARD-7", true, false))
                .isInstanceOf(StoreNotReadyException.class)
                .hasMessageContaining("now owned by localhost:8087");
        otherInstance.verify();
    }

    @Test
    @DisplayName("while the group is rebalancing there is no owner, and the answer is 'retry', not 404")
    void noOwnerDuringRebalance() {
        when(streams.queryMetadataForKey(eq(Topics.CARD_PROFILE_STORE), eq("CARD-7"), any(Serializer.class)))
                .thenReturn(KeyQueryMetadata.NOT_AVAILABLE);

        assertThatThrownBy(() -> service.find("CARD-7", false, false)).isInstanceOf(StoreNotReadyException.class);
    }

    @Test
    @DisplayName("the owner not knowing the card is a 404 here too")
    void ownerSaysNotFound() {
        ownedBy(OTHER, 3);
        otherInstance.expect(requestTo("http://localhost:8087/risk/cards/CARD-7?local=true&stale=false"))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        assertThat(service.find("CARD-7", false, false)).isEmpty();
    }

    @Test
    @DisplayName("owner down, and this instance holds the standby: answered at once, from the standby, marked stale")
    void ownerDownServedFromLocalStandby() {
        ownedBy(OTHER, 3, SELF);
        otherInstance.expect(requestTo("http://localhost:8087/risk/cards/CARD-7?local=true&stale=false"))
                .andRespond(withException(new IOException("connection refused")));
        CardStores stores = storesWithProfile();
        when(localStores.forPartition(3, true)).thenReturn(stores);

        CardRiskView view = service.find("CARD-7", false, false).orElseThrow();

        assertThat(view.stale()).isTrue();
        assertThat(view.servedBy()).isEqualTo("localhost:8088");
        assertThat(view.profile()).isNotNull();
        otherInstance.verify();
    }

    @Test
    @DisplayName("owner down, standby elsewhere: forwarded to the standby, asking for a stale read")
    void ownerDownServedFromRemoteStandby() {
        HostInfo standby = new HostInfo("localhost", 8086);
        ownedBy(OTHER, 3, standby);
        otherInstance.expect(requestTo("http://localhost:8087/risk/cards/CARD-7?local=true&stale=false"))
                .andRespond(withException(new IOException("connection refused")));
        otherInstance.expect(requestTo("http://localhost:8086/risk/cards/CARD-7?local=true&stale=true"))
                .andRespond(withSuccess("""
                        {"cardId":"CARD-7","servedBy":"localhost:8086","partition":3,"stale":true}
                        """, MediaType.APPLICATION_JSON));

        CardRiskView view = service.find("CARD-7", false, false).orElseThrow();

        assertThat(view.stale()).isTrue();
        assertThat(view.servedBy()).isEqualTo("localhost:8086");
        assertThat(view.routedVia()).isEqualTo("localhost:8088");
        otherInstance.verify();
    }

    @Test
    @DisplayName("a standby asked for a stale read serves it; asked for a fresh one, it refuses")
    void standbyOnlyServesStaleWhenAsked() {
        ownedBy(OTHER, 3, SELF);
        CardStores stores = storesWithProfile();
        when(localStores.forPartition(3, true)).thenReturn(stores);

        assertThat(service.find("CARD-7", true, true).orElseThrow().stale()).isTrue();
        assertThatThrownBy(() -> service.find("CARD-7", true, false)).isInstanceOf(StoreNotReadyException.class);
    }

    @Test
    @DisplayName("owner unreachable and no standby anywhere: 'retry shortly', not a guess")
    void ownerUnreachable() {
        ownedBy(OTHER, 3);
        otherInstance.expect(requestTo("http://localhost:8087/risk/cards/CARD-7?local=true&stale=false"))
                .andRespond(withException(new IOException("connection refused")));

        assertThatThrownBy(() -> service.find("CARD-7", false, false))
                .isInstanceOf(StoreNotReadyException.class)
                .hasMessageContaining("unreachable");
    }

    private void ownedBy(HostInfo active, int partition, HostInfo... standbys) {
        when(streams.queryMetadataForKey(eq(Topics.CARD_PROFILE_STORE), eq("CARD-7"), any(Serializer.class)))
                .thenReturn(new KeyQueryMetadata(active, Set.of(standbys), partition));
    }

    @SuppressWarnings("unchecked")
    private static CardStores storesWithProfile() {
        ReadOnlyKeyValueStore<String, CardProfile> profiles = mock(ReadOnlyKeyValueStore.class);
        when(profiles.get("CARD-7")).thenReturn(CardProfile.newBuilder()
                .setCardId("CARD-7").setCustomerId("CUST-7").setStatus(CardStatus.ACTIVE)
                .setDailyLimitMinor(100_000L).setCurrency("EUR").setRiskTier(RiskTier.LOW)
                .setHomeCountry("DE").setHomeLat(52.52).setHomeLon(13.405).build());

        ReadOnlyWindowStore<String, VelocityEntry> velocity = mock(ReadOnlyWindowStore.class);
        when(velocity.fetch(eq("CARD-7"), any(Instant.class), any(Instant.class))).thenReturn(emptyIterator());

        return new CardStores(profiles,
                (ReadOnlyKeyValueStore<String, DailySpend>) mock(ReadOnlyKeyValueStore.class),
                (ReadOnlyKeyValueStore<String, LastSeen>) mock(ReadOnlyKeyValueStore.class),
                velocity,
                (ReadOnlyKeyValueStore<String, Decision>) mock(ReadOnlyKeyValueStore.class));
    }

    private static WindowStoreIterator<VelocityEntry> emptyIterator() {
        return new WindowStoreIterator<>() {
            @Override
            public void close() {
            }

            @Override
            public Long peekNextKey() {
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public KeyValue<Long, VelocityEntry> next() {
                throw new NoSuchElementException();
            }
        };
    }
}
