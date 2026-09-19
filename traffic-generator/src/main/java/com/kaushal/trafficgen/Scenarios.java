package com.kaushal.trafficgen;

import com.kaushal.riskengine.Geo;
import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Card;
import com.kaushal.riskengine.avro.Customer;
import com.kaushal.riskengine.avro.Merchant;
import com.kaushal.riskengine.avro.Transaction;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Scripted traffic. Every scenario is seeded, so the same command produces the same
 * decisions every time - which is what makes the demo repeatable and the recorded GIF honest.
 *
 * <p>Each card shops near its customer's home. That matters since stage 2: the engine checks
 * impossible travel, so a generator that sends one card to Berlin and then Lisbon a minute
 * later is generating fraud whether it means to or not.
 */
public class Scenarios {

    /** How long to let the engine build card profiles before sending transactions. */
    private static final long REFERENCE_SETTLE_MILLIS = 3_000;

    /** A merchant within this distance of the customer's home counts as local. */
    private static final double LOCAL_RADIUS_KM = 50;

    private final Config config;
    private final Publisher publisher;
    private final ReferenceData.Seeded reference;
    private final Map<String, Customer> customersById;
    private final Map<String, List<Merchant>> localMerchantsByCard = new HashMap<>();

    public Scenarios(Config config, Publisher publisher, ReferenceData.Seeded reference) {
        this.config = config;
        this.publisher = publisher;
        this.reference = reference;
        this.customersById = reference.customers().stream()
                .collect(Collectors.toMap(Customer::getCustomerId, Function.identity()));
    }

    /** Publishes the compacted reference topics. Stage 5 hands this job to Debezium. */
    public void seed() throws InterruptedException {
        reference.customers().forEach(c -> publisher.send(Topics.CUSTOMERS, c.getCustomerId(), c));
        reference.cards().forEach(c -> publisher.send(Topics.CARDS, c.getCardId(), c));
        reference.merchants().forEach(m -> publisher.send(Topics.MERCHANTS, m.getMerchantId(), m));
        publisher.flush();
        System.out.printf("seeded %d customers, %d cards, %d merchants%n",
                reference.customers().size(), reference.cards().size(), reference.merchants().size());

        // A card profile is built by a foreign-key join that makes a round trip through two
        // internal topics. Sub-second with caching disabled on the reference stores, but a
        // transaction sent in the same instant can still beat it. A short pause keeps the
        // demo deterministic.
        Thread.sleep(REFERENCE_SETTLE_MILLIS);
    }

    /**
     * Ordinary traffic. Should approve almost everything; the only reasons expected are
     * MERCHANT_RISK on the odd casino or crypto purchase.
     */
    public void baseline(int count) throws InterruptedException {
        Random random = new Random(config.seed());
        List<Card> cards = reference.cards();

        for (int i = 0; i < count; i++) {
            Card card = cards.get(random.nextInt(cards.size()));
            List<Merchant> local = localMerchants(card);
            Merchant merchant = local.get(random.nextInt(local.size()));
            // Between EUR 3 and EUR 120.
            long amountMinor = 300 + random.nextInt(11_700);

            emit(card.getCardId(), merchant, amountMinor, Instant.now());

            // Poisson-ish arrivals: exponential inter-arrival gaps around 400ms.
            long gapMillis = (long) (-Math.log(1 - random.nextDouble()) * 400);
            Thread.sleep(config.pauseFor(gapMillis));
        }
        publisher.flush();
        System.out.println("baseline: sent " + count + " transactions");
        System.out.println("expect:   almost all APPROVE; MERCHANT_RISK noted on casino/crypto merchants");
    }

    /**
     * A bot checking which stolen card numbers are still live: many tiny authorisations,
     * spread across merchants so no single merchant sees a pattern.
     */
    public void cardTesting() throws InterruptedException {
        Card card = reference.cards().get(7);
        List<Merchant> local = localMerchants(card);
        List<Merchant> rotation = local.subList(0, Math.min(8, local.size()));
        Random random = new Random(config.seed());

        System.out.printf("card-testing: 20 authorisations under EUR 2 on %s across %d merchants%n",
                card.getCardId(), rotation.size());
        for (int i = 0; i < 20; i++) {
            Merchant merchant = rotation.get(i % rotation.size());
            long amountMinor = 20 + random.nextInt(160);
            emit(card.getCardId(), merchant, amountMinor, Instant.now());
            Thread.sleep(config.pauseFor(Duration.ofSeconds(9).toMillis()));
        }
        publisher.flush();
        System.out.println("expect:   first 3 APPROVE, then REVIEW (CARD_TESTING), then DECLINE once VELOCITY joins in");
    }

    /**
     * Two authorisations that no single cardholder could have made: home, then Sao Paulo
     * four minutes later.
     *
     * <p>The remote merchant is published as part of the scenario, because the seeded
     * reference data is European only. This is also the rule the Sparkov replay cannot
     * exercise - that dataset places merchants near the cardholder, so natural
     * impossible-travel events effectively do not occur in it.
     */
    public void impossibleTravel() throws InterruptedException {
        Card card = reference.cards().get(3);
        Merchant home = localMerchants(card).get(0);

        Merchant remote = Merchant.newBuilder()
                .setMerchantId("MERCH-SAOPAULO")
                .setName("Mercado Central Sao Paulo")
                .setMcc("5411")
                .setCountry("BR")
                .setLat(-23.5505)
                .setLon(-46.6333)
                .build();
        publisher.send(Topics.MERCHANTS, remote.getMerchantId(), remote);
        publisher.flush();
        // Give the GlobalKTable a moment to pick the merchant up before the transaction
        // that references it arrives.
        Thread.sleep(1_000);

        // The four-minute gap is in *event time* and must not depend on --speed. The first
        // authorisation is backdated by four minutes, so the gap is exact and neither event
        // lands in the future; --speed only shortens the wall-clock pause for the demo.
        Duration gap = Duration.ofMinutes(4);
        Instant first = Instant.now().minus(gap);
        emit(card.getCardId(), home, 3_450L, first);
        System.out.printf("impossible-travel: %s at %s (%s)%n", card.getCardId(), home.getName(), home.getCountry());

        Thread.sleep(config.pauseFor(gap.toMillis()));

        Instant second = first.plus(gap);
        // The fraudster's phone, not the cardholder's.
        emit(card.getCardId(), remote, 8_900L, second, "device-unrecognised-" + Integer.toHexString(remote.hashCode()));

        double km = Geo.distanceKm(home.getLat(), home.getLon(), remote.getLat(), remote.getLon());
        double kmh = Geo.impliedKmh(km, gap.toMillis());
        System.out.printf("impossible-travel: then %s - %.0f km in 4 min, implies %.0f km/h%n",
                remote.getCountry(), km, kmh);

        publisher.flush();
        System.out.println("expect:   first APPROVE, second DECLINE (GEO_VELOCITY)");
    }

    /** Walks a card up to its daily limit and one transaction past it. */
    public void limitBreach() throws InterruptedException {
        Card card = reference.cards().get(11);
        List<Merchant> local = localMerchants(card);
        long limit = card.getDailyLimitMinor();

        System.out.printf("limit-breach: %s has a daily limit of EUR %d.%02d%n",
                card.getCardId(), limit / 100, limit % 100);

        long spent = 0;
        int i = 0;
        // Four steps of a fifth of the limit, then one that crosses it.
        long step = limit / 5;
        while (spent + step < limit) {
            emit(card.getCardId(), local.get(i++ % local.size()), step, Instant.now());
            spent += step;
            Thread.sleep(config.pauseFor(Duration.ofSeconds(20).toMillis()));
        }

        long overshoot = (limit - spent) + 5_000L;
        emit(card.getCardId(), local.get(i % local.size()), overshoot, Instant.now());
        System.out.printf("limit-breach: spent EUR %d.%02d, then attempted EUR %d.%02d%n",
                spent / 100, spent % 100, overshoot / 100, overshoot % 100);

        publisher.flush();
        System.out.println("expect:   4 APPROVE, then DECLINE (DAILY_LIMIT)");
    }

    /**
     * Merchants within {@link #LOCAL_RADIUS_KM} of the customer's home, nearest first.
     * Falls back to the three nearest merchants overall if too few are local.
     */
    private List<Merchant> localMerchants(Card card) {
        return localMerchantsByCard.computeIfAbsent(card.getCardId(), id -> {
            Customer customer = customersById.get(card.getCustomerId());
            Comparator<Merchant> byDistance = Comparator.comparingDouble(m ->
                    Geo.distanceKm(customer.getHomeLat(), customer.getHomeLon(), m.getLat(), m.getLon()));

            List<Merchant> sorted = reference.merchants().stream().sorted(byDistance).toList();
            List<Merchant> local = sorted.stream()
                    .filter(m -> Geo.distanceKm(customer.getHomeLat(), customer.getHomeLon(),
                            m.getLat(), m.getLon()) <= LOCAL_RADIUS_KM)
                    .toList();
            return local.size() >= 3 ? local : sorted.subList(0, 3);
        });
    }

    /**
     * Sends transactions at a steady rate and times each one from send to its decision being
     * readable by a {@code read_committed} consumer - the latency a downstream system actually
     * sees. Run it once with the engine on {@code exactly_once_v2} and once on
     * {@code at_least_once} to measure what exactly-once costs.
     */
    public void latency(int count) throws InterruptedException {
        try (LatencyProbe probe = new LatencyProbe(config)) {
            probe.start();
            Random random = new Random(config.seed());
            List<Card> cards = reference.cards();
            for (int i = 0; i < count; i++) {
                Card card = cards.get(random.nextInt(cards.size()));
                List<Merchant> local = localMerchants(card);
                Transaction sent = transaction(card.getCardId(), local.get(random.nextInt(local.size())),
                        1_000 + random.nextInt(4_000), Instant.now(), device(card.getCardId()));
                probe.sent(sent.getTransactionId());
                publisher.send(Topics.TRANSACTIONS, card.getCardId(), sent);
                // 20 per second: a steady load, not a burst the broker has to queue.
                Thread.sleep(50);
            }
            publisher.flush();
            probe.awaitAndReport(Duration.ofSeconds(30));
        }
    }

    private void emit(String cardId, Merchant merchant, long amountMinor, Instant eventTime) {
        emit(cardId, merchant, amountMinor, eventTime, device(cardId));
    }

    private void emit(String cardId, Merchant merchant, long amountMinor, Instant eventTime, String device) {
        publisher.send(Topics.TRANSACTIONS, cardId, transaction(cardId, merchant, amountMinor, eventTime, device));
    }

    /** The cardholder's own device: stable per card, so a future rule can spot a new one. */
    private static String device(String cardId) {
        return "device-" + Integer.toHexString(cardId.hashCode());
    }

    private static Transaction transaction(String cardId, Merchant merchant, long amountMinor, Instant eventTime,
                                           String device) {
        return Transaction.newBuilder()
                .setTransactionId(UUID.randomUUID().toString())
                .setCardId(cardId)
                .setMerchantId(merchant.getMerchantId())
                .setAmountMinor(amountMinor)
                .setCurrency("EUR")
                .setEventTime(eventTime)
                // The transaction happens where the merchant is.
                .setLat(merchant.getLat())
                .setLon(merchant.getLon())
                .setCategory(merchant.getMcc())
                .setDeviceFingerprint(device)
                .build();
    }
}
