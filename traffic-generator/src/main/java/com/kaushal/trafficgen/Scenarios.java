package com.kaushal.trafficgen;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Card;
import com.kaushal.riskengine.avro.Merchant;
import com.kaushal.riskengine.avro.Transaction;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Scripted traffic. Every scenario is seeded, so the same command produces the same
 * decisions every time - which is what makes the dashboard demo repeatable and the
 * recorded GIF honest.
 */
public class Scenarios {

    private final Config config;
    private final Publisher publisher;
    private final ReferenceData.Seeded reference;

    public Scenarios(Config config, Publisher publisher, ReferenceData.Seeded reference) {
        this.config = config;
        this.publisher = publisher;
        this.reference = reference;
    }

    /** Publishes the compacted reference topics. Stage 5 hands this job to Debezium. */
    public void seed() {
        reference.customers().forEach(c -> publisher.send(Topics.CUSTOMERS, c.getCustomerId(), c));
        reference.cards().forEach(c -> publisher.send(Topics.CARDS, c.getCardId(), c));
        reference.merchants().forEach(m -> publisher.send(Topics.MERCHANTS, m.getMerchantId(), m));
        publisher.flush();
        System.out.printf("seeded %d customers, %d cards, %d merchants%n",
                reference.customers().size(), reference.cards().size(), reference.merchants().size());
    }

    /**
     * Ordinary traffic. Nothing here should trip a rule; it exists so the dashboard has a
     * baseline for the fraud scenarios to stand out against.
     */
    public void baseline(int count) throws InterruptedException {
        Random random = new Random(config.seed());
        List<Card> cards = reference.cards();
        List<Merchant> merchants = reference.merchants();

        for (int i = 0; i < count; i++) {
            Card card = cards.get(random.nextInt(cards.size()));
            Merchant merchant = merchants.get(random.nextInt(merchants.size()));
            // Between EUR 3 and EUR 120.
            long amountMinor = 300 + random.nextInt(11_700);

            emit(card.getCardId(), merchant, amountMinor, Instant.now());

            // Poisson-ish arrivals: exponential inter-arrival gaps around 400ms.
            long gapMillis = (long) (-Math.log(1 - random.nextDouble()) * 400);
            Thread.sleep(config.pauseFor(gapMillis));
        }
        publisher.flush();
        System.out.println("baseline: sent " + count + " transactions");
    }

    /**
     * A bot checking which stolen card numbers are still live: many tiny authorisations,
     * spread across merchants so no single merchant sees a pattern.
     */
    public void cardTesting() throws InterruptedException {
        Card card = reference.cards().get(7);
        List<Merchant> merchants = reference.merchants();
        Random random = new Random(config.seed());

        System.out.println("card-testing: hammering " + card.getCardId());
        for (int i = 0; i < 20; i++) {
            Merchant merchant = merchants.get(i % Math.min(8, merchants.size()));
            // Under EUR 2 - the classic card-testing signature.
            long amountMinor = 20 + random.nextInt(160);
            emit(card.getCardId(), merchant, amountMinor, Instant.now());
            Thread.sleep(config.pauseFor(Duration.ofSeconds(9).toMillis()));
        }
        publisher.flush();
    }

    /**
     * Two authorisations that no single cardholder could have made: Berlin, then Sao Paulo
     * four minutes later.
     *
     * <p>The remote merchant is published as part of the scenario, because the seeded
     * reference data is European only. This is also the rule the Sparkov replay cannot
     * exercise - that dataset places merchants near the cardholder, so natural
     * impossible-travel events effectively do not occur in it.
     */
    public void impossibleTravel() throws InterruptedException {
        Card card = reference.cards().get(3);
        Merchant home = reference.merchants().get(0);

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
        System.out.printf("impossible-travel: %s in %s%n", card.getCardId(), home.getCountry());

        Thread.sleep(config.pauseFor(gap.toMillis()));

        Instant second = first.plus(gap);
        emit(card.getCardId(), remote, 8_900L, second);

        double km = Geo.distanceKm(home.getLat(), home.getLon(), remote.getLat(), remote.getLon());
        double kmh = Geo.impliedKmh(km, Duration.ofMinutes(4).toMillis());
        System.out.printf("impossible-travel: then %s - %.0f km in 4 min, implies %.0f km/h%n",
                remote.getCountry(), km, kmh);

        publisher.flush();
    }

    /** Walks a card up to its daily limit and one transaction past it. */
    public void limitBreach() throws InterruptedException {
        Card card = reference.cards().get(11);
        List<Merchant> merchants = reference.merchants();
        long limit = card.getDailyLimitMinor();

        System.out.printf("limit-breach: %s has a daily limit of %d minor units%n",
                card.getCardId(), limit);

        long spent = 0;
        int i = 0;
        // Five roughly equal steps to just under the limit, then one that crosses it.
        long step = limit / 5;
        while (spent + step < limit) {
            emit(card.getCardId(), merchants.get(i++ % merchants.size()), step, Instant.now());
            spent += step;
            Thread.sleep(config.pauseFor(Duration.ofSeconds(20).toMillis()));
        }

        long overshoot = (limit - spent) + 5_000L;
        emit(card.getCardId(), merchants.get(i % merchants.size()), overshoot, Instant.now());
        System.out.printf("limit-breach: spent %d, then attempted %d%n", spent, overshoot);

        publisher.flush();
    }

    private void emit(String cardId, Merchant merchant, long amountMinor, Instant eventTime) {
        Transaction transaction = Transaction.newBuilder()
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
                .build();

        publisher.send(Topics.TRANSACTIONS, cardId, transaction);
    }
}
