package com.kaushal.trafficgen;

import com.kaushal.riskengine.avro.Card;
import com.kaushal.riskengine.avro.CardStatus;
import com.kaushal.riskengine.avro.Customer;
import com.kaushal.riskengine.avro.Merchant;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Seeds the compacted reference topics.
 *
 * <p>Stage 5 replaces this with Debezium reading Postgres. Until then the engine still
 * needs cards, customers and merchants to exist, because the enrichment join drops any
 * transaction whose card it has never seen.
 */
public final class ReferenceData {

    /** A handful of European cities, so the geo-velocity rule has real distances to work with. */
    public record City(String name, String country, double lat, double lon) {
    }

    public static final List<City> CITIES = List.of(
            new City("Berlin", "DE", 52.5200, 13.4050),
            new City("Munich", "DE", 48.1351, 11.5820),
            new City("Amsterdam", "NL", 52.3676, 4.9041),
            new City("Paris", "FR", 48.8566, 2.3522),
            new City("Madrid", "ES", 40.4168, -3.7038),
            new City("Dublin", "IE", 53.3498, -6.2603),
            new City("Warsaw", "PL", 52.2297, 21.0122),
            new City("Lisbon", "PT", 38.7223, -9.1393)
    );

    /** Category codes the merchant-risk rule treats as elevated. */
    private static final String[] MCCS = {"5411", "5812", "5541", "7995", "6051", "5732", "4121", "5999"};

    private static final String[] MERCHANT_NAMES = {
            "Kaufland", "Bistro Nord", "Shell Station", "Casino Royale", "CoinSwap",
            "ElektroMarkt", "CityCab", "Kiosk 24"
    };

    private ReferenceData() {
    }

    public record Seeded(List<Customer> customers, List<Card> cards, List<Merchant> merchants) {
    }

    public static Seeded generate(long seed, int customerCount, int merchantCount) {
        Random random = new Random(seed);

        List<Customer> customers = new ArrayList<>(customerCount);
        List<Card> cards = new ArrayList<>(customerCount);

        for (int i = 0; i < customerCount; i++) {
            City home = CITIES.get(random.nextInt(CITIES.size()));
            String customerId = "CUST-%04d".formatted(i);

            customers.add(Customer.newBuilder()
                    .setCustomerId(customerId)
                    .setRiskTier(com.kaushal.riskengine.avro.RiskTier.values()[random.nextInt(3)])
                    .setHomeCountry(home.country())
                    .setHomeLat(home.lat())
                    .setHomeLon(home.lon())
                    .build());

            // Limits between €500 and €3000, in minor units. Wide enough that the
            // limit-breach scenario has somewhere to walk to.
            long limitMinor = (50_000L + random.nextInt(26) * 10_000L);

            cards.add(Card.newBuilder()
                    .setCardId("CARD-%04d".formatted(i))
                    .setCustomerId(customerId)
                    .setStatus(CardStatus.ACTIVE)
                    .setDailyLimitMinor(limitMinor)
                    .setCurrency("EUR")
                    .build());
        }

        List<Merchant> merchants = new ArrayList<>(merchantCount);
        for (int i = 0; i < merchantCount; i++) {
            City city = CITIES.get(random.nextInt(CITIES.size()));
            merchants.add(Merchant.newBuilder()
                    .setMerchantId("MERCH-%04d".formatted(i))
                    .setName(MERCHANT_NAMES[i % MERCHANT_NAMES.length] + " " + city.name())
                    .setMcc(MCCS[random.nextInt(MCCS.length)])
                    .setCountry(city.country())
                    // Jitter so merchants are not all stacked on the city centre.
                    .setLat(city.lat() + (random.nextDouble() - 0.5) * 0.1)
                    .setLon(city.lon() + (random.nextDouble() - 0.5) * 0.1)
                    .build());
        }

        return new Seeded(customers, cards, merchants);
    }
}
