package com.kaushal.riskengine;

/**
 * Topic names and state store names in one place, so a rename is one edit rather than a
 * grep across the topology.
 *
 * <p>Every topic on the join path is created with the same partition count - see
 * {@code docker/create-topics.sh}. A KStream-KTable join requires co-partitioning, and
 * Kafka Streams does not warn when partition counts differ. It just never matches.
 */
public final class Topics {

    private Topics() {
    }

    public static final String TRANSACTIONS = "payments.transactions.v1";
    public static final String CARDS = "payments.cards.v1";
    public static final String CUSTOMERS = "payments.customers.v1";
    public static final String MERCHANTS = "payments.merchants.v1";

    /** Stage 1 only: lets you see the enrichment working in kafka-ui before rules exist. */
    public static final String ENRICHED = "payments.enriched.v1";

    public static final String DECISIONS = "payments.decisions.v1";
    public static final String MERCHANT_STATS = "risk.merchant-stats.v1";
    public static final String TRANSACTIONS_DLQ = "payments.transactions.dlq.v1";

    /** Queryable via Interactive Queries from stage 3. */
    public static final String CARD_PROFILE_STORE = "card-profile-store";
    public static final String MERCHANT_STORE = "merchant-store";
    public static final String CARDS_STORE = "cards-store";
    public static final String CUSTOMERS_STORE = "customers-store";
}
