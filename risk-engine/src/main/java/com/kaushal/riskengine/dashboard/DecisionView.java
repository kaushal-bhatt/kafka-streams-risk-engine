package com.kaushal.riskengine.dashboard;

import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.query.CardRiskView;

import java.time.Instant;
import java.util.List;

/** A decision as the dashboard sees it. JSON-friendly, unlike the Avro class. */
public record DecisionView(
        String transactionId,
        String cardId,
        String merchantId,
        String decision,
        int score,
        long amountMinor,
        List<CardRiskView.Reason> reasons,
        Instant evaluatedAt
) {

    static DecisionView of(Decision d) {
        return new DecisionView(
                d.getTransactionId(),
                d.getCardId(),
                d.getMerchantId(),
                d.getDecision().name(),
                d.getScore(),
                d.getAmountMinor(),
                d.getReasons().stream()
                        .map(r -> new CardRiskView.Reason(r.getRule(), r.getScore(), r.getDetail()))
                        .toList(),
                d.getEvaluatedAt());
    }
}
