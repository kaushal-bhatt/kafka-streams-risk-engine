package com.kaushal.riskengine.query;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Interactive Queries over HTTP. No database behind this - every answer comes out of the
 * engine's own state stores, on whichever instance owns the card.
 */
@RestController
@RequestMapping("/risk")
public class RiskQueryController {

    private final CardRiskQueryService queryService;
    private final MerchantStatsQueryService merchantStats;

    public RiskQueryController(CardRiskQueryService queryService, MerchantStatsQueryService merchantStats) {
        this.queryService = queryService;
        this.merchantStats = merchantStats;
    }

    /** A merchant's decision counts per 5-minute window: the most recent hour the engine holds. */
    @GetMapping("/merchants/{merchantId}/stats")
    public ResponseEntity<MerchantStatsView> merchant(@PathVariable String merchantId,
                                                      @RequestParam(defaultValue = "false") boolean local) {
        return merchantStats.find(merchantId, local)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/cards/{cardId}")
    public ResponseEntity<CardRiskView> card(@PathVariable String cardId,
                                             @RequestParam(defaultValue = "false") boolean local,
                                             @RequestParam(defaultValue = "false") boolean stale) {
        return queryService.find(cardId, local, stale)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/instances")
    public List<CardRiskQueryService.InstanceView> instances() {
        return queryService.instances();
    }

    /** A rebalance is not an error. Tell the caller to retry, and when. */
    @ExceptionHandler(StoreNotReadyException.class)
    public ResponseEntity<Map<String, String>> notReady(StoreNotReadyException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(Map.of("error", "not ready", "message", e.getMessage()));
    }
}
