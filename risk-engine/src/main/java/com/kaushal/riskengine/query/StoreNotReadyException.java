package com.kaushal.riskengine.query;

/**
 * The stores can't answer right now - usually because a rebalance is moving partitions
 * between instances. The right response is "try again in a moment", not an error page.
 */
public class StoreNotReadyException extends RuntimeException {

    public StoreNotReadyException(String message) {
        super(message);
    }

    public StoreNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }
}
