package com.kaushal.riskengine.query;

import org.apache.kafka.streams.state.HostInfo;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.util.Optional;

/**
 * Forwards an Interactive Query to the instance that owns the key, and translates its answer:
 * 404 becomes empty, 503 or an unreachable host becomes {@link StoreNotReadyException}
 * ("retry shortly"), anything else unexpected is an error.
 */
final class RemoteQueries {

    private final RestClient restClient;

    RemoteQueries(RestClient restClient) {
        this.restClient = restClient;
    }

    /**
     * @param pathTemplate a URI template for the path and query, e.g.
     *                     {@code "/risk/cards/{id}?local=true"}; variables are URL-encoded,
     *                     so ids containing spaces (Sparkov merchant names) are safe
     */
    <T> Optional<T> get(HostInfo host, String pathTemplate, Class<T> type, Object... vars) {
        Object[] all = new Object[vars.length + 2];
        all[0] = host.host();
        all[1] = host.port();
        System.arraycopy(vars, 0, all, 2, vars.length);
        String where = host.host() + ":" + host.port();
        try {
            return restClient.get()
                    .uri("http://{host}:{port}" + pathTemplate, all)
                    .exchange((request, response) -> {
                        int status = response.getStatusCode().value();
                        if (status == 404) {
                            return Optional.<T>empty();
                        }
                        if (status == 503) {
                            throw new StoreNotReadyException("owner " + where + " is not ready");
                        }
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            throw new IllegalStateException("owner " + where + " answered " + status);
                        }
                        return Optional.ofNullable(response.bodyTo(type));
                    });
        } catch (ResourceAccessException e) {
            // The owner is down. Kafka will notice and move its partitions once its session
            // times out; until then the honest answer is "try again shortly".
            throw new StoreNotReadyException("owner " + where + " is unreachable", e);
        }
    }
}
