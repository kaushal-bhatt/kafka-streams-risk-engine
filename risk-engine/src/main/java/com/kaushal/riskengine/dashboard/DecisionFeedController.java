package com.kaushal.riskengine.dashboard;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
public class DecisionFeedController {

    private final DecisionFeed feed;

    public DecisionFeedController(DecisionFeed feed) {
        this.feed = feed;
    }

    /** Server-sent events: one {@code snapshot} on connect, then one {@code decision} each. */
    @GetMapping(path = "/risk/decisions/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return feed.subscribe();
    }
}
