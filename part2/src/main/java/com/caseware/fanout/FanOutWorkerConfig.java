package com.caseware.fanout;

import java.time.Duration;
import java.util.Objects;

public record FanOutWorkerConfig(
        int pageSize,
        int maxInFlight,
        int maxAttempts,
        Duration initialBackoff,
        Duration maxBackoff,
        Duration downstreamTimeout,
        Duration shutdownTimeout) {

    public FanOutWorkerConfig {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize must be greater than zero");
        }
        if (maxInFlight <= 0) {
            throw new IllegalArgumentException("maxInFlight must be greater than zero");
        }
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be greater than zero");
        }
        Objects.requireNonNull(initialBackoff, "initialBackoff");
        Objects.requireNonNull(maxBackoff, "maxBackoff");
        Objects.requireNonNull(downstreamTimeout, "downstreamTimeout");
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
        if (initialBackoff.isNegative()) {
            throw new IllegalArgumentException("initialBackoff must not be negative");
        }
        if (maxBackoff.compareTo(initialBackoff) < 0) {
            throw new IllegalArgumentException("maxBackoff must not be shorter than initialBackoff");
        }
        if (downstreamTimeout.isNegative() || downstreamTimeout.isZero()) {
            throw new IllegalArgumentException("downstreamTimeout must be positive");
        }
        if (shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("shutdownTimeout must not be negative");
        }
    }

    public static FanOutWorkerConfig defaults() {
        return new FanOutWorkerConfig(
                200,
                56,
                3,
                Duration.ofMillis(200),
                Duration.ofSeconds(5),
                Duration.ofSeconds(90),
                Duration.ofSeconds(30));
    }
}
