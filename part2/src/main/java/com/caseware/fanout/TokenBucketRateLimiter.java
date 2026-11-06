package com.caseware.fanout;

import java.time.Duration;

public final class TokenBucketRateLimiter implements RateLimiter {

    private final double permitsPerSecond;
    private final double capacity;
    private final NanoClock clock;
    private final Sleeper sleeper;

    private double tokens;
    private long lastNanos;

    public TokenBucketRateLimiter(double permitsPerSecond, double capacity) {
        this(permitsPerSecond, capacity, NanoClock.system(), Sleeper.system());
    }

    public TokenBucketRateLimiter(double permitsPerSecond, double capacity, NanoClock clock, Sleeper sleeper) {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be greater than zero");
        }
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be at least one");
        }
        this.permitsPerSecond = permitsPerSecond;
        this.capacity = capacity;
        this.clock = clock;
        this.sleeper = sleeper;
        this.tokens = capacity;
        this.lastNanos = clock.nanos();
    }

    @Override
    public synchronized void acquire() throws InterruptedException {
        while (true) {
            long now = clock.nanos();
            long elapsed = now - lastNanos;
            if (elapsed > 0) {
                tokens = Math.min(capacity, tokens + (elapsed / 1_000_000_000.0) * permitsPerSecond);
                lastNanos = now;
            }
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return;
            }
            double missing = 1.0 - tokens;
            long waitNanos = (long) Math.ceil((missing / permitsPerSecond) * 1_000_000_000.0);
            if (waitNanos <= 0) {
                waitNanos = 1;
            }
            sleeper.sleep(Duration.ofNanos(waitNanos));
        }
    }
}
