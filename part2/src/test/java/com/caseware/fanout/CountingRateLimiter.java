package com.caseware.fanout;

import java.util.concurrent.atomic.AtomicInteger;

final class CountingRateLimiter implements RateLimiter {

    private final AtomicInteger acquisitions = new AtomicInteger();

    @Override
    public void acquire() {
        acquisitions.incrementAndGet();
    }

    int acquisitions() {
        return acquisitions.get();
    }
}
