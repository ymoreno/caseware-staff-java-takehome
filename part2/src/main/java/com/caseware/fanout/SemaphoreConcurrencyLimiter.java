package com.caseware.fanout;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public final class SemaphoreConcurrencyLimiter implements ConcurrencyLimiter {

    private final Semaphore semaphore;

    public SemaphoreConcurrencyLimiter(int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be greater than zero");
        }
        this.semaphore = new Semaphore(permits);
    }

    @Override
    public void acquire() throws InterruptedException {
        semaphore.acquire();
    }

    @Override
    public boolean tryAcquire(Duration timeout) throws InterruptedException {
        return semaphore.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void release() {
        semaphore.release();
    }
}
