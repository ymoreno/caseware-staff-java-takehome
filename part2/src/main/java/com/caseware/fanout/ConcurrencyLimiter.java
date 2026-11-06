package com.caseware.fanout;

import java.time.Duration;

public interface ConcurrencyLimiter {

    void acquire() throws InterruptedException;

    boolean tryAcquire(Duration timeout) throws InterruptedException;

    void release();
}
