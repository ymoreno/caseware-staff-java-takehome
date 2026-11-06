package com.caseware.fanout;

public interface RateLimiter {

    void acquire() throws InterruptedException;
}
