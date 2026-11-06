package com.caseware.fanout;

import java.time.Duration;

@FunctionalInterface
public interface Sleeper {

    void sleep(Duration duration) throws InterruptedException;

    static Sleeper system() {
        return duration -> {
            long millis = duration.toMillis();
            int nanos = (int) (duration.minusMillis(millis).toNanos());
            Thread.sleep(millis, nanos);
        };
    }
}
