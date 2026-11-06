package com.caseware.fanout;

import java.time.Duration;

final class ManualNanoClock implements NanoClock {

    private long nanos;

    ManualNanoClock(long startNanos) {
        this.nanos = startNanos;
    }

    @Override
    public long nanos() {
        return nanos;
    }

    void advance(Duration duration) {
        nanos += duration.toNanos();
    }
}
