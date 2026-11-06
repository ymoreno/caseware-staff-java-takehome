package com.caseware.fanout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

final class FakeSleeper implements Sleeper {

    private final ManualNanoClock clock;
    private final List<Duration> sleeps = new ArrayList<>();

    FakeSleeper(ManualNanoClock clock) {
        this.clock = clock;
    }

    @Override
    public void sleep(Duration duration) {
        sleeps.add(duration);
        clock.advance(duration);
    }

    Duration total() {
        return sleeps.stream().reduce(Duration.ZERO, Duration::plus);
    }

    List<Duration> sleeps() {
        return List.copyOf(sleeps);
    }
}
