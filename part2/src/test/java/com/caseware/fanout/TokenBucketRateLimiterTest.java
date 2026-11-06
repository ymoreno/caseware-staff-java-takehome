package com.caseware.fanout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class TokenBucketRateLimiterTest {

    @Test
    void allowsBurstUpToCapacity() throws InterruptedException {
        ManualNanoClock clock = new ManualNanoClock(0);
        FakeSleeper sleeper = new FakeSleeper(clock);
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1.0, 5.0, clock, sleeper);

        for (int i = 0; i < 5; i++) {
            limiter.acquire();
        }

        assertEquals(Duration.ZERO, sleeper.total());
    }

    @Test
    void waitsWhenPermitsAreExhausted() throws InterruptedException {
        ManualNanoClock clock = new ManualNanoClock(0);
        FakeSleeper sleeper = new FakeSleeper(clock);
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(2.0, 2.0, clock, sleeper);

        limiter.acquire();
        limiter.acquire();
        limiter.acquire();

        assertEquals(Duration.ofMillis(500), sleeper.total());
    }

    @Test
    void enforcesConfiguredRateOverTime() throws InterruptedException {
        ManualNanoClock clock = new ManualNanoClock(0);
        FakeSleeper sleeper = new FakeSleeper(clock);
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(4.0, 1.0, clock, sleeper);

        for (int i = 0; i < 5; i++) {
            limiter.acquire();
        }

        assertEquals(Duration.ofNanos(1_000_000_000L), sleeper.total());
    }

    @Test
    void rejectsInvalidConfiguration() {
        ManualNanoClock clock = new ManualNanoClock(0);
        FakeSleeper sleeper = new FakeSleeper(clock);

        assertThrows(IllegalArgumentException.class,
                () -> new TokenBucketRateLimiter(0, 1, clock, sleeper));
        assertThrows(IllegalArgumentException.class,
                () -> new TokenBucketRateLimiter(1, 0, clock, sleeper));
    }
}
