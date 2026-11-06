package com.caseware.fanout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

final class ScriptedPendingUpdateGateway implements PendingUpdateGateway {

    private final Map<String, Deque<Exception>> failures = new ConcurrentHashMap<>();
    private final List<EngagementRef> calls = Collections.synchronizedList(new ArrayList<>());
    private final List<String> keys = Collections.synchronizedList(new ArrayList<>());
    private final List<Duration> timeouts = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlight = new AtomicInteger();
    private final AtomicInteger completed = new AtomicInteger();

    private volatile CountDownLatch blockLatch = new CountDownLatch(0);
    private volatile CountDownLatch enteredLatch = new CountDownLatch(0);

    void failNext(String engagementId, Exception failure) {
        failures.computeIfAbsent(engagementId, key -> new ConcurrentLinkedDeque<>()).add(failure);
    }

    void failTimes(String engagementId, int times, Supplier<Exception> factory) {
        for (int i = 0; i < times; i++) {
            failNext(engagementId, factory.get());
        }
    }

    void blockUntil(CountDownLatch blockLatch, CountDownLatch enteredLatch) {
        this.blockLatch = blockLatch;
        this.enteredLatch = enteredLatch;
    }

    @Override
    public void markPending(EngagementRef engagement, String templateId, String version,
            String idempotencyKey, Duration timeout)
            throws TransientDownstreamException, PermanentDownstreamException {
        calls.add(engagement);
        keys.add(idempotencyKey);
        timeouts.add(timeout);
        int current = inFlight.incrementAndGet();
        maxInFlight.accumulateAndGet(current, Math::max);
        enteredLatch.countDown();
        try {
            blockLatch.await();
            Deque<Exception> queue = failures.get(engagement.engagementId());
            if (queue != null) {
                Exception failure = queue.poll();
                if (failure instanceof TransientDownstreamException transientFailure) {
                    throw transientFailure;
                }
                if (failure instanceof PermanentDownstreamException permanentFailure) {
                    throw permanentFailure;
                }
                if (failure instanceof RuntimeException runtimeFailure) {
                    throw runtimeFailure;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            inFlight.decrementAndGet();
            completed.incrementAndGet();
        }
    }

    int calls() {
        return calls.size();
    }

    List<EngagementRef> callList() {
        return List.copyOf(calls);
    }

    int maxInFlight() {
        return maxInFlight.get();
    }

    int completed() {
        return completed.get();
    }

    String lastKey() {
        return keys.get(keys.size() - 1);
    }

    Duration lastTimeout() {
        return timeouts.get(timeouts.size() - 1);
    }

    boolean allCompleted() {
        return inFlight.get() == 0;
    }
}
