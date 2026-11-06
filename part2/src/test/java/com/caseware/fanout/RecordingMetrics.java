package com.caseware.fanout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class RecordingMetrics implements Metrics {

    private final Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();
    private final Map<String, List<Duration>> timings = new ConcurrentHashMap<>();

    @Override
    public void increment(String name, Map<String, String> tags) {
        counters.computeIfAbsent(name, key -> new AtomicInteger()).incrementAndGet();
    }

    @Override
    public void timing(String name, Duration duration, Map<String, String> tags) {
        timings.computeIfAbsent(name, key -> Collections.synchronizedList(new ArrayList<>())).add(duration);
    }

    int count(String name) {
        AtomicInteger counter = counters.get(name);
        return counter == null ? 0 : counter.get();
    }

    int timingCount(String name) {
        List<Duration> values = timings.get(name);
        return values == null ? 0 : values.size();
    }
}
