package com.caseware.fanout;

import java.time.Duration;
import java.util.Map;

public interface Metrics {

    void increment(String name, Map<String, String> tags);

    void timing(String name, Duration duration, Map<String, String> tags);
}
