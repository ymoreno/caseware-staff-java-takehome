package com.caseware.fanout;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class InMemoryIdempotencyStore implements IdempotencyStore {

    private final Set<String> keys = ConcurrentHashMap.newKeySet();

    @Override
    public boolean claim(String key) {
        return keys.add(key);
    }

    @Override
    public void release(String key) {
        keys.remove(key);
    }

    boolean contains(String key) {
        return keys.contains(key);
    }
}
