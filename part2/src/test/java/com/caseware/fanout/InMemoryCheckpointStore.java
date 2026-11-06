package com.caseware.fanout;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

final class InMemoryCheckpointStore implements CheckpointStore {

    private final Map<String, String> cursors = new ConcurrentHashMap<>();

    @Override
    public Optional<String> load(String eventId, String region) {
        return Optional.ofNullable(cursors.get(key(eventId, region)));
    }

    @Override
    public void save(String eventId, String region, String cursor) {
        cursors.put(key(eventId, region), cursor);
    }

    private static String key(String eventId, String region) {
        return eventId + ":" + region;
    }
}
