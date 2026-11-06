package com.caseware.fanout;

import java.util.Optional;

public interface CheckpointStore {

    Optional<String> load(String eventId, String region);

    void save(String eventId, String region, String cursor);
}
