package com.caseware.fanout;

import java.util.Objects;

public record EngagementRef(String firmId, String engagementId, String region) {

    public EngagementRef {
        Objects.requireNonNull(firmId, "firmId");
        Objects.requireNonNull(engagementId, "engagementId");
        Objects.requireNonNull(region, "region");
    }
}
