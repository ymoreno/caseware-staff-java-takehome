package com.caseware.fanout;

public record FailedTask(
        String eventId,
        String firmId,
        String engagementId,
        String templateId,
        String version,
        String region,
        int attempts) {
}
