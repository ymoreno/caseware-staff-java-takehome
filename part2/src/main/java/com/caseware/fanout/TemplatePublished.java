package com.caseware.fanout;

import java.time.Instant;
import java.util.Objects;

public record TemplatePublished(
        String eventId,
        String templateId,
        String version,
        String region,
        Instant publishedAt) {

    public TemplatePublished {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(templateId, "templateId");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(region, "region");
        Objects.requireNonNull(publishedAt, "publishedAt");
    }
}
