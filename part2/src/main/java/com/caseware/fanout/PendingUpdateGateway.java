package com.caseware.fanout;

import java.time.Duration;

public interface PendingUpdateGateway {

    void markPending(
            EngagementRef engagement,
            String templateId,
            String version,
            String idempotencyKey,
            Duration timeout) throws TransientDownstreamException, PermanentDownstreamException;
}
