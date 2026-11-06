package com.caseware.fanout;

import java.util.Objects;

public final class DlqReplay {

    private final IdempotencyStore idempotencyStore;

    public DlqReplay(IdempotencyStore idempotencyStore) {
        this.idempotencyStore = Objects.requireNonNull(idempotencyStore, "idempotencyStore");
    }

    public boolean replay(TemplatePublished event, FailedTask task, FanOutWorker worker) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(worker, "worker");
        if (!event.eventId().equals(task.eventId())
                || !event.templateId().equals(task.templateId())
                || !event.version().equals(task.version())
                || !event.region().equals(task.region())) {
            throw new IllegalArgumentException("event and failed task do not match");
        }
        release(task);
        return worker.reprocess(event, new EngagementRef(task.firmId(), task.engagementId(), task.region()));
    }

    public void release(FailedTask task) {
        Objects.requireNonNull(task, "task");
        idempotencyStore.release(task.eventId() + ":" + task.engagementId());
    }

    public void release(String eventId, String engagementId) {
        idempotencyStore.release(eventId + ":" + engagementId);
    }
}
