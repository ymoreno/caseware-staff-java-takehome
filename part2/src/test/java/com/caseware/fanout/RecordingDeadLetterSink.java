package com.caseware.fanout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class RecordingDeadLetterSink implements DeadLetterSink {

    private final List<FailedTask> tasks = Collections.synchronizedList(new ArrayList<>());
    private final List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void send(FailedTask task, Throwable error) {
        tasks.add(task);
        errors.add(error);
    }

    List<FailedTask> tasks() {
        return List.copyOf(tasks);
    }

    List<Throwable> errors() {
        return List.copyOf(errors);
    }
}
