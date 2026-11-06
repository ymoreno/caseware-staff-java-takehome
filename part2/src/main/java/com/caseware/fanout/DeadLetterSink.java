package com.caseware.fanout;

public interface DeadLetterSink {

    void send(FailedTask task, Throwable error);
}
