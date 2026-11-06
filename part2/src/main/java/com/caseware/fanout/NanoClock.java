package com.caseware.fanout;

@FunctionalInterface
public interface NanoClock {

    long nanos();

    static NanoClock system() {
        return System::nanoTime;
    }
}
