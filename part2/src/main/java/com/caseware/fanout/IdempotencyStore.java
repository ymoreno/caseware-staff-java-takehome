package com.caseware.fanout;

public interface IdempotencyStore {

    boolean claim(String key);

    void release(String key);
}
