package com.caseware.fanout;

public class PermanentDownstreamException extends Exception {

    public PermanentDownstreamException(String message) {
        super(message);
    }

    public PermanentDownstreamException(String message, Throwable cause) {
        super(message, cause);
    }
}
