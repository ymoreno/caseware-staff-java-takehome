package com.caseware.fanout;

public class TransientDownstreamException extends Exception {

    public TransientDownstreamException(String message) {
        super(message);
    }

    public TransientDownstreamException(String message, Throwable cause) {
        super(message, cause);
    }
}
