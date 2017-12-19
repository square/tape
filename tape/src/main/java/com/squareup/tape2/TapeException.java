package com.squareup.tape2;

/** Thrown when an internal error has occurred in tape lib **/
public class TapeException extends RuntimeException {

    private static final long serialVersionUID = 5138225684096988531L;

    public TapeException(String message, Exception cause) {
        super(message, cause);
    }

}
