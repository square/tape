package com.squareup.tape2;

/** Thrown when an internal error has occurred in tape lib **/
public class TapeException extends RuntimeException {

    public TapeException(String message, Exception cause) {
        super(message, cause);
    }

}
