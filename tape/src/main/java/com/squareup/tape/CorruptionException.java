package com.squareup.tape;

import java.io.IOException;

/**
 * Exception class representing corruption in the Tape queue.
 */
public class CorruptionException extends IOException {
  public CorruptionException(String message) {
    super(message);
  }
}
