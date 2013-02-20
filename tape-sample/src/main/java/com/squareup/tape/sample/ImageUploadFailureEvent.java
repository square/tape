// Copyright 2012 Square, Inc.
package com.squareup.tape.sample;

public class ImageUploadFailureEvent {
  public final int numRetries;

  public ImageUploadFailureEvent(int numRetries) {
    this.numRetries = numRetries;
  }
}
