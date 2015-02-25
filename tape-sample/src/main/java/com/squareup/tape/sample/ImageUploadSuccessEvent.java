// Copyright 2012 Square, Inc.
package com.squareup.tape.sample;

public class ImageUploadSuccessEvent {
  public final String url;
  public final int numRetries;

  public ImageUploadSuccessEvent(String url, int numRetries) {
    this.url = url;
    this.numRetries = numRetries;
  }
}
