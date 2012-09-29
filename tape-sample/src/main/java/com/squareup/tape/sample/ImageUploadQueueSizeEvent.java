// Copyright 2012 Square, Inc.
package com.squareup.tape.sample;

public class ImageUploadQueueSizeEvent {
  public final int size;

  public ImageUploadQueueSizeEvent(int size) {
    this.size = size;
  }
}
