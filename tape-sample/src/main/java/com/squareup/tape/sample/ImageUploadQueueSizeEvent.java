// Copyright 2012 Square, Inc.
package com.squareup.tape.sample;

public class ImageUploadQueueSizeEvent {
  public final long size;

  public ImageUploadQueueSizeEvent(long size) {
    this.size = size;
  }
}
