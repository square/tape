// Copyright 2012 Square, Inc.
package com.squareup.tape.sample;

public class ImageUploadSuccessEvent {
  public final String url;

  public ImageUploadSuccessEvent(String url) {
    this.url = url;
  }
}
