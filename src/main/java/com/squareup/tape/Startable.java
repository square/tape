// Copyright 2012 Square, Inc.
package com.squareup.tape;

/** Something that can be started. */
public interface Startable {

  /** Starts something. Idempotent. */
  void start();
}
