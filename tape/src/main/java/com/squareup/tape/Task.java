// Copyright 2012 Square, Inc.
package com.squareup.tape;

import java.io.Serializable;

/**
 * An idempotent task that can be enqueued, persisted (using serialization),
 * and executed at a later time.
 *
 * @param <T> The type of callback.
 */
public interface Task<T> extends Serializable {

  /**
   * Executes this task. The members of this task instance have been injected
   * prior to calling this method.
   *
   * @param callback to report result to
   */
  void execute(T callback);
}
