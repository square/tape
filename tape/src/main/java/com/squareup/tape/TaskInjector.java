// Copyright 2012 Square, Inc.
package com.squareup.tape;

/**
 * Inject dependencies into tasks of any kind.
 *
 * @param <T> The type of tasks to inject.
 */
public interface TaskInjector<T extends Task> {
  void injectMembers(T task);
}
