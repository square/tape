// Copyright 2012 Square, Inc.
package com.squareup.tape;

import java.io.IOException;

/**
 * Persistent task queue. Not safe for concurrent use.
 *
 * @param <T> The type of tasks in the queue.
 */
public class TaskQueue<T extends Task> implements ObjectQueue<T> {

  private final TaskInjector<T> taskInjector;
  private final ObjectQueue<T> delegate;

  public TaskQueue(ObjectQueue<T> delegate) {
    this(delegate, null);
  }

  public TaskQueue(ObjectQueue<T> delegate, TaskInjector<T> taskInjector) {
    this.delegate = delegate;
    this.taskInjector = taskInjector;
  }

  /**
   * {@inheritDoc}
   *
   * Overridden to inject members into Tasks.
   */
  @Override public T peek() throws IOException {
    T task = delegate.peek();
    if (task != null && taskInjector != null) {
      taskInjector.injectMembers(task);
    }
    return task;
  }

  @Override public int size() {
    return delegate.size();
  }

  @Override public void add(T entry) throws IOException {
    delegate.add(entry);
  }

  @Override public void remove() throws IOException {
    delegate.remove();
  }

  @Override public void setListener(final Listener<T> listener) throws IOException {
    if (listener != null) {
      // Intercept event delivery to pass the correct TaskQueue instance to listener.
      delegate.setListener(new Listener<T>() {
        @Override
        public void onAdd(ObjectQueue<T> queue, T entry) {
          listener.onAdd(TaskQueue.this, entry);
        }

        @Override
        public void onRemove(ObjectQueue<T> queue) {
          listener.onRemove(TaskQueue.this);
        }
      });
    } else {
      delegate.setListener(null);
    }
  }
}
