// Copyright 2012 Square, Inc.
package com.squareup.tape;

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
  @Override public T peek() {
    T task = delegate.peek();
    if (task != null && taskInjector != null) {
      taskInjector.injectMembers(task);
    }
    return task;
  }

  @Override public int size() {
    return delegate.size();
  }

  @Override public void add(T entry) {
    delegate.add(entry);
  }

  @Override public void remove() {
    delegate.remove();
  }

  @Override public void setListener(Listener<T> listener) {
    delegate.setListener(listener);
  }
}
