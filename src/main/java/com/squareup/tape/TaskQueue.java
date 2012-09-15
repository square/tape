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
  private final Startable startable;

  public TaskQueue(ObjectQueue<T> delegate, TaskInjector<T> taskInjector, Startable startable) {
    this.delegate = delegate;
    this.taskInjector = taskInjector;
    this.startable = startable;
  }

  /**
   * {@inheritDoc}
   *
   * Overridden to inject members into Tasks.
   */
  @Override public T peek() {
    T task = delegate.peek();

    if (task == null) {
      return null;
    }
    taskInjector.injectMembers(task);

    return task;
  }

  @Override public int size() {
    return delegate.size();
  }

  @Override public void add(T entry) {
    delegate.add(entry);
    if (startable != null) {
      startable.start();
    }
  }

  @Override public void remove() {
    delegate.remove();
  }

  @Override public void setListener(Listener<T> listener) {
    delegate.setListener(listener);
  }
}
