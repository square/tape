// Copyright 2012 Square, Inc.
package com.squareup.tape;

import java.util.LinkedList;
import java.util.Queue;

/**
 * A queue for objects that are not serious enough to be written to disk.  Objects in this queue
 * are kept in memory and will not be serialized.
 *
 * @param <T> The type of elements in the queue.
 */
public class InMemoryObjectQueue<T> implements ObjectQueue<T> {
  private final Queue<T> tasks;
  private Listener<T> listener;

  @SuppressWarnings("unchecked")
  public InMemoryObjectQueue() {
    tasks = (Queue<T>) new LinkedList();
  }

  @Override public void add(T entry) {
    tasks.add(entry);
    if (listener != null) listener.onAdd(this, entry);
  }

  @Override public T peek() {
    return tasks.peek();
  }

  @Override public int size() {
    return tasks.size();
  }

  @Override public void remove() {
    tasks.remove();
    if (listener != null) listener.onRemove(this);
  }

  @Override public void setListener(Listener<T> listener) {
    if (listener != null) {
      for (T task : tasks) {
        listener.onAdd(this, task);
      }
    }
    this.listener = listener;
  }
}
