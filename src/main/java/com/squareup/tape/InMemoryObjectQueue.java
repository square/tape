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
  private Startable startable;
  private Queue<T> tasks;

  @SuppressWarnings("unchecked")
  public InMemoryObjectQueue(Startable startable) {
    this.startable = startable;
    tasks = (Queue<T>) new LinkedList();
  }

  @Override public void add(T entry) {
    tasks.add(entry);
    startable.start();
  }

  @Override public T peek() {
    return tasks.peek();
  }

  @Override public int size() {
    return tasks.size();
  }

  @Override public void remove() {
    tasks.remove();
  }

  @Override public void setListener(Listener<T> listener) {
    throw new UnsupportedOperationException("Listener not supported for in-memory queue");
  }
}
