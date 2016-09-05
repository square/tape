// Copyright 2012 Square, Inc.
package com.squareup.tape;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * A queue for objects that are not serious enough to be written to disk. Objects in this queue
 * are kept in memory and will not be serialized.
 *
 * @param <T> The type of elements in the queue.
 */
public final class InMemoryObjectQueue<T> extends ObjectQueue<T> {
  // LinkedList can be used both as a List and Queue.
  private final LinkedList<T> entries;
  private Listener<T> listener;

  @SuppressWarnings("unchecked")
  public InMemoryObjectQueue() {
    entries = new LinkedList<T>();
  }

  @Override public void add(T entry) {
    entries.add(entry);
    if (listener != null) listener.onAdd(this, entry);
  }

  @Override public T peek() {
    return entries.peek();
  }

  @Override public List<T> peek(int max) throws IOException {
    int end = Math.min(max, entries.size());
    List<T> subList = entries.subList(0, end);
    return Collections.unmodifiableList(subList);
  }

  @Override public int size() {
    return entries.size();
  }

  @Override public void remove(int n) throws IOException {
    for (int i = 0; i < n; i++) {
      entries.remove();
      if (listener != null) listener.onRemove(this);
    }
  }

  @Override public void setListener(Listener<T> listener) {
    if (listener != null) {
      for (T entry : entries) {
        listener.onAdd(this, entry);
      }
    }
    this.listener = listener;
  }
}
