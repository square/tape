// Copyright 2011 Square, Inc.
package com.squareup.tape;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A queue of objects.
 *
 * @param <T> The type of queue for the elements.
 */
public abstract class ObjectQueue<T> implements Iterable<T> {

  /** Returns the number of entries in the queue. */
  abstract int size();

  /** Enqueues an entry that can be processed at any time. */
  abstract void add(T entry) throws IOException;

  /**
   * Returns the head of the queue, or {@code null} if the queue is empty. Does not modify the
   * queue.
   */
  abstract T peek() throws IOException;

  /**
   * Reads up to {@code max} entries from the head of the queue without removing the entries.
   * If the queue's {@link #size()} is less than {@code max} then only {@link #size()} entries
   * are read.
   */
  List<T> peek(int max) throws IOException {
    int end = Math.min(max, size());
    List<T> subList = new ArrayList<T>(end);
    Iterator<T> iterator = iterator();
    for (int i = 0; i < end; i++) {
      subList.add(iterator.next());
    }
    return Collections.unmodifiableList(subList);
  }

  /** Returns the entries in the queue as an unmodifiable {@link List}.*/
  List<T> asList() throws IOException {
    return peek(size());
  }

  /** Removes the head of the queue. */
  void remove() throws IOException {
    remove(1);
  }

  /** Removes {@code n} entries from the head of the queue. */
  abstract void remove(int n) throws IOException;

  /** Clears this queue. Also truncates the file to the initial size. */
  void clear() throws IOException {
    remove(size());
  }

  /**
   * Sets a listener on this queue. Invokes {@link Listener#onAdd} once for each entry that's
   * already in the queue. If an error occurs while reading the data, the listener will not receive
   * further notifications.
   */
  abstract void setListener(Listener<T> listener) throws IOException;

  /**
   * Listens for changes to the queue.
   *
   * @param <T> The type of elements in the queue.
   */
  interface Listener<T> {

    /** Called after an entry is added. */
    void onAdd(ObjectQueue<T> queue, T entry);

    /** Called after an entry is removed. */
    void onRemove(ObjectQueue<T> queue);
  }
}
