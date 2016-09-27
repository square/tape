// Copyright 2012 Square, Inc.
package com.squareup.tape2;

import java.io.File;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

final class InMemoryObjectQueue<T> extends ObjectQueue<T> {
  // LinkedList can be used both as a List (for get(n)) and Queue (for peek() and remove()).
  @Private final LinkedList<T> entries;
  /**
   * The number of times this file has been structurally modified — it is incremented during {@link
   * #remove(int)} and {@link #add(Object)}. Used by {@link InMemoryObjectQueue.EntryIterator} to
   * guard against concurrent modification.
   */
  @Private int modCount = 0;
  private boolean closed;

  InMemoryObjectQueue() {
    entries = new LinkedList<T>();
  }

  @Override public File file() {
    return null;
  }

  @Override public void add(T entry) throws IOException {
    if (closed) throw new IOException("closed");
    modCount++;
    entries.add(entry);
  }

  @Override public T peek() throws IOException {
    if (closed) throw new IOException("closed");
    return entries.peek();
  }

  @Override public int size() {
    return entries.size();
  }

  @Override public void remove(int n) throws IOException {
    if (closed) throw new IOException("closed");
    modCount++;
    for (int i = 0; i < n; i++) {
      entries.remove();
    }
  }

  /**
   * Returns an iterator over entries in this queue.
   *
   * <p>The iterator disallows modifications to the queue during iteration. Removing entries from
   * the head of the queue is permitted during iteration using{@link Iterator#remove()}.
   */
  @Override public Iterator<T> iterator() {
    return new EntryIterator();
  }

  @Override public void close() throws IOException {
    closed = true;
  }

  private final class EntryIterator implements Iterator<T> {
    /** Index of element to be returned by subsequent call to next. */
    int nextElementIndex = 0;

    /**
     * The {@link #modCount} value that the iterator believes that the backing QueueFile should
     * have. If this expectation is violated, the iterator has detected concurrent modification.
     */
    int expectedModCount = modCount;

    @Private EntryIterator() {

    }

    @Override public boolean hasNext() {
      checkForComodification();

      return nextElementIndex != size();
    }

    @Override public T next() {
      if (closed) throw new IllegalStateException("closed");
      checkForComodification();

      if (nextElementIndex >= size()) throw new NoSuchElementException();

      return entries.get(nextElementIndex++);
    }

    @Override public void remove() {
      if (closed) throw new IllegalStateException("closed");
      checkForComodification();

      if (size() == 0) throw new NoSuchElementException();
      if (nextElementIndex != 1) {
        throw new UnsupportedOperationException("Removal is only permitted from the head.");
      }

      try {
        InMemoryObjectQueue.this.remove();
      } catch (IOException e) {
        throw new RuntimeException("todo: throw a proper error", e);
      }

      expectedModCount = modCount;
      nextElementIndex--;
    }

    private void checkForComodification() {
      if (modCount != expectedModCount) throw new ConcurrentModificationException();
    }
  }
}