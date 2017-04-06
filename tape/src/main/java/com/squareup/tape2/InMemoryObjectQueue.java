// Copyright 2012 Square, Inc.
package com.squareup.tape2;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

final class InMemoryObjectQueue<T> extends ObjectQueue<T> {
  private final Deque<T> entries;
  /**
   * The number of times this file has been structurally modified — it is incremented during {@link
   * #remove(int)} and {@link #add(Object)}. Used by {@link InMemoryObjectQueue.EntryIterator} to
   * guard against concurrent modification.
   */
  @Private int modCount = 0;
  @Private boolean closed;

  InMemoryObjectQueue() {
    entries = new ArrayDeque<>();
  }

  @Override public QueueFile file() {
    return null;
  }

  @Override public void add(T entry) throws IOException {
    if (closed) throw new IOException("closed");
    modCount++;
    entries.addLast(entry);
  }

  @Override public T peek() throws IOException {
    if (closed) throw new IOException("closed");
    return entries.peekFirst();
  }

  @Override public List<T> asList() throws IOException {
    return new ArrayList<>(entries);
  }

  @Override public int size() {
    return entries.size();
  }

  @Override public void remove(int n) throws IOException {
    if (closed) throw new IOException("closed");
    modCount++;
    for (int i = 0; i < n; i++) {
      entries.removeFirst();
    }
  }

  /**
   * Returns an iterator over entries in this queue.
   *
   * <p>The iterator disallows modifications to the queue during iteration. Removing entries from
   * the head of the queue is permitted during iteration using{@link Iterator#remove()}.
   */
  @Override public Iterator<T> iterator() {
    return new EntryIterator(entries.iterator());
  }

  @Override public void close() throws IOException {
    closed = true;
  }

  private final class EntryIterator implements Iterator<T> {
    private final Iterator<T> delegate;
    private int index = 0;

    /**
     * The {@link #modCount} value that the iterator believes that the backing QueueFile should
     * have. If this expectation is violated, the iterator has detected concurrent modification.
     */
    private int expectedModCount = modCount;

    @Private EntryIterator(Iterator<T> delegate) {
      this.delegate = delegate;
    }

    @Override public boolean hasNext() {
      checkForComodification();
      return delegate.hasNext();
    }

    @Override public T next() {
      if (closed) throw new IllegalStateException("closed");
      checkForComodification();

      T next = delegate.next();
      index += 1;
      return next;
    }

    @Override public void remove() {
      if (closed) throw new IllegalStateException("closed");
      checkForComodification();

      if (size() == 0) throw new NoSuchElementException();
      if (index != 1) {
        throw new UnsupportedOperationException("Removal is only permitted from the head.");
      }

      try {
        InMemoryObjectQueue.this.remove();
      } catch (IOException e) {
        throw new TapeException("Error is occurred while deleting object", e);
      }

      expectedModCount = modCount;
      index -= 1;
    }

    private void checkForComodification() {
      if (modCount != expectedModCount) throw new ConcurrentModificationException();
    }
  }
}