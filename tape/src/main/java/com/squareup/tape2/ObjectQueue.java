// Copyright 2011 Square, Inc.
package com.squareup.tape2;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import okio.BufferedSink;
import okio.BufferedSource;

/** A queue of objects. */
public abstract class ObjectQueue<T> implements Iterable<T>, Closeable {
  /** A queue for objects that are atomically and durably serialized to {@code file}. */
  public static <T> ObjectQueue<T> create(QueueFile qf, Converter<T> converter) {
    return new FileObjectQueue<>(qf, converter);
  }

  /**
   * A queue for objects that are not serious enough to be written to disk. Objects in this queue
   * are kept in memory and will not be serialized.
   */
  public static <T> ObjectQueue<T> createInMemory() {
    return new InMemoryObjectQueue<>();
  }

  /** The underlying {@link QueueFile} backing this queue, or null if it's only in memory. */
  public abstract @Nullable QueueFile file();

  /** Returns the number of entries in the queue. */
  public abstract int size();

  /** Returns {@code true} if this queue contains no entries. */
  public boolean isEmpty() {
    return size() == 0;
  }

  /** Enqueues an entry that can be processed at any time. */
  public abstract void add(T entry) throws IOException;

  /**
   * Returns the head of the queue, or {@code null} if the queue is empty. Does not modify the
   * queue.
   */
  public abstract @Nullable T peek() throws IOException;

  /**
   * Reads up to {@code max} entries from the head of the queue without removing the entries.
   * If the queue's {@link #size()} is less than {@code max} then only {@link #size()} entries
   * are read.
   */
  public List<T> peek(int max) throws IOException {
    int end = Math.min(max, size());
    List<T> subList = new ArrayList<T>(end);
    Iterator<T> iterator = iterator();
    for (int i = 0; i < end; i++) {
      subList.add(iterator.next());
    }
    return Collections.unmodifiableList(subList);
  }

  /** Returns the entries in the queue as an unmodifiable {@link List}. */
  public List<T> asList() throws IOException {
    return peek(size());
  }

  /** Removes the head of the queue. */
  public void remove() throws IOException {
    remove(1);
  }

  /** Removes {@code n} entries from the head of the queue. */
  public abstract void remove(int n) throws IOException;

  /** Clears this queue. Also truncates the file to the initial size. */
  public void clear() throws IOException {
    remove(size());
  }

  /**
   * Convert a byte stream to and from a concrete type.
   *
   * @param <T> Object type.
   */
  public interface Converter<T> {
    /** Converts bytes to an object. */
    T from(BufferedSource source) throws IOException;

    /** Converts o to bytes written to the specified stream. */
    void toStream(T o, BufferedSink sink) throws IOException;
  }
}
