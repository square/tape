// Copyright 2011 Square, Inc.
package com.squareup.tape2;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** A queue of objects. */
public abstract class ObjectQueue<T> implements Iterable<T>, Closeable {
  /** A queue for objects that are atomically and durably serialized to {@code file}. */
  public static <T> ObjectQueue<T> create(File file, Converter<T> converter) throws IOException {
    return new FileObjectQueue<T>(file, converter);
  }

  /**
   * A queue for objects that are not serious enough to be written to disk. Objects in this queue
   * are kept in memory and will not be serialized.
   */
  public static <T> ObjectQueue<T> createInMemory() {
    return new InMemoryObjectQueue<T>();
  }

  /** The underlying {@link File} backing this queue, or null if it's only in memory. */
  public abstract File file();

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
  public abstract T peek() throws IOException;

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

  /** Returns the entries in the queue as an unmodifiable {@link List}.*/
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
    T from(byte[] bytes) throws IOException;

    /** Converts o to bytes written to the specified stream. */
    void toStream(T o, OutputStream bytes) throws IOException;
  }
}
