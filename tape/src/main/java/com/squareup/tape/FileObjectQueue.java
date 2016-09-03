// Copyright 2012 Square, Inc.
package com.squareup.tape;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * Base queue class, implements common functionality for a QueueFile-backed
 * queue manager.  This class is not thread safe; instances should be kept
 * thread-confined.
 *
 * @param <T> The type of elements in the queue.
 */
public final class FileObjectQueue<T> extends ObjectQueue<T> implements Closeable {
  /** Backing storage implementation. */
  private final QueueFile queueFile;
  /** Reusable byte output buffer. */
  private final DirectByteArrayOutputStream bytes = new DirectByteArrayOutputStream();
  /** Keep file around for error reporting. */
  private final File file;
  final Converter<T> converter;
  private Listener<T> listener;

  public FileObjectQueue(File file, Converter<T> converter) throws IOException {
    this.file = file;
    this.converter = converter;
    this.queueFile = new QueueFile(file);
  }

  public File file() {
    return file;
  }

  @Override public int size() {
    return queueFile.size();
  }

  @Override public void add(T entry) throws IOException {
    bytes.reset();
    converter.toStream(entry, bytes);
    queueFile.add(bytes.getArray(), 0, bytes.size());
    if (listener != null) listener.onAdd(this, entry);
  }

  @Override public T peek() throws IOException {
    byte[] bytes = queueFile.peek();
    if (bytes == null) return null;
    return converter.from(bytes);
  }

  /**
   * Reads up to {@code max} entries from the head of the queue without removing the entries.
   * If the queue's {@link #size()} is less than {@code max} then only {@link #size()} entries
   * are read.
   */
  @Override public List<T> peek(int max) throws IOException {
    List<T> entries = new ArrayList<T>(queueFile.size() >  max ? max : queueFile.size());
    int count = 0;
    for (byte[] data : queueFile) {
      if (++count > max) {
        break;
      }
      entries.add(converter.from(data));
    }
    return unmodifiableList(entries);
  }

  @Override public List<T> asList() throws IOException {
    return peek(size());
  }

  @Override public void remove(int n) throws IOException {
    queueFile.remove(n);
    if (listener != null) {
      for (int i = 0; i < n; i++) {
        listener.onRemove(this);
      }
    }
  }

  @Override public void close() throws IOException {
    queueFile.close();
  }

  @Override public void setListener(Listener<T> listener) throws IOException {
    if (listener != null) {
      for (byte[] data : queueFile) {
        listener.onAdd(this, converter.from(data));
      }
    }
    this.listener = listener;
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

  /** Enables direct access to the internal array. Avoids unnecessary copying. */
  private static class DirectByteArrayOutputStream extends ByteArrayOutputStream {
    public DirectByteArrayOutputStream() {
      super();
    }

    /**
     * Gets a reference to the internal byte array.  The {@link #size()} method indicates how many
     * bytes contain actual data added since the last {@link #reset()} call.
     */
    public byte[] getArray() {
      return buf;
    }
  }
}
