// Copyright 2012 Square, Inc.
package com.squareup.tape;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Base queue class, implements common functionality for a QueueFile-backed
 * queue manager. This class is not thread safe; instances should be kept
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
  @Private final Converter<T> converter;
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
   * Returns an iterator over entries in this queue.
   *
   * <p>The iterator disallows modifications to the queue during iteration. Removing entries from
   * the head of the queue is permitted during iteration using {@link Iterator#remove()}.
   *
   * <p>The iterator may throw an unchecked {@link RuntimeException} during {@link Iterator#next()}
   * or {@link Iterator#remove()}.
   */
  @Override public Iterator<T> iterator() {
    return new QueueFileIterator(queueFile.iterator());
  }

  private final class QueueFileIterator implements Iterator<T> {
    final Iterator<byte[]> iterator;

    @Private QueueFileIterator(Iterator<byte[]> iterator) {
      this.iterator = iterator;
    }

    @Override public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override public T next() {
      byte[] data = iterator.next();
      try {
        return converter.from(data);
      } catch (IOException e) {
        throw new RuntimeException("todo: throw a proper error", e);
      }
    }

    @Override public void remove() {
      iterator.remove();
    }
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
