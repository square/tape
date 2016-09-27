// Copyright 2012 Square, Inc.
package com.squareup.tape2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

final class FileObjectQueue<T> extends ObjectQueue<T> {
  /** Backing storage implementation. */
  private final QueueFile queueFile;
  /** Reusable byte output buffer. */
  private final DirectByteArrayOutputStream bytes = new DirectByteArrayOutputStream();
  /** Keep file around for error reporting. */
  private final File file;
  @Private final Converter<T> converter;

  FileObjectQueue(File file, Converter<T> converter) throws IOException {
    this.file = file;
    this.converter = converter;
    this.queueFile = new QueueFile(file);
  }

  @Override public File file() {
    return file;
  }

  @Override public int size() {
    return queueFile.size();
  }

  @Override public void add(T entry) throws IOException {
    bytes.reset();
    converter.toStream(entry, bytes);
    queueFile.add(bytes.getArray(), 0, bytes.size());
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
  }

  @Override public void close() throws IOException {
    queueFile.close();
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
