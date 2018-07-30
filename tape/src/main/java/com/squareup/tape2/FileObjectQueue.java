// Copyright 2012 Square, Inc.
package com.squareup.tape2;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import okio.Buffer;

final class FileObjectQueue<T> extends ObjectQueue<T> {
  /** Backing storage implementation. */
  private final QueueFile queueFile;
  /** Reusable byte output buffer. */
  @Private final Converter<T> converter;

  FileObjectQueue(QueueFile queueFile, Converter<T> converter) {
    this.queueFile = queueFile;
    this.converter = converter;
  }

  @Override public @Nonnull QueueFile file() {
    return queueFile;
  }

  @Override public int size() {
    return queueFile.size();
  }

  @Override public boolean isEmpty() {
    return queueFile.isEmpty();
  }

  @Override public void add(T entry) throws IOException {
    Buffer buffer = new Buffer();
    converter.toStream(entry, buffer);
    byte[] data = buffer.readByteArray();
    queueFile.add(data, 0, data.length);
  }

  @Override public @Nullable T peek() throws IOException {
    byte[] bytes = queueFile.peek();
    if (bytes == null) return null;
    return converter.from(new Buffer().write(bytes));
  }

  @Override public void remove() throws IOException {
    queueFile.remove();
  }

  @Override public void remove(int n) throws IOException {
    queueFile.remove(n);
  }

  @Override public void clear() throws IOException {
    queueFile.clear();
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

  @Override public String toString() {
    return "FileObjectQueue{"
        + "queueFile=" + queueFile
        + '}';
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
        return converter.from(new Buffer().write(data));
      } catch (IOException e) {
        throw new RuntimeException("todo: throw a proper error", e);
      }
    }

    @Override public void remove() {
      iterator.remove();
    }
  }
}
