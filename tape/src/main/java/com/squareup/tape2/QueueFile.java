/*
 * Copyright (C) 2010 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.squareup.tape2;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.Math.min;

/**
 * A reliable, efficient, file-based, FIFO queue. Additions and removals are O(1). All operations
 * are atomic. Writes are synchronous; data will be written to disk before an operation returns.
 * The underlying file is structured to survive process and even system crashes. If an I/O
 * exception is thrown during a mutating change, the change is aborted. It is safe to continue to
 * use a {@code QueueFile} instance after an exception.
 *
 * <p><strong>Note that this implementation is not synchronized.</strong>
 *
 * <p>In a traditional queue, the remove operation returns an element. In this queue,
 * {@link #peek} and {@link #remove} are used in conjunction. Use
 * {@code peek} to retrieve the first element, and then {@code remove} to remove it after
 * successful processing. If the system crashes after {@code peek} and during processing, the
 * element will remain in the queue, to be processed when the system restarts.
 *
 * <p><strong>NOTE:</strong> The current implementation is built for file systems that support
 * atomic segment writes (like YAFFS). Most conventional file systems don't support this; if the
 * power goes out while writing a segment, the segment will contain garbage and the file will be
 * corrupt. We'll add journaling support so this class can be used with more file systems later.
 *
 * @author Bob Lee (bob@squareup.com)
 */
public final class QueueFile implements Closeable, Iterable<byte[]> {
  /** Initial file size in bytes. */
  private static final int INITIAL_LENGTH = 4096; // one file system block

  /** A block of nothing to write over old data. */
  private static final byte[] ZEROES = new byte[INITIAL_LENGTH];

  /** Length of header in bytes. */
  static final int HEADER_LENGTH = 16;

  /**
   * The underlying file. Uses a ring buffer to store entries. Designed so that a modification
   * isn't committed or visible until we write the header. The header is much smaller than a
   * segment. So long as the underlying file system supports atomic segment writes, changes to the
   * queue are atomic. Storing the file length ensures we can recover from a failed expansion
   * (i.e. if setting the file length succeeds but the process dies before the data can be copied).
   * <p/>
   * <pre>
   *   Format:
   *     Header              (16 bytes)
   *     Element Ring Buffer (File Length - 16 bytes)
   * <p/>
   *   Header:
   *     File Length            (4 bytes)
   *     Element Count          (4 bytes)
   *     First Element Position (4 bytes, =0 if null)
   *     Last Element Position  (4 bytes, =0 if null)
   * <p/>
   *   Element:
   *     Length (4 bytes)
   *     Data   (Length bytes)
   * </pre>
   *
   * Visible for testing.
   */
  final RandomAccessFile raf;

  /** Cached file length. Always a power of 2. */
  int fileLength;

  /** Number of elements. */
  @Private int elementCount;

  /** Pointer to first (or eldest) element. */
  @Private Element first;

  /** Pointer to last (or newest) element. */
  private Element last;

  /** In-memory buffer. Big enough to hold the header. */
  private final byte[] buffer = new byte[HEADER_LENGTH];

  /**
   * The number of times this file has been structurally modified — it is incremented during
   * {@link #remove(int)} and {@link #add(byte[], int, int)}. Used by {@link ElementIterator}
   * to guard against concurrent modification.
   */
  @Private int modCount = 0;

  /** When true, removing an element will also overwrite data with zero bytes. */
  private final boolean zero;

  @Private boolean closed;

  /**
   * Constructs a new queue backed by the given file. Only one instance should access a given file
   * at a time.
   */
  public QueueFile(File file) throws IOException {
    this(file, true);
  }

  /**
   * Constructs a new queue backed by the given file. Only one instance should access a given file
   * at a time.
   *
   * @param zero When true, removing an element will also overwrite data with zero bytes.
   */
  public QueueFile(File file, boolean zero) throws IOException {
    this(initializeFromFile(file), zero);
  }

  private static RandomAccessFile initializeFromFile(File file) throws IOException {
    if (!file.exists()) {
      // Use a temp file so we don't leave a partially-initialized file.
      File tempFile = new File(file.getPath() + ".tmp");
      RandomAccessFile raf = open(tempFile);
      try {
        raf.setLength(INITIAL_LENGTH);
        raf.seek(0);
        raf.writeInt(INITIAL_LENGTH);
      } finally {
        raf.close();
      }

      // A rename is atomic.
      if (!tempFile.renameTo(file)) {
        throw new IOException("Rename failed!");
      }
    }

    return open(file);
  }

  /** Opens a random access file that writes synchronously. */
  private static RandomAccessFile open(File file) throws FileNotFoundException {
    return new RandomAccessFile(file, "rwd");
  }

  QueueFile(RandomAccessFile raf, boolean zero) throws IOException {
    this.raf = raf;
    this.zero = zero;

    raf.seek(0);
    raf.readFully(buffer);
    fileLength = readInt(buffer, 0);
    if (fileLength > raf.length()) {
      throw new IOException(
          "File is truncated. Expected length: " + fileLength + ", Actual length: " + raf.length());
    } else if (fileLength <= HEADER_LENGTH) {
      throw new IOException(
          "File is corrupt; length stored in header (" + fileLength + ") is invalid.");
    }
    elementCount = readInt(buffer, 4);
    int firstOffset = readInt(buffer, 8);
    int lastOffset = readInt(buffer, 12);
    first = readElement(firstOffset);
    last = readElement(lastOffset);
  }

  /**
   * Stores an {@code int} in the {@code byte[]}. The behavior is equivalent to calling
   * {@link RandomAccessFile#writeInt}.
   */
  private static void writeInt(byte[] buffer, int offset, int value) {
    buffer[offset    ] = (byte) (value >> 24);
    buffer[offset + 1] = (byte) (value >> 16);
    buffer[offset + 2] = (byte) (value >> 8);
    buffer[offset + 3] = (byte) value;
  }

  /** Reads an {@code int} from the {@code byte[]}. */
  private static int readInt(byte[] buffer, int offset) {
    return ((buffer[offset    ] & 0xff) << 24)
        +  ((buffer[offset + 1] & 0xff) << 16)
        +  ((buffer[offset + 2] & 0xff) << 8)
        +   (buffer[offset + 3] & 0xff);
  }

  /**
   * Writes header atomically. The arguments contain the updated values. The class member fields
   * should not have changed yet. This only updates the state in the file. It's up to the caller to
   * update the class member variables *after* this call succeeds. Assumes segment writes are
   * atomic in the underlying file system.
   */
  private void writeHeader(int fileLength, int elementCount, int firstPosition, int lastPosition)
      throws IOException {
    writeInt(buffer, 0, fileLength);
    writeInt(buffer, 4, elementCount);
    writeInt(buffer, 8, firstPosition);
    writeInt(buffer, 12, lastPosition);
    raf.seek(0);
    raf.write(buffer);
  }

  @Private Element readElement(int position) throws IOException {
    if (position == 0) return Element.NULL;
    ringRead(position, buffer, 0, Element.HEADER_LENGTH);
    int length = readInt(buffer, 0);
    return new Element(position, length);
  }

  /** Wraps the position if it exceeds the end of the file. */
  @Private int wrapPosition(int position) {
    return position < fileLength ? position
        : HEADER_LENGTH + position - fileLength;
  }

  /**
   * Writes count bytes from buffer to position in file. Automatically wraps write if position is
   * past the end of the file or if buffer overlaps it.
   *
   * @param position in file to write to
   * @param buffer to write from
   * @param count # of bytes to write
   */
  private void ringWrite(int position, byte[] buffer, int offset, int count) throws IOException {
    position = wrapPosition(position);
    if (position + count <= fileLength) {
      raf.seek(position);
      raf.write(buffer, offset, count);
    } else {
      // The write overlaps the EOF.
      // # of bytes to write before the EOF.
      int beforeEof = fileLength - position;
      raf.seek(position);
      raf.write(buffer, offset, beforeEof);
      raf.seek(HEADER_LENGTH);
      raf.write(buffer, offset + beforeEof, count - beforeEof);
    }
  }

  private void ringErase(int position, int length) throws IOException {
    while (length > 0) {
      int chunk = min(length, ZEROES.length);
      ringWrite(position, ZEROES, 0, chunk);
      length -= chunk;
      position += chunk;
    }
  }

  /**
   * Reads count bytes into buffer from file. Wraps if necessary.
   *
   * @param position in file to read from
   * @param buffer to read into
   * @param count # of bytes to read
   */
  @Private void ringRead(int position, byte[] buffer, int offset, int count) throws IOException {
    position = wrapPosition(position);
    if (position + count <= fileLength) {
      raf.seek(position);
      raf.readFully(buffer, offset, count);
    } else {
      // The read overlaps the EOF.
      // # of bytes to read before the EOF.
      int beforeEof = fileLength - position;
      raf.seek(position);
      raf.readFully(buffer, offset, beforeEof);
      raf.seek(HEADER_LENGTH);
      raf.readFully(buffer, offset + beforeEof, count - beforeEof);
    }
  }

  /**
   * Adds an element to the end of the queue.
   *
   * @param data to copy bytes from
   */
  public void add(byte[] data) throws IOException {
    add(data, 0, data.length);
  }

  /**
   * Adds an element to the end of the queue.
   *
   * @param data to copy bytes from
   * @param offset to start from in buffer
   * @param count number of bytes to copy
   * @throws IndexOutOfBoundsException if {@code offset < 0} or {@code count < 0}, or if {@code
   * offset + count} is bigger than the length of {@code buffer}.
   */
  public void add(byte[] data, int offset, int count) throws IOException {
    if (data == null) {
      throw new NullPointerException("data == null");
    }
    if ((offset | count) < 0 || count > data.length - offset) {
      throw new IndexOutOfBoundsException();
    }
    if (closed) throw new IOException("closed");

    expandIfNecessary(count);

    // Insert a new element after the current last element.
    boolean wasEmpty = isEmpty();
    int position = wasEmpty ? HEADER_LENGTH
        : wrapPosition(last.position + Element.HEADER_LENGTH + last.length);
    Element newLast = new Element(position, count);

    // Write length.
    writeInt(buffer, 0, count);
    ringWrite(newLast.position, buffer, 0, Element.HEADER_LENGTH);

    // Write data.
    ringWrite(newLast.position + Element.HEADER_LENGTH, data, offset, count);

    // Commit the addition. If wasEmpty, first == last.
    int firstPosition = wasEmpty ? newLast.position : first.position;
    writeHeader(fileLength, elementCount + 1, firstPosition, newLast.position);
    last = newLast;
    elementCount++;
    modCount++;
    if (wasEmpty) first = last; // first element
  }

  private int usedBytes() {
    if (elementCount == 0) return HEADER_LENGTH;

    if (last.position >= first.position) {
      // Contiguous queue.
      return (last.position - first.position)   // all but last entry
          + Element.HEADER_LENGTH + last.length // last entry
          + HEADER_LENGTH;
    } else {
      // tail < head. The queue wraps.
      return last.position                      // buffer front + header
          + Element.HEADER_LENGTH + last.length // last entry
          + fileLength - first.position;        // buffer end
    }
  }

  private int remainingBytes() {
    return fileLength - usedBytes();
  }

  /** Returns true if this queue contains no entries. */
  public boolean isEmpty() {
    return elementCount == 0;
  }

  /**
   * If necessary, expands the file to accommodate an additional element of the given length.
   *
   * @param dataLength length of data being added
   */
  private void expandIfNecessary(int dataLength) throws IOException {
    int elementLength = Element.HEADER_LENGTH + dataLength;
    int remainingBytes = remainingBytes();
    if (remainingBytes >= elementLength) return;

    // Expand.
    int previousLength = fileLength;
    int newLength;
    // Double the length until we can fit the new data.
    do {
      remainingBytes += previousLength;
      newLength = previousLength << 1;
      previousLength = newLength;
    } while (remainingBytes < elementLength);

    setLength(newLength);

    // Calculate the position of the tail end of the data in the ring buffer
    int endOfLastElement = wrapPosition(last.position + Element.HEADER_LENGTH + last.length);

    // If the buffer is split, we need to make it contiguous
    if (endOfLastElement <= first.position) {
      FileChannel channel = raf.getChannel();
      channel.position(fileLength); // destination position
      int count = endOfLastElement - HEADER_LENGTH;
      if (channel.transferTo(HEADER_LENGTH, count, channel) != count) {
        throw new AssertionError("Copied insufficient number of bytes!");
      }
      if (zero) {
        ringErase(HEADER_LENGTH, count);
      }
    }

    // Commit the expansion.
    if (last.position < first.position) {
      int newLastPosition = fileLength + last.position - HEADER_LENGTH;
      writeHeader(newLength, elementCount, first.position, newLastPosition);
      last = new Element(newLastPosition, last.length);
    } else {
      writeHeader(newLength, elementCount, first.position, last.position);
    }

    fileLength = newLength;
  }

  /** Sets the length of the file. */
  private void setLength(int newLength) throws IOException {
    // Set new file length (considered metadata) and sync it to storage.
    raf.setLength(newLength);
    raf.getChannel().force(true);
  }

  /** Reads the eldest element. Returns null if the queue is empty. */
  public byte[] peek() throws IOException {
    if (closed) throw new IOException("closed");
    if (isEmpty()) return null;
    int length = first.length;
    byte[] data = new byte[length];
    ringRead(first.position + Element.HEADER_LENGTH, data, 0, length);
    return data;
  }

  /**
   * Returns an iterator over elements in this QueueFile.
   *
   * <p>The iterator disallows modifications to be made to the QueueFile during iteration. Removing
   * elements from the head of the QueueFile is permitted during iteration using
   * {@link Iterator#remove()}.
   *
   * <p>The iterator may throw an unchecked {@link RuntimeException} during {@link Iterator#next()}
   * or {@link Iterator#remove()}.
   */
  @Override public Iterator<byte[]> iterator() {
    return new ElementIterator();
  }

  private final class ElementIterator implements Iterator<byte[]> {
    /** Index of element to be returned by subsequent call to next. */
    int nextElementIndex = 0;

    /** Position of element to be returned by subsequent call to next. */
    private int nextElementPosition = first.position;

    /**
     * The {@link #modCount} value that the iterator believes that the backing QueueFile should
     * have. If this expectation is violated, the iterator has detected concurrent modification.
     */
    int expectedModCount = modCount;

    @Private ElementIterator() {
    }

    private void checkForComodification() {
      if (modCount != expectedModCount) throw new ConcurrentModificationException();
    }

    @Override public boolean hasNext() {
      if (closed) throw new IllegalStateException("closed");
      checkForComodification();
      return nextElementIndex != elementCount;
    }

    @Override public byte[] next() {
      if (closed) throw new IllegalStateException("closed");
      checkForComodification();
      if (isEmpty()) throw new NoSuchElementException();
      if (nextElementIndex >= elementCount) throw new NoSuchElementException();

      try {
        // Read the current element.
        Element current = readElement(nextElementPosition);
        byte[] buffer = new byte[current.length];
        nextElementPosition = wrapPosition(current.position + Element.HEADER_LENGTH);
        ringRead(nextElementPosition, buffer, 0, current.length);

        // Update the pointer to the next element.
        nextElementPosition =
            wrapPosition(current.position + Element.HEADER_LENGTH + current.length);
        nextElementIndex++;

        // Return the read element.
        return buffer;
      } catch (IOException e) {
        throw new RuntimeException("todo: throw a proper error", e);
      }
    }

    @Override public void remove() {
      checkForComodification();

      if (isEmpty()) throw new NoSuchElementException();
      if (nextElementIndex != 1) {
        throw new UnsupportedOperationException("Removal is only permitted from the head.");
      }

      try {
        QueueFile.this.remove();
      } catch (IOException e) {
        throw new RuntimeException("todo: throw a proper error", e);
      }

      expectedModCount = modCount;
      nextElementIndex--;
    }
  }

  /** Returns the number of elements in this queue. */
  public int size() {
    return elementCount;
  }

  /**
   * Removes the eldest element.
   *
   * @throws NoSuchElementException if the queue is empty
   */
  public void remove() throws IOException {
    remove(1);
  }

  /**
   * Removes the eldest {@code n} elements.
   *
   * @throws NoSuchElementException if the queue is empty
   */
  public void remove(int n) throws IOException {
    if (n < 0) {
      throw new IllegalArgumentException("Cannot remove negative (" + n + ") number of elements.");
    }
    if (n == 0) {
      return;
    }
    if (n == elementCount) {
      clear();
      return;
    }
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    if (n > elementCount) {
      throw new IllegalArgumentException(
          "Cannot remove more elements (" + n + ") than present in queue (" + elementCount + ").");
    }

    final int eraseStartPosition = first.position;
    int eraseTotalLength = 0;

    // Read the position and length of the new first element.
    int newFirstPosition = first.position;
    int newFirstLength = first.length;
    for (int i = 0; i < n; i++) {
      eraseTotalLength += Element.HEADER_LENGTH + newFirstLength;
      newFirstPosition = wrapPosition(newFirstPosition + Element.HEADER_LENGTH + newFirstLength);
      ringRead(newFirstPosition, buffer, 0, Element.HEADER_LENGTH);
      newFirstLength = readInt(buffer, 0);
    }

    // Commit the header.
    writeHeader(fileLength, elementCount - n, newFirstPosition, last.position);
    elementCount -= n;
    modCount++;
    first = new Element(newFirstPosition, newFirstLength);

    if (zero) {
      ringErase(eraseStartPosition, eraseTotalLength);
    }
  }

  /** Clears this queue. Truncates the file to the initial size. */
  public void clear() throws IOException {
    if (closed) throw new IOException("closed");

    // Commit the header.
    writeHeader(INITIAL_LENGTH, 0, 0, 0);

    if (zero) {
      // Zero out data.
      raf.seek(HEADER_LENGTH);
      raf.write(ZEROES, 0, INITIAL_LENGTH - HEADER_LENGTH);
    }

    elementCount = 0;
    first = Element.NULL;
    last = Element.NULL;
    if (fileLength > INITIAL_LENGTH) setLength(INITIAL_LENGTH);
    fileLength = INITIAL_LENGTH;
    modCount++;
  }

  @Override public void close() throws IOException {
    closed = true;
    raf.close();
  }

  @Override public String toString() {
    return getClass().getSimpleName()
        + "[length=" + fileLength
        + ", size=" + elementCount
        + ", first=" + first
        + ", last=" + last
        + "]";
  }

  /** A pointer to an element. */
  static class Element {
    static final Element NULL = new Element(0, 0);

    /** Length of element header in bytes. */
    static final int HEADER_LENGTH = 4;

    /** Position in file. */
    final int position;

    /** The length of the data. */
    final int length;

    /**
     * Constructs a new element.
     *
     * @param position within file
     * @param length of data
     */
    Element(int position, int length) {
      this.position = position;
      this.length = length;
    }

    @Override public String toString() {
      return getClass().getSimpleName()
          + "[position=" + position
          + ", length=" + length
          + "]";
    }
  }
}
