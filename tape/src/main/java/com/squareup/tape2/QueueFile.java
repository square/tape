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
import javax.annotation.Nullable;

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
 * Construct instances with {@link Builder}.
 *
 * @author Bob Lee (bob@squareup.com)
 */
public final class QueueFile implements Closeable, Iterable<byte[]> {
  /** Leading bit set to 1 indicating a versioned header and the version of 1. */
  private static final int VERSIONED_HEADER = 0x80000001;

  /** Initial file size in bytes. */
  static final int INITIAL_LENGTH = 4096; // one file system block

  /** A block of nothing to write over old data. */
  private static final byte[] ZEROES = new byte[INITIAL_LENGTH];

  /**
   * The underlying file. Uses a ring buffer to store entries. Designed so that a modification
   * isn't committed or visible until we write the header. The header is much smaller than a
   * segment. So long as the underlying file system supports atomic segment writes, changes to the
   * queue are atomic. Storing the file length ensures we can recover from a failed expansion
   * (i.e. if setting the file length succeeds but the process dies before the data can be copied).
   * <p>
   * This implementation supports two versions of the on-disk format.
   * <pre>
   * Format:
   *   16-32 bytes      Header
   *   ...              Data
   *
   * Header (32 bytes):
   *   1 bit            Versioned indicator [0 = legacy (see "Legacy Header"), 1 = versioned]
   *   31 bits          Version, always 1
   *   8 bytes          File length
   *   4 bytes          Element count
   *   8 bytes          Head element position
   *   8 bytes          Tail element position
   *
   * Legacy Header (16 bytes):
   *   1 bit            Legacy indicator, always 0 (see "Header")
   *   31 bits          File length
   *   4 bytes          Element count
   *   4 bytes          Head element position
   *   4 bytes          Tail element position
   *
   * Element:
   *   4 bytes          Data length
   *   ...              Data
   * </pre>
   */
  final RandomAccessFile raf;

  /** Keep file around for error reporting. */
  final File file;

  /** True when using the versioned header format. Otherwise use the legacy format. */
  final boolean versioned;

  /** The header length in bytes: 16 or 32. */
  final int headerLength;

  /** Cached file length. Always a power of 2. */
  long fileLength;

  /** Number of elements. */
  @Private int elementCount;

  /** Pointer to first (or eldest) element. */
  @Private Element first;

  /** Pointer to last (or newest) element. */
  private Element last;

  /** In-memory buffer. Big enough to hold the header. */
  private final byte[] buffer = new byte[32];

  /**
   * The number of times this file has been structurally modified — it is incremented during
   * {@link #remove(int)} and {@link #add(byte[], int, int)}. Used by {@link ElementIterator}
   * to guard against concurrent modification.
   */
  @Private int modCount = 0;

  /** When true, removing an element will also overwrite data with zero bytes. */
  private final boolean zero;

  @Private boolean closed;

  @Private static RandomAccessFile initializeFromFile(File file, boolean forceLegacy)
      throws IOException {
    if (!file.exists()) {
      // Use a temp file so we don't leave a partially-initialized file.
      File tempFile = new File(file.getPath() + ".tmp");
      RandomAccessFile raf = open(tempFile);
      try {
        raf.setLength(INITIAL_LENGTH);
        raf.seek(0);
        if (forceLegacy) {
          raf.writeInt(INITIAL_LENGTH);
        } else {
          raf.writeInt(VERSIONED_HEADER);
          raf.writeLong(INITIAL_LENGTH);
        }
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

  QueueFile(File file, RandomAccessFile raf, boolean zero, boolean forceLegacy) throws IOException {
    this.file = file;
    this.raf = raf;
    this.zero = zero;

    raf.seek(0);
    raf.readFully(buffer);

    versioned = !forceLegacy && (buffer[0] & 0x80) != 0;
    long firstOffset;
    long lastOffset;
    if (versioned) {
      headerLength = 32;

      int version = readInt(buffer, 0) & 0x7FFFFFFF;
      if (version != 1) {
        throw new IOException(
            "Unable to read version " + version + " format. Supported versions are 1 and legacy.");
      }
      fileLength = readLong(buffer, 4);
      elementCount = readInt(buffer, 12);
      firstOffset = readLong(buffer, 16);
      lastOffset = readLong(buffer, 24);
    } else {
      headerLength = 16;

      fileLength = readInt(buffer, 0);
      elementCount = readInt(buffer, 4);
      firstOffset = readInt(buffer, 8);
      lastOffset = readInt(buffer, 12);
    }

    if (fileLength > raf.length()) {
      throw new IOException(
          "File is truncated. Expected length: " + fileLength + ", Actual length: " + raf.length());
    } else if (fileLength <= headerLength) {
      throw new IOException(
          "File is corrupt; length stored in header (" + fileLength + ") is invalid.");
    }

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
   * Stores an {@code long} in the {@code byte[]}. The behavior is equivalent to calling
   * {@link RandomAccessFile#writeLong}.
   */
  private static void writeLong(byte[] buffer, int offset, long value) {
    buffer[offset    ] = (byte) (value >> 56);
    buffer[offset + 1] = (byte) (value >> 48);
    buffer[offset + 2] = (byte) (value >> 40);
    buffer[offset + 3] = (byte) (value >> 32);
    buffer[offset + 4] = (byte) (value >> 24);
    buffer[offset + 5] = (byte) (value >> 16);
    buffer[offset + 6] = (byte) (value >> 8);
    buffer[offset + 7] = (byte) value;
  }

  /** Reads an {@code long} from the {@code byte[]}. */
  private static long readLong(byte[] buffer, int offset) {
    return ((buffer[offset    ] & 0xffL) << 56)
        +  ((buffer[offset + 1] & 0xffL) << 48)
        +  ((buffer[offset + 2] & 0xffL) << 40)
        +  ((buffer[offset + 3] & 0xffL) << 32)
        +  ((buffer[offset + 4] & 0xffL) << 24)
        +  ((buffer[offset + 5] & 0xffL) << 16)
        +  ((buffer[offset + 6] & 0xffL) << 8)
        +   (buffer[offset + 7] & 0xffL);
  }

  /**
   * Writes header atomically. The arguments contain the updated values. The class member fields
   * should not have changed yet. This only updates the state in the file. It's up to the caller to
   * update the class member variables *after* this call succeeds. Assumes segment writes are
   * atomic in the underlying file system.
   */
  private void writeHeader(long fileLength, int elementCount, long firstPosition, long lastPosition)
      throws IOException {
    raf.seek(0);

    if (versioned) {
      writeInt(buffer, 0, VERSIONED_HEADER);
      writeLong(buffer, 4, fileLength);
      writeInt(buffer, 12, elementCount);
      writeLong(buffer, 16, firstPosition);
      writeLong(buffer, 24, lastPosition);
      raf.write(buffer, 0, 32);
      return;
    }

    // Legacy queue header.
    writeInt(buffer, 0, (int) fileLength); // Signed, so leading bit is always 0 aka legacy.
    writeInt(buffer, 4, elementCount);
    writeInt(buffer, 8, (int) firstPosition);
    writeInt(buffer, 12, (int) lastPosition);
    raf.write(buffer, 0, 16);
  }

  @Private Element readElement(long position) throws IOException {
    if (position == 0) return Element.NULL;
    ringRead(position, buffer, 0, Element.HEADER_LENGTH);
    int length = readInt(buffer, 0);
    return new Element(position, length);
  }

  /** Wraps the position if it exceeds the end of the file. */
  @Private long wrapPosition(long position) {
    return position < fileLength ? position
        : headerLength + position - fileLength;
  }

  /**
   * Writes count bytes from buffer to position in file. Automatically wraps write if position is
   * past the end of the file or if buffer overlaps it.
   *
   * @param position in file to write to
   * @param buffer to write from
   * @param count # of bytes to write
   */
  private void ringWrite(long position, byte[] buffer, int offset, int count) throws IOException {
    position = wrapPosition(position);
    if (position + count <= fileLength) {
      raf.seek(position);
      raf.write(buffer, offset, count);
    } else {
      // The write overlaps the EOF.
      // # of bytes to write before the EOF. Guaranteed to be less than Integer.MAX_VALUE.
      int beforeEof = (int) (fileLength - position);
      raf.seek(position);
      raf.write(buffer, offset, beforeEof);
      raf.seek(headerLength);
      raf.write(buffer, offset + beforeEof, count - beforeEof);
    }
  }

  private void ringErase(long position, long length) throws IOException {
    while (length > 0) {
      int chunk = (int) min(length, ZEROES.length);
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
  @Private void ringRead(long position, byte[] buffer, int offset, int count) throws IOException {
    position = wrapPosition(position);
    if (position + count <= fileLength) {
      raf.seek(position);
      raf.readFully(buffer, offset, count);
    } else {
      // The read overlaps the EOF.
      // # of bytes to read before the EOF. Guaranteed to be less than Integer.MAX_VALUE.
      int beforeEof = (int) (fileLength - position);
      raf.seek(position);
      raf.readFully(buffer, offset, beforeEof);
      raf.seek(headerLength);
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
    if (closed) throw new IllegalStateException("closed");

    expandIfNecessary(count);

    // Insert a new element after the current last element.
    boolean wasEmpty = isEmpty();
    long position = wasEmpty ? headerLength
        : wrapPosition(last.position + Element.HEADER_LENGTH + last.length);
    Element newLast = new Element(position, count);

    // Write length.
    writeInt(buffer, 0, count);
    ringWrite(newLast.position, buffer, 0, Element.HEADER_LENGTH);

    // Write data.
    ringWrite(newLast.position + Element.HEADER_LENGTH, data, offset, count);

    // Commit the addition. If wasEmpty, first == last.
    long firstPosition = wasEmpty ? newLast.position : first.position;
    writeHeader(fileLength, elementCount + 1, firstPosition, newLast.position);
    last = newLast;
    elementCount++;
    modCount++;
    if (wasEmpty) first = last; // first element
  }

  private long usedBytes() {
    if (elementCount == 0) return headerLength;

    if (last.position >= first.position) {
      // Contiguous queue.
      return (last.position - first.position)   // all but last entry
          + Element.HEADER_LENGTH + last.length // last entry
          + headerLength;
    } else {
      // tail < head. The queue wraps.
      return last.position                      // buffer front + header
          + Element.HEADER_LENGTH + last.length // last entry
          + fileLength - first.position;        // buffer end
    }
  }

  private long remainingBytes() {
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
  private void expandIfNecessary(long dataLength) throws IOException {
    long elementLength = Element.HEADER_LENGTH + dataLength;
    long remainingBytes = remainingBytes();
    if (remainingBytes >= elementLength) return;

    // Expand.
    long previousLength = fileLength;
    long newLength;
    // Double the length until we can fit the new data.
    do {
      remainingBytes += previousLength;
      newLength = previousLength << 1;
      previousLength = newLength;
    } while (remainingBytes < elementLength);

    setLength(newLength);

    // Calculate the position of the tail end of the data in the ring buffer
    long endOfLastElement = wrapPosition(last.position + Element.HEADER_LENGTH + last.length);
    long count = 0;
    // If the buffer is split, we need to make it contiguous
    if (endOfLastElement <= first.position) {
      FileChannel channel = raf.getChannel();
      channel.position(fileLength); // destination position
      count = endOfLastElement - headerLength;
      if (channel.transferTo(headerLength, count, channel) != count) {
        throw new AssertionError("Copied insufficient number of bytes!");
      }
    }

    // Commit the expansion.
    if (last.position < first.position) {
      long newLastPosition = fileLength + last.position - headerLength;
      writeHeader(newLength, elementCount, first.position, newLastPosition);
      last = new Element(newLastPosition, last.length);
    } else {
      writeHeader(newLength, elementCount, first.position, last.position);
    }

    fileLength = newLength;

    if (zero) {
      ringErase(headerLength, count);
    }
  }

  /** Sets the length of the file. */
  private void setLength(long newLength) throws IOException {
    // Set new file length (considered metadata) and sync it to storage.
    raf.setLength(newLength);
    raf.getChannel().force(true);
  }

  /** Reads the eldest element. Returns null if the queue is empty. */
  public @Nullable byte[] peek() throws IOException {
    if (closed) throw new IllegalStateException("closed");
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
   * <p>The iterator may throw an unchecked {@link IOException} during {@link Iterator#next()}
   * or {@link Iterator#remove()}.
   */
  @Override public Iterator<byte[]> iterator() {
    return new ElementIterator();
  }

  private final class ElementIterator implements Iterator<byte[]> {
    /** Index of element to be returned by subsequent call to next. */
    int nextElementIndex = 0;

    /** Position of element to be returned by subsequent call to next. */
    private long nextElementPosition = first.position;

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
        throw QueueFile.<Error>getSneakyThrowable(e);
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
        throw QueueFile.<Error>getSneakyThrowable(e);
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

    long eraseStartPosition = first.position;
    long eraseTotalLength = 0;

    // Read the position and length of the new first element.
    long newFirstPosition = first.position;
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
    if (closed) throw new IllegalStateException("closed");

    // Commit the header.
    writeHeader(INITIAL_LENGTH, 0, 0, 0);

    if (zero) {
      // Zero out data.
      raf.seek(headerLength);
      raf.write(ZEROES, 0, INITIAL_LENGTH - headerLength);
    }

    elementCount = 0;
    first = Element.NULL;
    last = Element.NULL;
    if (fileLength > INITIAL_LENGTH) setLength(INITIAL_LENGTH);
    fileLength = INITIAL_LENGTH;
    modCount++;
  }

  /** The underlying {@link File} backing this queue. */
  public File file() {
    return file;
  }

  @Override public void close() throws IOException {
    closed = true;
    raf.close();
  }

  @Override public String toString() {
    return "QueueFile{"
        + "file=" + file
        + ", zero=" + zero
        + ", versioned=" + versioned
        + ", length=" + fileLength
        + ", size=" + elementCount
        + ", first=" + first
        + ", last=" + last
        + '}';
  }

  /** A pointer to an element. */
  static class Element {
    static final Element NULL = new Element(0, 0);

    /** Length of element header in bytes. */
    static final int HEADER_LENGTH = 4;

    /** Position in file. */
    final long position;

    /** The length of the data. */
    final int length;

    /**
     * Constructs a new element.
     *
     * @param position within file
     * @param length of data
     */
    Element(long position, int length) {
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

  /** Fluent API for creating {@link QueueFile} instances. */
  public static final class Builder {
    final File file;
    boolean zero = true;
    boolean forceLegacy = false;

    /** Start constructing a new queue backed by the given file. */
    public Builder(File file) {
      if (file == null) {
        throw new NullPointerException("file == null");
      }
      this.file = file;
    }

    /** When true, removing an element will also overwrite data with zero bytes. */
    public Builder zero(boolean zero) {
      this.zero = zero;
      return this;
    }

    /** When true, only the legacy (Tape 1.x) format will be used. */
    public Builder forceLegacy(boolean forceLegacy) {
      this.forceLegacy = forceLegacy;
      return this;
    }

    /**
     * Constructs a new queue backed by the given builder. Only one instance should access a given
     * file at a time.
     */
    public QueueFile build() throws IOException {
      RandomAccessFile raf = initializeFromFile(file, forceLegacy);
      QueueFile qf = null;
      try {
        qf = new QueueFile(file, raf, zero, forceLegacy);
        return qf;
      } finally {
        if (qf == null) {
          raf.close();
        }
      }
    }
  }

  /**
   * Use this to throw checked exceptions from iterator methods that do not declare that they throw
   * checked exceptions.
   */
  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  static <T extends Throwable> T getSneakyThrowable(Throwable t) throws T {
    throw (T) t;
  }
}
