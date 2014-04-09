package com.squareup.tape;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.min;

/**
 * A reliable, efficient, file-based, FIFO queue. Additions and removals are
 * O(1). All operations are atomic. Writes are synchronous; data will be written
 * to disk before an operation returns. The underlying file is structured to
 * survive process and even system crashes. If an I/O exception is thrown during
 * a mutating change, the change is aborted. It is safe to continue to use a
 * {@code QueueFile} instance after an exception.
 * <p/>
 * <p>All operations are synchronized. In a traditional queue, the remove
 * operation returns an element. In this queue, {@link #peek} and {@link
 * #remove} are used in conjunction. Use {@code peek} to retrieve the first
 * element, and then {@code remove} to remove it after successful processing. If
 * the system crashes after {@code peek} and during processing, the element will
 * remain in the queue, to be processed when the system restarts.
 * <p/>
 * <p><strong>NOTE:</strong> The current implementation is built
 * for file systems that support atomic segment writes (like YAFFS). Most
 * conventional file systems don't support this; if the power goes out while
 * writing a segment, the segment will contain garbage and the file will be
 * corrupt. We'll add journaling support so this class can be used with more
 * file systems later.
 * <p/>
 * <p>This abstract implementation contains logic shared between BigQueueFileImpl
 * and QueueFileImpl. All operations assume that the file size can extend beyond
 * 2G (this is limited manually within QueueFileImpl). Element sizes are
 * restricted to be less than 2G however (since byte array cannot be larger than
 * that).
 *
 * @author Bob Lee (bob@squareup.com)
 * @author Clement Pang (me@clementpang.com)
 */
public abstract class AbstractQueueFile implements QueueFile {

  private static final Logger LOGGER = Logger.getLogger(AbstractQueueFile.class.getName());

  /**
   * Initial file size in bytes.
   */
  protected static final int INITIAL_LENGTH = 4096; // one file system block
  /**
   * A block of nothing to write over old data.
   */
  protected static final byte[] ZEROES = new byte[INITIAL_LENGTH];

  protected final int headerLength;
  /**
   * In-memory buffer. Big enough to hold the header.
   */
  protected final byte[] buffer;

  /**
   * The underlying file. Uses a ring buffer to store entries. Designed so that
   * a modification isn't committed or visible until we write the header. The
   * header is much smaller than a segment. So long as the underlying file
   * system supports atomic segment writes, changes to the queue are atomic.
   * Storing the file length ensures we can recover from a failed expansion
   * (i.e. if setting the file length succeeds but the process dies before the
   * data can be copied).
   * <p/>
   * Visible for testing.
   */
  protected final RandomAccessFile raf;

  /**
   * Cached file length. Always a power of 2.
   */
  protected long fileLength;

  /**
   * Number of elements.
   */
  protected long elementCount;

  /**
   * Pointer to first (or eldest) element.
   */
  protected Element first;

  /**
   * Pointer to last (or newest) element.
   */
  protected Element last;

  protected AbstractQueueFile(File file, int headerLength) throws IOException {
    this.headerLength = headerLength;
    this.buffer = new byte[headerLength];
    if (!file.exists()) {
      raf = initialize(file);
    } else {
      raf = open(file);
    }
  }

  protected AbstractQueueFile(RandomAccessFile raf, int headerLength) {
    this.headerLength = headerLength;
    this.buffer = new byte[headerLength];
    this.raf = raf;
  }

  /**
   * Opens a random access file that writes synchronously.
   */
  private static RandomAccessFile open(File file) throws FileNotFoundException {
    return new RandomAccessFile(file, "rwd");
  }

  /**
   * Stores int in buffer. The behavior is equivalent to calling {@link
   * java.io.RandomAccessFile#writeInt}.
   */
  protected static void writeInt(byte[] buffer, int offset, int value) {
    buffer[offset] = (byte) (value >> 24);
    buffer[offset + 1] = (byte) (value >> 16);
    buffer[offset + 2] = (byte) (value >> 8);
    buffer[offset + 3] = (byte) value;
  }

  /**
   * Stores int values in buffer. The behavior is equivalent to calling {@link
   * java.io.RandomAccessFile#writeInt} for each value.
   */
  protected static void writeInts(byte[] buffer, int... values) {
    int offset = 0;
    for (int value : values) {
      writeInt(buffer, offset, value);
      offset += 4;
    }
  }

  /**
   * Reads an int from a byte[].
   */
  protected static int readInt(byte[] buffer, int offset) {
    return ((buffer[offset] & 0xff) << 24)
        + ((buffer[offset + 1] & 0xff) << 16)
        + ((buffer[offset + 2] & 0xff) << 8)
        + (buffer[offset + 3] & 0xff);
  }

  /**
   * Returns t unless it's null.
   *
   * @throws NullPointerException if t is null
   */
  protected static <T> T nonNull(T t, String name) {
    if (t == null) throw new NullPointerException(name);
    return t;
  }

  /**
   * Stores long in buffer. The behavior is equivalent to calling {@link
   * java.io.RandomAccessFile#writeLong(long)}.
   */
  protected static void writeLong(byte[] buffer, int offset, long value) {
    ByteBuffer.wrap(buffer).putLong(offset, value);
  }

  /**
   * Stores long values in buffer. The behavior is equivalent to calling {@link
   * java.io.RandomAccessFile#writeLong(long)} for each value.
   */
  protected static void writeLongs(byte[] buffer, long... values) {
    int offset = 0;
    for (long value : values) {
      writeLong(buffer, offset, value);
      offset += 8;
    }
  }

  /**
   * Reads an long from a byte[].
   */
  protected static long readLong(byte[] buffer, int offset) {
    return ByteBuffer.wrap(buffer).getLong(offset);
  }

  /**
   * Initialize the buffer if it does not yet exist.
   *
   * @param file File to initialize. Does not yet exist.
   */
  private RandomAccessFile initialize(File file) throws IOException {
    // Use a temp file so we don't leave a partially-initialized file.
    File tempFile = new File(file.getPath() + ".tmp");
    RandomAccessFile raf = open(tempFile);
    try {
      raf.setLength(INITIAL_LENGTH);
      raf.seek(0);
      writeHeader(raf, INITIAL_LENGTH, 0, 0, 0);
    } finally {
      raf.close();
    }

    // A rename is atomic.
    if (!tempFile.renameTo(file)) throw new IOException("Rename failed!");
    return open(file);
  }

  protected abstract void writeHeader(long fileLength, long elementCount, long firstPosition, long lastPosition) throws IOException;

  protected abstract void writeHeader(RandomAccessFile raf, long fileLength, long elementCount, long firstPosition,
                                      long lastPosition) throws IOException;

  /**
   * Returns the number of used bytes.
   */
  long usedBytes() {
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

  /**
   * Returns number of unused bytes.
   */
  private long remainingBytes() {
    return fileLength - usedBytes();
  }

  /**
   * Returns true if this queue contains no entries.
   */
  public synchronized boolean isEmpty() {
    return elementCount == 0;
  }

  /**
   * Reads the eldest element. Returns null if the queue is empty.
   */
  public synchronized byte[] peek() throws IOException {
    if (isEmpty()) return null;
    int length = first.length;
    byte[] data = new byte[length];
    ringRead(first.position + Element.HEADER_LENGTH, data, 0, length);
    return data;
  }

  /**
   * Invokes reader with the eldest element, if an element is available.
   */
  public synchronized void peek(QueueFile.ElementReader reader) throws IOException {
    if (elementCount > 0) {
      reader.read(new ElementInputStream(first), first.length);
    }
  }

  /**
   * Invokes the given reader once for each element in the queue, from eldest to
   * most recently added.
   */
  public synchronized void forEach(QueueFile.ElementReader reader) throws IOException {
    long position = first.position;
    for (int i = 0; i < elementCount; i++) {
      Element current = readElement(position);
      reader.read(new ElementInputStream(current), current.length);
      position = wrapPosition(current.position + Element.HEADER_LENGTH + current.length);
    }
  }

  @Override
  public void add(byte[] data) throws IOException {
    add(data, 0, data.length);
  }

  /**
   * Adds an element to the end of the queue.
   *
   * @param data   to copy bytes from
   * @param offset to start from in buffer
   * @param count  number of bytes to copy
   * @throws IndexOutOfBoundsException if {@code offset < 0} or {@code count < 0}, or if {@code offset + count} is
   *                                   bigger than the length of {@code buffer}.
   */
  public synchronized void add(byte[] data, int offset, int count) throws IOException {
    nonNull(data, "buffer");
    if ((offset | count) < 0 || count > data.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    expandIfNecessary(count);

    // Insert a new element after the current last element.
    boolean wasEmpty = isEmpty();
    long position = wasEmpty ? headerLength : wrapPosition(last.position + Element.HEADER_LENGTH + last.length);
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
    if (wasEmpty) first = last; // first element
  }

  /**
   * If necessary, expands the file to accommodate an additional element of the
   * given length.
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

    // If the buffer is split, we need to make it contiguous
    if (endOfLastElement <= first.position) {
      FileChannel channel = raf.getChannel();
      channel.position(fileLength); // destination position
      long count = endOfLastElement - Element.HEADER_LENGTH;
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
  }

  /**
   * Writes count bytes from buffer to position in file. Automatically wraps
   * write if position is past the end of the file or if buffer overlaps it.
   *
   * @param position in file to write to
   * @param buffer   to write from
   * @param offset   offset in the buffer to start writing
   * @param count    # of bytes to write
   */
  private void ringWrite(long position, byte[] buffer, int offset, int count) throws IOException {
    position = wrapPosition(position);
    if (position + count <= fileLength) {
      raf.seek(position);
      raf.write(buffer, offset, count);
    } else {
      // The write overlaps the EOF.
      // # of bytes to write before the EOF.
      int beforeEof = (int) (fileLength - position);
      raf.seek(position);
      raf.write(buffer, offset, beforeEof);
      raf.seek(headerLength);
      raf.write(buffer, offset + beforeEof, count - beforeEof);
    }
  }

  private void ringErase(long position, int length) throws IOException {
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
   * @param buffer   to read into
   * @param count    # of bytes to read
   */
  private void ringRead(long position, byte[] buffer, int offset, int count) throws IOException {
    position = wrapPosition(position);
    if (position + count <= fileLength) {
      raf.seek(position);
      raf.readFully(buffer, offset, count);
    } else {
      // The read overlaps the EOF.
      // # of bytes to read before the EOF.
      int beforeEof = (int) (fileLength - position);
      raf.seek(position);
      raf.readFully(buffer, offset, beforeEof);
      raf.seek(headerLength);
      raf.readFully(buffer, offset + beforeEof, count - beforeEof);
    }
  }

  /**
   * Wraps the position if it exceeds the end of the file.
   */
  private long wrapPosition(long position) {
    return position < fileLength ? position
        : headerLength + position - fileLength;
  }

  /**
   * Returns the Element for the given offset.
   */
  protected Element readElement(long position) throws IOException {
    if (position == 0) return Element.NULL;
    raf.seek(position);
    return new Element(position, raf.readInt());
  }

  /**
   * Sets the length of the file.
   */
  private void setLength(long newLength) throws IOException {
    // Set new file length (considered metadata) and sync it to storage.
    raf.setLength(newLength);
    raf.getChannel().force(true);
  }

  /**
   * Clears this queue. Truncates the file to the initial size.
   */
  public synchronized void clear() throws IOException {
    raf.seek(0);
    raf.write(ZEROES);
    writeHeader(INITIAL_LENGTH, 0, 0, 0);
    elementCount = 0;
    first = Element.NULL;
    last = Element.NULL;
    if (fileLength > INITIAL_LENGTH) setLength(INITIAL_LENGTH);
    fileLength = INITIAL_LENGTH;
  }

  /**
   * Returns the number of elements in this queue.
   */
  public synchronized long size() {
    return elementCount;
  }

  /**
   * Removes the eldest element.
   *
   * @throws java.util.NoSuchElementException if the queue is empty
   */
  public synchronized void remove() throws IOException {
    if (isEmpty()) throw new NoSuchElementException();
    if (elementCount == 1) {
      clear();
    } else {
      // assert elementCount > 1
      int firstTotalLength = Element.HEADER_LENGTH + first.length;

      ringErase(first.position, firstTotalLength);

      long newFirstPosition = wrapPosition(first.position + firstTotalLength);
      ringRead(newFirstPosition, buffer, 0, Element.HEADER_LENGTH);
      int length = readInt(buffer, 0);
      writeHeader(fileLength, elementCount - 1, newFirstPosition, last.position);
      elementCount--;
      first = new Element(newFirstPosition, length);
    }
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append(getClass().getSimpleName()).append('[');
    builder.append("fileLength=").append(fileLength);
    builder.append(", size=").append(elementCount);
    builder.append(", first=").append(first);
    builder.append(", last=").append(last);
    builder.append(", element lengths=[");
    try {
      forEach(new QueueFile.ElementReader() {
        boolean first = true;

        @Override
        public void read(InputStream in, int length) throws IOException {
          if (first) {
            first = false;
          } else {
            builder.append(", ");
          }
          builder.append(length);
        }
      });
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "read error", e);
    }
    builder.append("]]");
    return builder.toString();
  }

  /**
   * Closes the underlying file.
   */
  public synchronized void close() throws IOException {
    raf.close();
  }

  /**
   * A pointer to an element.
   */
  static class Element {

    /**
     * Length of element header in bytes.
     */
    static final int HEADER_LENGTH = 4;

    /**
     * Null element.
     */
    static final Element NULL = new Element(0, 0);

    /**
     * Position in file.
     */
    final long position;

    /**
     * The length of the data.
     */
    final int length;

    /**
     * Constructs a new element.
     *
     * @param position within file
     * @param length   of data
     */
    Element(long position, int length) {
      this.position = position;
      this.length = length;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "["
          + "position = " + position
          + ", length = " + length + "]";
    }
  }

  /**
   * Reads a single element.
   */
  private final class ElementInputStream extends InputStream {
    private long position;
    private int remaining;

    private ElementInputStream(Element element) {
      position = wrapPosition(element.position + Element.HEADER_LENGTH);
      remaining = element.length;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      nonNull(buffer, "buffer");
      if ((offset | length) < 0 || length > buffer.length - offset) {
        throw new ArrayIndexOutOfBoundsException();
      }
      if (remaining > 0) {
        if (length > remaining) length = remaining;
        ringRead(position, buffer, offset, length);
        position = wrapPosition(position + length);
        remaining -= length;
        return length;
      } else {
        return -1;
      }
    }

    @Override
    public int read() throws IOException {
      if (remaining == 0) return -1;
      raf.seek(position);
      int b = raf.read();
      position = wrapPosition(position + 1);
      remaining--;
      return b;
    }
  }
}
