package com.squareup.tape;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * A QueueFile implementation that supports files larger than 2G. Note that files are NOT
 * compatible with {@link QueueFileImpl} since the header format is different.
 * <p>Buffer file format:
 * <pre>
 *   Format:
 *     Header              (32 bytes)
 *     Element Ring Buffer (File Length - 32 bytes)
 *
 *   Header:
 *     File Length            (8 bytes)
 *     Element Count          (8 bytes)
 *     First Element Position (8 bytes, =0 if null)
 *     Last Element Position  (8 bytes, =0 if null)
 *
 *   Element:
 *     Length (4 bytes)
 *     Data   (Length bytes)
 * </pre>
 *
 * @author Clement Pang (me@clementpang.com)
 */
public class BigQueueFileImpl extends AbstractQueueFile {

  /**
   * Length of header in bytes.
   */
  static final int HEADER_LENGTH = 32;

  /**
   * Constructs a new queue backed by the given file. Only one {@code QueueFile}
   * instance should access a given file at a time.
   */
  public BigQueueFileImpl(File file) throws IOException {
    super(file, HEADER_LENGTH);
    readHeader();
  }

  /**
   * For testing.
   */
  BigQueueFileImpl(RandomAccessFile raf) throws IOException {
    super(raf, HEADER_LENGTH);
    readHeader();
  }

  /**
   * Reads the header.
   */
  private void readHeader() throws IOException {
    raf.seek(0);
    raf.readFully(buffer);
    fileLength = readLong(buffer, 0);
    if (fileLength > raf.length()) {
      throw new IOException("File is truncated. Expected length: " + fileLength + ", Actual length: " + raf.length());
    } else if (fileLength == 0) {
      throw new IOException("File is corrupt; length stored in header is 0.");
    }
    elementCount = readLong(buffer, 8);
    long firstOffset = readLong(buffer, 16);
    long lastOffset = readLong(buffer, 24);
    first = readElement(firstOffset);
    last = readElement(lastOffset);
  }

  @Override
  protected void writeHeader(long fileLength, long elementCount, long firstPosition, long lastPosition) throws IOException {
    writeHeader(raf, fileLength, elementCount, firstPosition, lastPosition);
  }

  /**
   * Writes header atomically. The arguments contain the updated values. The
   * class member fields should not have changed yet. This only updates the
   * state in the file. It's up to the caller to update the class member
   * variables *after* this call succeeds. Assumes segment writes are atomic in
   * the underlying file system.
   */
  @Override
  protected void writeHeader(RandomAccessFile raf, long fileLength, long elementCount, long firstPosition, long lastPosition) throws IOException {
    writeLongs(buffer, fileLength, elementCount, firstPosition, lastPosition);
    raf.seek(0);
    raf.write(buffer);
  }
}
