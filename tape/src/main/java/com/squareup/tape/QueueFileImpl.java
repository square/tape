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
package com.squareup.tape;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Legacy QueueFile implementation that can only support file sizes up to 2G
 * and element count up to 2m.
 * <p/>
 * <p>Buffer file format:
 * <pre>
 *   Format:
 *     Header              (16 bytes)
 *     Element Ring Buffer (File Length - 16 bytes)
 *
 *   Header:
 *     File Length            (4 bytes)
 *     Element Count          (4 bytes)
 *     First Element Position (4 bytes, =0 if null)
 *     Last Element Position  (4 bytes, =0 if null)
 *
 *   Element:
 *     Length (4 bytes)
 *     Data   (Length bytes)
 * </pre>
 *
 * @author Bob Lee (bob@squareup.com)
 */
public class QueueFileImpl extends AbstractQueueFile {

  /**
   * Length of header in bytes.
   */
  static final int HEADER_LENGTH = 16;

  /**
   * Constructs a new queue backed by the given file. Only one {@code QueueFile}
   * instance should access a given file at a time.
   */
  public QueueFileImpl(File file) throws IOException {
    super(file, HEADER_LENGTH);
    readHeader();
  }

  /**
   * For testing.
   */
  QueueFileImpl(RandomAccessFile raf) throws IOException {
    super(raf, HEADER_LENGTH);
    readHeader();
  }

  /**
   * Reads the header.
   */
  private void readHeader() throws IOException {
    raf.seek(0);
    raf.readFully(buffer);
    fileLength = readInt(buffer, 0);
    if (fileLength > raf.length()) {
      throw new IOException("File is truncated. Expected length: " + fileLength + ", Actual length: " + raf.length());
    } else if (fileLength == 0) {
      throw new IOException("File is corrupt; length stored in header is 0.");
    }
    elementCount = readInt(buffer, 4);
    int firstOffset = readInt(buffer, 8);
    int lastOffset = readInt(buffer, 12);
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
    if (fileLength > Integer.MAX_VALUE) {
      throw new IOException("file sizes larger than 2G is not supported");
    }
    if (elementCount > Integer.MAX_VALUE) {
      throw new IOException("elementCount larger than 2m is not supported");
    }
    if (firstPosition >= Integer.MAX_VALUE) {
      throw new IOException("firstPosition is invalid");
    }
    if (lastPosition >= Integer.MAX_VALUE) {
      throw new IOException("lastPosition is invalid");
    }
    writeInts(buffer, (int) fileLength, (int) elementCount, (int) firstPosition, (int) lastPosition);
    raf.seek(0);
    raf.write(buffer);
  }

  @Override
  protected long getMaxFileSize() {
    return Integer.MAX_VALUE;
  }
}
