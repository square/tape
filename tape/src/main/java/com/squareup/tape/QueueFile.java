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

import java.io.IOException;
import java.io.InputStream;

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
 *
 * @author Bob Lee (bob@squareup.com)
 */
public interface QueueFile {

  /** for testing */
  int getFileLength();

  /**
   * Adds an element to the end of the queue.
   *
   * @param data to copy bytes from
   */
  public void add(byte[] data) throws IOException;
  
  /**
   * Adds an element to the end of the queue.
   *
   * @param data   to copy bytes from
   * @param offset to start from in buffer
   * @param count  number of bytes to copy
   * @throws IndexOutOfBoundsException if {@code offset < 0} or {@code count < 0}, or if {@code offset + count} is
   *                                   bigger than the length of {@code buffer}.
   */
  public void add(byte[] data, int offset, int count) throws IOException;
  

  /** Reads the eldest element. Returns null if the queue is empty. */
  public byte[] peek() throws IOException;

  /** Invokes reader with the eldest element, if an element is available. */
  public void peek(ElementReader reader) throws IOException;

  /**
   * Invokes the given reader once for each element in the queue, from eldest to
   * most recently added.
   */
  public void forEach(ElementReader reader) throws IOException;


  /** Returns the number of elements in this queue. */
  public int size();

  /**
   * Removes the eldest element.
   *
   * @throws java.util.NoSuchElementException if the queue is empty
   */
  public void remove() throws IOException;

  /** Clears this queue. Truncates the file to the initial size. */
  public void clear() throws IOException;

  /** Closes the underlying file. */
  public void close() throws IOException;

  @Override public String toString();

   /**
   * Reads queue elements. Enables partial reads as opposed to reading all of
   * the bytes into a byte[].
   */
  public interface ElementReader {

    /*
     * TODO: Support remove() call from read().
     */

    /**
     * Called once per element.
     *
     * @param in     stream of element data. Reads as many bytes as requested,
     *               unless fewer than the request number of bytes remains, in
     *               which case it reads all the remaining bytes. Not buffered.
     * @param length of element data in bytes
     */
    void read(InputStream in, int length) throws IOException;
  }
}
