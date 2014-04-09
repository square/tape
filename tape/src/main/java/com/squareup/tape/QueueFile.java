package com.squareup.tape;

import java.io.IOException;
import java.io.InputStream;

/**
 * Queue File interface.
 *
 * @author Bob Lee (bob@squareup.com)
 * @author Clement Pang (me@clementpang.com)
 */
public interface QueueFile {
  /**
   * Adds an element to the end of the queue.
   *
   * @param data to copy bytes from
   */
  void add(byte[] data) throws IOException;

  /**
   * Adds an element to the end of the queue.
   *
   * @param data   to copy bytes from
   * @param offset to start from in buffer
   * @param count  number of bytes to copy
   * @throws IndexOutOfBoundsException if {@code offset < 0} or {@code count < 0}, or if {@code offset + count} is
   *                                   bigger than the length of {@code buffer}.
   */
  void add(byte[] data, int offset, int count) throws IOException;

  /**
   * Returns true if this queue contains no entries.
   */
  boolean isEmpty();

  /**
   * Reads the eldest element. Returns null if the queue is empty.
   */
  byte[] peek() throws IOException;

  /**
   * Invokes reader with the eldest element, if an element is available.
   */
  void peek(ElementReader reader) throws IOException;

  /**
   * Invokes the given reader once for each element in the queue, from eldest to
   * most recently added.
   */
  void forEach(ElementReader reader) throws IOException;

  /**
   * Returns the number of elements in this queue.
   */
  long size();

  /**
   * Removes the eldest element.
   *
   * @throws java.util.NoSuchElementException if the queue is empty
   */
  void remove() throws IOException;

  /**
   * Clears this queue. Truncates the file to the initial size.
   */
  void clear() throws IOException;

  /**
   * Closes the underlying file.
   */
  void close() throws IOException;

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
