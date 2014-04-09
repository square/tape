package com.squareup.tape;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Tests for BigQueueFileImpl.
 *
 * @author Bob Lee (bob@squareup.com)
 */
@SuppressWarnings({"ResultOfMethodCallIgnored"})
public class BigQueueFileImplTest extends AbstractQueueFileTest {

  @Override
  protected AbstractQueueFile getQueueFile(File file) throws IOException {
    return new BigQueueFileImpl(file);
  }

  @Override
  protected AbstractQueueFile getQueueFile(RandomAccessFile raf) throws IOException {
    return new BigQueueFileImpl(raf);
  }

  @Override
  protected int getHeaderLength() throws IOException {
    return BigQueueFileImpl.HEADER_LENGTH;
  }

  /**
   * Exercise a bug where an expanding queue file where the start and end positions
   * are the same causes corruption.
   */
  @Test
  public void testSaturatedFileExpansionMovesElements() throws IOException {
    BigQueueFileImpl queue = new BigQueueFileImpl(file);

    // Create test data - 1016-byte blocks marked consecutively 1, 2, 3, 4, 5 and 6,
    // four of which perfectly fill the queue file, taking into account the file header
    // and the item headers.
    // Each item is of length
    // (AbstractQueueFile.INITIAL_LENGTH - BigQueueFileImpl.HEADER_LENGTH) / 4 - element_header_length
    // = 1008 bytes
    byte[][] values = new byte[6][];
    for (int blockNum = 0; blockNum < values.length; blockNum++) {
      values[blockNum] = new byte[1008];
      for (int i = 0; i < values[blockNum].length; i++) {
        values[blockNum][i] = (byte) (blockNum + 1);
      }
    }

    // Saturate the queue file
    queue.add(values[0]);
    queue.add(values[1]);
    queue.add(values[2]);
    queue.add(values[3]);

    // Remove an element and add a new one so that the position of the start and
    // end of the queue are equal
    queue.remove();
    queue.add(values[4]);

    // Cause the queue file to expand
    queue.add(values[5]);

    // Make sure values are not corrupted
    for (int i = 1; i < 6; i++) {
      assertThat(queue.peek()).isEqualTo(values[i]);
      queue.remove();
    }

    queue.close();
  }
}
