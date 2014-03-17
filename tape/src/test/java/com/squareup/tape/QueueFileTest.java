// Copyright 2010 Square, Inc.
package com.squareup.tape;

import com.squareup.tape.QueueFile.Element;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.squareup.tape.QueueFile.HEADER_LENGTH;
import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

/**
 * Tests for QueueFile.
 *
 * @author Bob Lee (bob@squareup.com)
 */
@SuppressWarnings({"ResultOfMethodCallIgnored"})
public class QueueFileTest {
  private static final Logger logger =
      Logger.getLogger(QueueFileTest.class.getName());

  /**
   * Takes up 33401 bytes in the queue (N*(N+1)/2+4*N). Picked 254 instead of
   * 255 so that the number of bytes isn't a multiple of 4.
   */
  private static int N = 254;
  private static byte[][] values = new byte[N][];

  static {
    for (int i = 0; i < N; i++) {
      byte[] value = new byte[i];
      // Example: values[3] = { 3, 2, 1 }
      for (int ii = 0; ii < i; ii++) value[ii] = (byte) (i - ii);
      values[i] = value;
    }
  }

  private File file;

  @Before public void setUp() throws Exception {
    file = File.createTempFile("test.queue", null);
    file.delete();
  }

  @After public void tearDown() throws Exception {
    file.delete();
  }

  @Test public void testAddOneElement() throws IOException {
    // This test ensures that we update 'first' correctly.
    QueueFile queue = new QueueFile(file);
    byte[] expected = values[253];
    queue.add(expected);
    assertThat(queue.peek()).isEqualTo(expected);
    queue.close();
    queue = new QueueFile(file);
    assertThat(queue.peek()).isEqualTo(expected);
  }

  @Test public void testClearErases() throws IOException {
    QueueFile queue = new QueueFile(file);
    byte[] expected = values[253];
    queue.add(expected);

    // Confirm that the data was in the file before we cleared.
    byte[] data = new byte[expected.length];
    queue.raf.seek(HEADER_LENGTH + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, expected.length);
    assertThat(data).isEqualTo(expected);

    queue.clear();

    // Should have been erased.
    queue.raf.seek(HEADER_LENGTH + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, expected.length);
    assertThat(data).isEqualTo(new byte[expected.length]);
  }

  @Test public void testClearDoesNotCorrupt() throws IOException {
    QueueFile queue = new QueueFile(file);
    byte[] stuff = values[253];
    queue.add(stuff);
    queue.clear();

    queue = new QueueFile(file);
    assertThat(queue.isEmpty()).isTrue();
    assertThat(queue.peek()).isNull();

    queue.add(values[25]);
    assertThat(queue.peek()).isEqualTo(values[25]);
  }

  @Test public void removeErasesEagerly() throws IOException {
    QueueFile queue = new QueueFile(file);

    byte[] firstStuff = values[127];
    queue.add(firstStuff);

    byte[] secondStuff = values[253];
    queue.add(secondStuff);

    // Confirm that first stuff was in the file before we remove.
    byte[] data = new byte[firstStuff.length];
    queue.raf.seek(HEADER_LENGTH + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, firstStuff.length);
    assertThat(data).isEqualTo(firstStuff);

    queue.remove();

    // Next record is intact
    assertThat(queue.peek()).isEqualTo(secondStuff);

    // First should have been erased.
    queue.raf.seek(HEADER_LENGTH + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, firstStuff.length);
    assertThat(data).isEqualTo(new byte[firstStuff.length]);
  }

  @Test public void testZeroSizeInHeaderComplains() throws IOException {
    RandomAccessFile emptyFile = new RandomAccessFile(file, "rwd");
    emptyFile.setLength(4096);
    emptyFile.getChannel().force(true);
    emptyFile.close();

    try {
      new QueueFile(file);
      fail("Should have complained about bad header length");
    } catch (IOException ex) {
      assertThat(ex).hasMessage("File is corrupt; length stored in header is 0.");
    }
  }

  @Test public void removeDoesNotCorrupt() throws IOException {
    QueueFile queue = new QueueFile(file);

    queue.add(values[127]);
    byte[] secondStuff = values[253];
    queue.add(secondStuff);
    queue.remove();

    queue = new QueueFile(file);
    assertThat(queue.peek()).isEqualTo(secondStuff);
  }

  @Test public void removingBigDamnBlocksErasesEffectively() throws IOException {
    byte[] bigBoy = new byte[7000];
    for (int i = 0; i < 7000; i += 100) {
      System.arraycopy(values[100], 0, bigBoy, i, values[100].length);
    }

    QueueFile queue = new QueueFile(file);

    queue.add(bigBoy);
    byte[] secondStuff = values[123];
    queue.add(secondStuff);

    // Confirm that bigBoy was in the file before we remove.
    byte[] data = new byte[bigBoy.length];
    queue.raf.seek(HEADER_LENGTH + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, bigBoy.length);
    assertThat(data).isEqualTo(bigBoy);

    queue.remove();

    // Next record is intact
    assertThat(queue.peek()).isEqualTo(secondStuff);

    // First should have been erased.
    queue.raf.seek(HEADER_LENGTH + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, bigBoy.length);
    assertThat(data).isEqualTo(new byte[bigBoy.length]);
  }

  @Test public void testAddAndRemoveElements() throws IOException {
    long start = System.nanoTime();

    Queue<byte[]> expected = new LinkedList<byte[]>();

    for (int round = 0; round < 5; round++) {
      QueueFile queue = new QueueFile(file);
      for (int i = 0; i < N; i++) {
        queue.add(values[i]);
        expected.add(values[i]);
      }

      // Leave N elements in round N, 15 total for 5 rounds. Removing all the
      // elements would be like starting with an empty queue.
      for (int i = 0; i < N - round - 1; i++) {
        assertThat(queue.peek()).isEqualTo(expected.remove());
        queue.remove();
      }
      queue.close();
    }

    // Remove and validate remaining 15 elements.
    QueueFile queue = new QueueFile(file);
    assertThat(queue.size()).isEqualTo(15);
    assertThat(queue.size()).isEqualTo(expected.size());
    while (!expected.isEmpty()) {
      assertThat(queue.peek()).isEqualTo(expected.remove());
      queue.remove();
    }
    queue.close();

    // length() returns 0, but I checked the size w/ 'ls', and it is correct.
    // assertEquals(65536, file.length());

    logger.info("Ran in " + ((System.nanoTime() - start) / 1000000) + "ms.");
  }

  /** Tests queue expansion when the data crosses EOF. */
  @Test public void testSplitExpansion() throws IOException {
    // This should result in 3560 bytes.
    int max = 80;

    Queue<byte[]> expected = new LinkedList<byte[]>();
    QueueFile queue = new QueueFile(file);

    for (int i = 0; i < max; i++) {
      expected.add(values[i]);
      queue.add(values[i]);
    }

    // Remove all but 1.
    for (int i = 1; i < max; i++) {
      assertThat(queue.peek()).isEqualTo(expected.remove());
      queue.remove();
    }

    // This should wrap around before expanding.
    for (int i = 0; i < N; i++) {
      expected.add(values[i]);
      queue.add(values[i]);
    }

    while (!expected.isEmpty()) {
      assertThat(queue.peek()).isEqualTo(expected.remove());
      queue.remove();
    }

    queue.close();
  }

  @Test public void testFailedAdd() throws IOException {
    QueueFile queueFile = new QueueFile(file);
    queueFile.add(values[253]);
    queueFile.close();

    final BrokenRandomAccessFile braf = new BrokenRandomAccessFile(file, "rwd");
    queueFile = new QueueFile(braf);

    try {
      queueFile.add(values[252]);
      fail();
    } catch (IOException e) { /* expected */ }

    braf.rejectCommit = false;

    // Allow a subsequent add to succeed.
    queueFile.add(values[251]);

    queueFile.close();

    queueFile = new QueueFile(file);
    Assertions.assertThat(queueFile.size()).isEqualTo(2);
    assertThat(queueFile.peek()).isEqualTo(values[253]);
    queueFile.remove();
    assertThat(queueFile.peek()).isEqualTo(values[251]);
  }

  @Test public void testFailedRemoval() throws IOException {
    QueueFile queueFile = new QueueFile(file);
    queueFile.add(values[253]);
    queueFile.close();

    final BrokenRandomAccessFile braf = new BrokenRandomAccessFile(file, "rwd");
    queueFile = new QueueFile(braf);

    try {
      queueFile.remove();
      fail();
    } catch (IOException e) { /* expected */ }

    queueFile.close();

    queueFile = new QueueFile(file);
    assertThat(queueFile.size()).isEqualTo(1);
    assertThat(queueFile.peek()).isEqualTo(values[253]);

    queueFile.add(values[99]);
    queueFile.remove();
    assertThat(queueFile.peek()).isEqualTo(values[99]);
  }

  @Test public void testFailedExpansion() throws IOException {
    QueueFile queueFile = new QueueFile(file);
    queueFile.add(values[253]);
    queueFile.close();

    final BrokenRandomAccessFile braf = new BrokenRandomAccessFile(file, "rwd");
    queueFile = new QueueFile(braf);

    try {
      // This should trigger an expansion which should fail.
      queueFile.add(new byte[8000]);
      fail();
    } catch (IOException e) { /* expected */ }

    queueFile.close();

    queueFile = new QueueFile(file);

    assertThat(queueFile.size()).isEqualTo(1);
    assertThat(queueFile.peek()).isEqualTo(values[253]);
    assertThat(queueFile.fileLength).isEqualTo(4096);

    queueFile.add(values[99]);
    queueFile.remove();
    assertThat(queueFile.peek()).isEqualTo(values[99]);
  }

  @Test public void testPeekWithElementReader() throws IOException {
    QueueFile queueFile = new QueueFile(file);
    final byte[] a = {1, 2};
    queueFile.add(a);
    final byte[] b = {3, 4, 5};
    queueFile.add(b);

    final AtomicInteger peeks = new AtomicInteger(0);

    queueFile.peek(new QueueFile.ElementReader() {
      @Override public void read(InputStream in, int length) throws IOException {
        peeks.incrementAndGet();

        assertThat(length).isEqualTo(2);
        byte[] actual = new byte[length];
        in.read(actual);
        assertThat(actual).isEqualTo(a);
      }
    });

    queueFile.peek(new QueueFile.ElementReader() {
      @Override public void read(InputStream in, int length) throws IOException {
        peeks.incrementAndGet();

        assertThat(length).isEqualTo(2);
        assertThat(in.read()).isEqualTo(1);
        assertThat(in.read()).isEqualTo(2);
        assertThat(in.read()).isEqualTo(-1);
      }
    });

    queueFile.remove();

    queueFile.peek(new QueueFile.ElementReader() {
      @Override public void read(InputStream in, int length) throws IOException {
        peeks.incrementAndGet();

        assertThat(length).isEqualTo(3);
        byte[] actual = new byte[length];
        in.read(actual);
        assertThat(actual).isEqualTo(b);
      }
    });

    assertThat(peeks.get()).isEqualTo(3);
    assertThat(queueFile.peek()).isEqualTo(b);
    assertThat(queueFile.size()).isEqualTo(1);
  }

  @Test public void testForEach() throws IOException {
    QueueFile queueFile = new QueueFile(file);

    final byte[] a = {1, 2};
    queueFile.add(a);
    final byte[] b = {3, 4, 5};
    queueFile.add(b);

    final int[] iteration = new int[]{0};
    QueueFile.ElementReader elementReader = new QueueFile.ElementReader() {
      @Override public void read(InputStream in, int length) throws IOException {
        if (iteration[0] == 0) {
          assertThat(length).isEqualTo(2);
          byte[] actual = new byte[length];
          in.read(actual);
          assertThat(actual).isEqualTo(a);
        } else if (iteration[0] == 1) {
          assertThat(length).isEqualTo(3);
          byte[] actual = new byte[length];
          in.read(actual);
          assertThat(actual).isEqualTo(b);
        } else {
          fail();
        }
        iteration[0]++;
      }
    };

    queueFile.forEach(elementReader);

    assertThat(queueFile.peek()).isEqualTo(a);
    assertThat(iteration[0]).isEqualTo(2);
  }

  @Test public void testForEachReadWithOffset() throws IOException {
      QueueFile queueFile = new QueueFile(file);

      queueFile.add(new byte[] {1, 2});
      queueFile.add(new byte[] {3, 4, 5});

      final byte[] actual = new byte[5];
      final int[] offset = new int[] {0};

      QueueFile.ElementReader elementReader = new QueueFile.ElementReader() {
        @Override public void read(InputStream in, int length) throws IOException {
          in.read(actual, offset[0], length);
          offset[0] += length;
        }
      };

      queueFile.forEach(elementReader);

      assertThat(actual).isEqualTo(new byte[] {1, 2, 3, 4, 5});
    }

  @Test public void testForEachStreamCopy() throws IOException {
    final QueueFile queueFile = new QueueFile(file);
    queueFile.add(new byte[] {1, 2});
    queueFile.add(new byte[] {3, 4, 5});

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final byte[] buffer = new byte[8];

    final QueueFile.ElementReader elementReader = new QueueFile.ElementReader() {
      @Override public void read(InputStream in, int length) throws IOException {
        // A common idiom for copying data between two streams, but it depends on the
        // InputStream correctly returning -1 when no more data is available
        int count;
        while ((count = in.read(buffer)) != -1) {
          if (count == 0) {
            // In the past, the ElementInputStream.read(byte[], int, int) method would return 0
            // when no more bytes were available for reading. This test detects that error.
            //
            // Note: 0 is a valid return value for InputStream.read(byte[], int, int), which happens
            // when the passed length is zero. We could trigger that through InputStream.read(byte[])
            // by passing a zero-length buffer. However, since we won't do that during this test,
            // we can safely assume that a return value of 0 indicates the past error in logic.
            fail("This test should never receive a result of 0 from InputStream.read(byte[])");
          }
          baos.write(buffer, 0, count);
        }
      }
    };

    queueFile.forEach(elementReader);
    assertThat(baos.toByteArray()).isEqualTo(new byte[] {1, 2, 3, 4, 5});
  }

  /**
   * Exercise a bug where wrapped elements were getting corrupted when the
   * QueueFile was forced to expand in size and a portion of the final Element
   * had been wrapped into space at the beginning of the file.
   */
  @Test public void testFileExpansionDoesntCorruptWrappedElements()
      throws IOException {
    QueueFile queue = new QueueFile(file);

    // Create test data - 1k blocks marked consecutively 1, 2, 3, 4 and 5.
    byte[][] values = new byte[5][];
    for (int blockNum = 0; blockNum < values.length; blockNum++) {
      values[blockNum] = new byte[1024];
      for (int i = 0; i < values[blockNum].length; i++) {
        values[blockNum][i] = (byte) (blockNum + 1);
      }
    }

    // First, add the first two blocks to the queue, remove one leaving a
    // 1K space at the beginning of the buffer.
    queue.add(values[0]);
    queue.add(values[1]);
    queue.remove();

    // The trailing end of block "4" will be wrapped to the start of the buffer.
    queue.add(values[2]);
    queue.add(values[3]);

    // Cause buffer to expand as there isn't space between the end of block "4"
    // and the start of block "2".  Internally the queue should cause block "4"
    // to be contiguous, but there was a bug where that wasn't happening.
    queue.add(values[4]);

    // Make sure values are not corrupted, specifically block "4" that wasn't
    // being made contiguous in the version with the bug.
    for (int blockNum = 1; blockNum < values.length; blockNum++) {
      byte[] value = queue.peek();
      queue.remove();

      for (int i = 0; i < value.length; i++) {
        assertThat(value[i]).isEqualTo((byte) (blockNum + 1))
            .as("Block " + (blockNum + 1) + " corrupted at byte index " + i);
      }
    }

    queue.close();
  }

  /**
   * Exercise a bug where wrapped elements were getting corrupted when the
   * QueueFile was forced to expand in size and a portion of the final Element
   * had been wrapped into space at the beginning of the file - if multiple
   * Elements have been written to empty buffer space at the start does the
   * expansion correctly update all their positions?
   */
  @Test public void testFileExpansionCorrectlyMovesElements() throws IOException {
    QueueFile queue = new QueueFile(file);

    // Create test data - 1k blocks marked consecutively 1, 2, 3, 4 and 5.
    byte[][] values = new byte[5][];
    for (int blockNum = 0; blockNum < values.length; blockNum++) {
      values[blockNum] = new byte[1024];
      for (int i = 0; i < values[blockNum].length; i++) {
        values[blockNum][i] = (byte) (blockNum + 1);
      }
    }

    // smaller data elements
    byte[][] smaller = new byte[3][];
    for (int blockNum = 0; blockNum < smaller.length; blockNum++) {
      smaller[blockNum] = new byte[256];
      for (int i = 0; i < smaller[blockNum].length; i++) {
        smaller[blockNum][i] = (byte) (blockNum + 6);
      }
    }

    // First, add the first two blocks to the queue, remove one leaving a
    // 1K space at the beginning of the buffer.
    queue.add(values[0]);
    queue.add(values[1]);
    queue.remove();

    // The trailing end of block "4" will be wrapped to the start of the buffer.
    queue.add(values[2]);
    queue.add(values[3]);

    // Now fill in some space with smaller blocks, none of which will cause
    // an expansion.
    queue.add(smaller[0]);
    queue.add(smaller[1]);
    queue.add(smaller[2]);

    // Cause buffer to expand as there isn't space between the end of the
    // smaller block "8" and the start of block "2".  Internally the queue
    // should cause all of tbe smaller blocks, and the trailing end of
    // block "5" to be moved to the end of the file.
    queue.add(values[4]);

    byte[] expectedBlockNumbers = {2, 3, 4, 6, 7, 8, 5};

    // Make sure values are not corrupted, specifically block "4" that wasn't
    // being made contiguous in the version with the bug.
    for (byte expectedBlockNumber : expectedBlockNumbers) {
      byte[] value = queue.peek();
      queue.remove();

      for (int i = 0; i < value.length; i++) {
        assertThat(value[i]).isEqualTo(expectedBlockNumber)
            .as("Block " + expectedBlockNumber + " corrupted at byte index " + i);
      }
    }

    queue.close();
  }

  /**
   * Exercise a bug where an expanding queue file where the start and end positions
   * are the same causes corruption.
   */
  @Test public void testSaturatedFileExpansionMovesElements() throws IOException {
    QueueFile queue = new QueueFile(file);

    // Create test data - 1016-byte blocks marked consecutively 1, 2, 3, 4, 5 and 6,
    // four of which perfectly fill the queue file, taking into account the file header
    // and the item headers.
    // Each item is of length
    // (QueueFile.INITIAL_LENGTH - QueueFile.HEADER_LENGTH) / 4 - element_header_length
    // = 1016 bytes
    byte[][] values = new byte[6][];
    for (int blockNum = 0; blockNum < values.length; blockNum++) {
      values[blockNum] = new byte[1016];
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

  /**
   * A RandomAccessFile that can break when you go to write the COMMITTED
   * status.
   */
  static class BrokenRandomAccessFile extends RandomAccessFile {
    boolean rejectCommit = true;

    BrokenRandomAccessFile(File file, String mode)
        throws FileNotFoundException {
      super(file, mode);
    }

    @Override public void write(byte[] buffer) throws IOException {
      if (rejectCommit && getFilePointer() == 0) {
        throw new IOException("No commit for you!");
      }
      super.write(buffer);
    }
  }
}
