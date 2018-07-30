// Copyright 2010 Square, Inc.
package com.squareup.tape2;

import com.squareup.tape2.QueueFile.Element;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.logging.Logger;
import okio.BufferedSource;
import okio.Okio;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.google.common.truth.Truth.assertThat;
import static com.squareup.tape2.QueueFile.INITIAL_LENGTH;
import static org.junit.Assert.fail;

/**
 * Tests for QueueFile.
 *
 * @author Bob Lee (bob@squareup.com)
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
@RunWith(Parameterized.class)
public class QueueFileTest {
  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {"Legacy"   , true , 16},
        new Object[] {"Versioned", false, 32}
    );
  }

  private static final Logger logger = Logger.getLogger(QueueFileTest.class.getName());

  /**
   * Takes up 33401 bytes in the queue (N*(N+1)/2+4*N). Picked 254 instead of 255 so that the number
   * of bytes isn't a multiple of 4.
   */
  private static final int N = 254;
  private static byte[][] values = new byte[N][];

  static {
    for (int i = 0; i < N; i++) {
      byte[] value = new byte[i];
      // Example: values[3] = { 3, 2, 1 }
      for (int ii = 0; ii < i; ii++)
        value[ii] = (byte) (i - ii);
      values[i] = value;
    }
  }

  private final boolean forceLegacy;
  private final int headerLength;

  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private File file;

  public QueueFileTest(String name, boolean forceLegacy, int headerLength) {
    this.forceLegacy = forceLegacy;
    this.headerLength = headerLength;
  }

  private QueueFile newQueueFile() throws IOException {
    return newQueueFile(true);
  }

  private QueueFile newQueueFile(RandomAccessFile raf) throws IOException {
    return new QueueFile(this.file, raf, true, forceLegacy);
  }

  private QueueFile newQueueFile(boolean zero) throws IOException {
    return new QueueFile.Builder(file).zero(zero).forceLegacy(forceLegacy).build();
  }

  @Before public void setUp() throws Exception {
    File parent = folder.getRoot();
    file = new File(parent, "queue-file");
  }

  @Test public void testAddOneElement() throws IOException {
    // This test ensures that we update 'first' correctly.
    QueueFile queue = newQueueFile();
    byte[] expected = values[253];
    queue.add(expected);
    assertThat(queue.peek()).isEqualTo(expected);
    queue.close();
    queue = newQueueFile();
    assertThat(queue.peek()).isEqualTo(expected);
  }

  @Test public void testClearErases() throws IOException {
    QueueFile queue = newQueueFile();
    byte[] expected = values[253];
    queue.add(expected);

    // Confirm that the data was in the file before we cleared.
    byte[] data = new byte[expected.length];
    queue.raf.seek(headerLength + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, expected.length);
    assertThat(data).isEqualTo(expected);

    queue.clear();

    // Should have been erased.
    queue.raf.seek(headerLength + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, expected.length);
    assertThat(data).isEqualTo(new byte[expected.length]);
  }

  @Test public void testClearDoesNotCorrupt() throws IOException {
    QueueFile queue = newQueueFile();
    byte[] stuff = values[253];
    queue.add(stuff);
    queue.clear();

    queue = newQueueFile();
    assertThat(queue.isEmpty()).isTrue();
    assertThat(queue.peek()).isNull();

    queue.add(values[25]);
    assertThat(queue.peek()).isEqualTo(values[25]);
  }

  @Test public void removeErasesEagerly() throws IOException {
    QueueFile queue = newQueueFile();

    byte[] firstStuff = values[127];
    queue.add(firstStuff);

    byte[] secondStuff = values[253];
    queue.add(secondStuff);

    // Confirm that first stuff was in the file before we remove.
    byte[] data = new byte[firstStuff.length];
    queue.raf.seek(headerLength + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, firstStuff.length);
    assertThat(data).isEqualTo(firstStuff);

    queue.remove();

    // Next record is intact
    assertThat(queue.peek()).isEqualTo(secondStuff);

    // First should have been erased.
    queue.raf.seek(headerLength + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, firstStuff.length);
    assertThat(data).isEqualTo(new byte[firstStuff.length]);
  }

  @Test public void testZeroSizeInHeaderThrows() throws IOException {
    RandomAccessFile emptyFile = new RandomAccessFile(file, "rwd");
    emptyFile.setLength(INITIAL_LENGTH);
    emptyFile.getChannel().force(true);
    emptyFile.close();

    try {
      newQueueFile();
      fail("Should have thrown about bad header length");
    } catch (IOException ex) {
      assertThat(ex).hasMessageThat()
          .isEqualTo("File is corrupt; length stored in header (0) is invalid.");
    }
  }

  @Test public void testSizeLessThanHeaderThrows() throws IOException {
    RandomAccessFile emptyFile = new RandomAccessFile(file, "rwd");
    emptyFile.setLength(INITIAL_LENGTH);
    if (forceLegacy) {
      emptyFile.writeInt(headerLength - 1);
    } else {
      emptyFile.writeInt(0x80000001);
      emptyFile.writeLong(headerLength - 1);
    }
    emptyFile.getChannel().force(true);
    emptyFile.close();

    try {
      newQueueFile();
      fail();
    } catch (IOException ex) {
      assertThat(ex.getMessage()).isIn(
          Arrays.asList("File is corrupt; length stored in header (15) is invalid.",
              "File is corrupt; length stored in header (31) is invalid."));
    }
  }

  @Test public void testNegativeSizeInHeaderThrows() throws IOException {
    RandomAccessFile emptyFile = new RandomAccessFile(file, "rwd");
    emptyFile.seek(0);
    emptyFile.writeInt(-2147483648);
    emptyFile.setLength(INITIAL_LENGTH);
    emptyFile.getChannel().force(true);
    emptyFile.close();

    try {
      newQueueFile();
      fail("Should have thrown about bad header length");
    } catch (IOException ex) {
      assertThat(ex.getMessage()).isIn(
          Arrays.asList("File is corrupt; length stored in header (-2147483648) is invalid.",
              "Unable to read version 0 format. Supported versions are 1 and legacy."));
    }
  }

  @Test public void removeMultipleDoesNotCorrupt() throws IOException {
    QueueFile queue = newQueueFile();
    for (int i = 0; i < 10; i++) {
      queue.add(values[i]);
    }

    queue.remove(1);
    assertThat(queue.size()).isEqualTo(9);
    assertThat(queue.peek()).isEqualTo(values[1]);

    queue.remove(3);
    queue = newQueueFile();
    assertThat(queue.size()).isEqualTo(6);
    assertThat(queue.peek()).isEqualTo(values[4]);

    queue.remove(6);
    assertThat(queue.isEmpty()).isTrue();
    assertThat(queue.peek()).isNull();
  }

  @Test public void removeDoesNotCorrupt() throws IOException {
    QueueFile queue = newQueueFile();

    queue.add(values[127]);
    byte[] secondStuff = values[253];
    queue.add(secondStuff);
    queue.remove();

    queue = newQueueFile();
    assertThat(queue.peek()).isEqualTo(secondStuff);
  }

  @Test public void removeFromEmptyFileThrows() throws IOException {
    QueueFile queue = newQueueFile();

    try {
      queue.remove();
      fail("Should have thrown about removing from empty file.");
    } catch (NoSuchElementException ignored) {
    }
  }

  @Test public void removeZeroFromEmptyFileDoesNothing() throws IOException {
    QueueFile queue = newQueueFile();
    queue.remove(0);
    assertThat(queue.isEmpty()).isTrue();
  }

  @Test public void removeNegativeNumberOfElementsThrows() throws IOException {
    QueueFile queue = newQueueFile();
    queue.add(values[127]);

    try {
      queue.remove(-1);
      fail("Should have thrown about removing negative number of elements.");
    } catch (IllegalArgumentException ex) {
      assertThat(ex) //
          .hasMessageThat().isEqualTo("Cannot remove negative (-1) number of elements.");
    }
  }

  @Test public void removeZeroElementsDoesNothing() throws IOException {
    QueueFile queue = newQueueFile();
    queue.add(values[127]);

    queue.remove(0);
    assertThat(queue.size()).isEqualTo(1);
  }

  @Test public void removeBeyondQueueSizeElementsThrows() throws IOException {
    QueueFile queue = newQueueFile();
    queue.add(values[127]);

    try {
      queue.remove(10);
      fail("Should have thrown about removing too many elements.");
    } catch (IllegalArgumentException ex) {
      assertThat(ex) //
          .hasMessageThat()
          .isEqualTo("Cannot remove more elements (10) than present in queue (1).");
    }
  }

  @Test public void removingBigDamnBlocksErasesEffectively() throws IOException {
    byte[] bigBoy = new byte[7000];
    for (int i = 0; i < 7000; i += 100) {
      System.arraycopy(values[100], 0, bigBoy, i, values[100].length);
    }

    QueueFile queue = newQueueFile();

    queue.add(bigBoy);
    byte[] secondStuff = values[123];
    queue.add(secondStuff);

    // Confirm that bigBoy was in the file before we remove.
    byte[] data = new byte[bigBoy.length];
    queue.raf.seek(headerLength + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, bigBoy.length);
    assertThat(data).isEqualTo(bigBoy);

    queue.remove();

    // Next record is intact
    assertThat(queue.peek()).isEqualTo(secondStuff);

    // First should have been erased.
    queue.raf.seek(headerLength + Element.HEADER_LENGTH);
    queue.raf.readFully(data, 0, bigBoy.length);
    assertThat(data).isEqualTo(new byte[bigBoy.length]);
  }

  @Test public void testAddAndRemoveElements() throws IOException {
    long start = System.nanoTime();

    Queue<byte[]> expected = new ArrayDeque<>();

    for (int round = 0; round < 5; round++) {
      QueueFile queue = newQueueFile();
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
    QueueFile queue = newQueueFile();
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

    Queue<byte[]> expected = new ArrayDeque<>();
    QueueFile queue = newQueueFile();

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

  /** Tests failed queue expansion when the data crosses EOF. */
  @Test public void testFailedSplitExpansion() throws IOException {
    // This should results in a full file, but doesn't trigger an expansion (yet)
    int max = 86;

    Queue<byte[]> expected = new ArrayDeque<>();
    QueueFile queue = newQueueFile();

    for (int i = 0; i < max; i++) {
      expected.add(values[i]);
      queue.add(values[i]);
    }

    // Remove all but 1 value and add back
    // This should wrap around before expanding.
    for (int i = 0; i < max - 1; i++) {
      assertThat(queue.peek()).isEqualTo(expected.remove());
      queue.remove();

      expected.add(values[i]);
      queue.add(values[i]);
    }

    //Try to insert element that causes file expansion, but fail
    long fileLengthBeforeExpansion = file.length();
    BrokenRandomAccessFile braf = new BrokenRandomAccessFile(file, "rwd");
    queue = newQueueFile(braf);
    try {
      queue.add(values[max]);
      fail();
    } catch (IOException e) { /* expected */ }

    //Check that the queue continues valid
    braf.rejectCommit = false;
    while (!expected.isEmpty()) {
      assertThat(queue.peek()).isEqualTo(expected.remove());
      queue.remove();
    }

    queue.close();
  }

  @Test public void testFailedAdd() throws IOException {
    QueueFile queueFile = newQueueFile();
    queueFile.add(values[253]);
    queueFile.close();

    BrokenRandomAccessFile braf = new BrokenRandomAccessFile(file, "rwd");
    queueFile = newQueueFile(braf);

    try {
      queueFile.add(values[252]);
      fail();
    } catch (IOException e) { /* expected */ }

    braf.rejectCommit = false;

    // Allow a subsequent add to succeed.
    queueFile.add(values[251]);

    queueFile.close();

    queueFile = newQueueFile();
    assertThat(queueFile.size()).isEqualTo(2);
    assertThat(queueFile.peek()).isEqualTo(values[253]);
    queueFile.remove();
    assertThat(queueFile.peek()).isEqualTo(values[251]);
  }

  @Test public void testFailedRemoval() throws IOException {
    QueueFile queueFile = newQueueFile();
    queueFile.add(values[253]);
    queueFile.close();

    BrokenRandomAccessFile braf = new BrokenRandomAccessFile(file, "rwd");
    queueFile = newQueueFile(braf);

    try {
      queueFile.remove();
      fail();
    } catch (IOException e) { /* expected */ }

    queueFile.close();

    queueFile = newQueueFile();
    assertThat(queueFile.size()).isEqualTo(1);
    assertThat(queueFile.peek()).isEqualTo(values[253]);

    queueFile.add(values[99]);
    queueFile.remove();
    assertThat(queueFile.peek()).isEqualTo(values[99]);
  }

  @Test public void testFailedExpansion() throws IOException {
    QueueFile queueFile = newQueueFile();
    queueFile.add(values[253]);
    queueFile.close();

    BrokenRandomAccessFile braf = new BrokenRandomAccessFile(file, "rwd");
    queueFile = newQueueFile(braf);

    try {
      // This should trigger an expansion which should fail.
      queueFile.add(new byte[8000]);
      fail();
    } catch (IOException e) { /* expected */ }

    queueFile.close();

    queueFile = newQueueFile();
    assertThat(queueFile.size()).isEqualTo(1);
    assertThat(queueFile.peek()).isEqualTo(values[253]);
    assertThat(queueFile.fileLength).isEqualTo(4096);

    queueFile.add(values[99]);
    queueFile.remove();
    assertThat(queueFile.peek()).isEqualTo(values[99]);
  }

  /**
   * Exercise a bug where wrapped elements were getting corrupted when the
   * QueueFile was forced to expand in size and a portion of the final Element
   * had been wrapped into space at the beginning of the file.
   */
  @Test public void testFileExpansionDoesntCorruptWrappedElements()
      throws IOException {
    QueueFile queue = newQueueFile();

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
        assertThat(value[i]).named("Block %1$d corrupted at byte index %2$d.", blockNum + 1, i)
            .isEqualTo((byte) (blockNum + 1));
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
    QueueFile queue = newQueueFile();

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
        assertThat(value[i]).named("Block %1$d corrupted at byte index %2$d.", expectedBlockNumber,
            i).isEqualTo(expectedBlockNumber);
      }
    }

    queue.close();
  }

  @Test public void removingElementZeroesData() throws IOException {
    QueueFile queueFile = newQueueFile(true);
    queueFile.add(values[4]);
    queueFile.remove();
    queueFile.close();

    BufferedSource source = Okio.buffer(Okio.source(file));
    source.skip(headerLength);
    source.skip(Element.HEADER_LENGTH);
    assertThat(source.readByteString(4).hex()).isEqualTo("00000000");
  }

  @Test public void removingElementDoesNotZeroData() throws IOException {
    QueueFile queueFile = newQueueFile(false);
    queueFile.add(values[4]);
    queueFile.remove();
    queueFile.close();

    BufferedSource source = Okio.buffer(Okio.source(file));
    source.skip(headerLength);
    source.skip(Element.HEADER_LENGTH);
    assertThat(source.readByteString(4).hex()).isEqualTo("04030201");

    source.close();
  }

  /**
   * Exercise a bug where file expansion would leave garbage at the start of the header
   * and after the last element.
   */
  @Test public void testFileExpansionCorrectlyZeroesData()
      throws IOException {
    QueueFile queue = newQueueFile();

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
    // and the start of block "2". Internally the queue will cause block "4"
    // to be contiguous. There was a bug where the start of the buffer still
    // contained the tail end of block "4", and garbage was copied after the tail
    // end of the last element.
    queue.add(values[4]);

    // Read from header to first element and make sure it's zeroed.
    int firstElementPadding = Element.HEADER_LENGTH + 1024;
    byte[] data = new byte[firstElementPadding];
    queue.raf.seek(headerLength);
    queue.raf.readFully(data, 0, firstElementPadding);
    assertThat(data).isEqualTo(new byte[firstElementPadding]);

    // Read from the last element to the end and make sure it's zeroed.
    int endOfLastElement = headerLength + firstElementPadding + 4 * (Element.HEADER_LENGTH + 1024);
    int readLength = (int) (queue.raf.length() - endOfLastElement);
    data = new byte[readLength];
    queue.raf.seek(endOfLastElement);
    queue.raf.readFully(data, 0, readLength);
    assertThat(data).isEqualTo(new byte[readLength]);
  }

  /**
   * Exercise a bug where an expanding queue file where the start and end positions
   * are the same causes corruption.
   */
  @Test public void testSaturatedFileExpansionMovesElements() throws IOException {
    QueueFile queue = newQueueFile();

    // Create test data - 1016-byte blocks marked consecutively 1, 2, 3, 4, 5 and 6,
    // four of which perfectly fill the queue file, taking into account the file header
    // and the item headers.
    // Each item is of length
    // (QueueFile.INITIAL_LENGTH - headerLength) / 4 - element_header_length
    // = 1016 bytes
    byte[][] values = new byte[6][];
    for (int blockNum = 0; blockNum < values.length; blockNum++) {
      values[blockNum] = new byte[(INITIAL_LENGTH - headerLength) / 4 - Element.HEADER_LENGTH];
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
   * Exercise a bug where opening a queue whose first or last element's header
   * was non contiguous throws an {@link java.io.EOFException}.
   */
  @Test public void testReadHeadersFromNonContiguousQueueWorks() throws IOException {
    QueueFile queueFile = newQueueFile();

    // Fill the queue up to `length - 2` (i.e. remainingBytes() == 2).
    for (int i = 0; i < 15; i++) {
      queueFile.add(values[N - 1]);
    }
    queueFile.add(values[219]);

    // Remove first item so we have room to add another one without growing the file.
    queueFile.remove();

    // Add any element element and close the queue.
    queueFile.add(values[6]);
    int queueSize = queueFile.size();
    queueFile.close();

    // File should not be corrupted.
    QueueFile queueFile2 = newQueueFile();
    assertThat(queueFile2.size()).isEqualTo(queueSize);
  }

  @Test public void testIterator() throws IOException {
    byte[] data = values[10];

    for (int i = 0; i < 10; i++) {
      QueueFile queueFile = newQueueFile();
      for (int j = 0; j < i; j++) {
        queueFile.add(data);
      }

      int saw = 0;
      for (byte[] element : queueFile) {
        assertThat(element).isEqualTo(data);
        saw++;
      }
      assertThat(saw).isEqualTo(i);
      queueFile.close();
      file.delete();
    }
  }

  @Test public void testIteratorNextThrowsWhenEmpty() throws IOException {
    QueueFile queueFile = newQueueFile();

    Iterator<byte[]> iterator = queueFile.iterator();

    try {
      iterator.next();
      fail();
    } catch (NoSuchElementException ignored) {
    }
  }

  @Test public void testIteratorNextThrowsWhenExhausted() throws IOException {
    QueueFile queueFile = newQueueFile();
    queueFile.add(values[0]);

    Iterator<byte[]> iterator = queueFile.iterator();
    iterator.next();

    try {
      iterator.next();
      fail();
    } catch (NoSuchElementException ignored) {
    }
  }

  @Test public void testIteratorRemove() throws IOException {
    QueueFile queueFile = newQueueFile();
    for (int i = 0; i < 15; i++) {
      queueFile.add(values[i]);
    }

    Iterator<byte[]> iterator = queueFile.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      iterator.remove();
    }

    assertThat(queueFile).isEmpty();
  }

  @Test public void testIteratorRemoveDisallowsConcurrentModification() throws IOException {
    QueueFile queueFile = newQueueFile();
    for (int i = 0; i < 15; i++) {
      queueFile.add(values[i]);
    }

    Iterator<byte[]> iterator = queueFile.iterator();
    iterator.next();
    queueFile.remove();
    try {
      iterator.remove();
      fail();
    } catch (ConcurrentModificationException ignored) {
    }
  }

  @Test public void testIteratorHasNextDisallowsConcurrentModification() throws IOException {
    QueueFile queueFile = newQueueFile();
    for (int i = 0; i < 15; i++) {
      queueFile.add(values[i]);
    }

    Iterator<byte[]> iterator = queueFile.iterator();
    iterator.next();
    queueFile.remove();
    try {
      iterator.hasNext();
      fail();
    } catch (ConcurrentModificationException ignored) {
    }
  }

  @Test public void testIteratorDisallowsConcurrentModificationWithClear() throws IOException {
    QueueFile queueFile = newQueueFile();
    for (int i = 0; i < 15; i++) {
      queueFile.add(values[i]);
    }

    Iterator<byte[]> iterator = queueFile.iterator();
    iterator.next();
    queueFile.clear();
    try {
      iterator.hasNext();
      fail();
    } catch (ConcurrentModificationException ignored) {
    }
  }

  @Test public void testIteratorOnlyRemovesFromHead() throws IOException {
    QueueFile queueFile = newQueueFile();
    for (int i = 0; i < 15; i++) {
      queueFile.add(values[i]);
    }

    Iterator<byte[]> iterator = queueFile.iterator();
    iterator.next();
    iterator.next();

    try {
      iterator.remove();
      fail();
    } catch (UnsupportedOperationException ex) {
      assertThat(ex).hasMessageThat().isEqualTo("Removal is only permitted from the head.");
    }
  }

  @Test public void iteratorThrowsIOException() throws IOException {
    QueueFile queueFile = newQueueFile();
    queueFile.add(values[253]);
    queueFile.close();

    final class BrokenRandomAccessFile extends RandomAccessFile {
      boolean fail = false;

      BrokenRandomAccessFile(File file, String mode)
          throws FileNotFoundException {
        super(file, mode);
      }

      @Override public void write(byte[] b, int off, int len) throws IOException {
        if (fail) {
          throw new IOException();
        }
        super.write(b, off, len);
      }

      @Override public int read(byte[] b, int off, int len) throws IOException {
        if (fail) {
          throw new IOException();
        }
        return super.read(b, off, len);
      }
    }
    BrokenRandomAccessFile braf = new BrokenRandomAccessFile(file, "rwd");
    queueFile = newQueueFile(braf);
    Iterator<byte[]> iterator = queueFile.iterator();

    braf.fail = true;
    try {
      iterator.next();
      fail();
    } catch (Exception ioe) {
      assertThat(ioe).isInstanceOf(IOException.class);
    }

    braf.fail = false;
    iterator.next();

    braf.fail = true;
    try {
      iterator.remove();
      fail();
    } catch (Exception ioe) {
      assertThat(ioe).isInstanceOf(IOException.class);
    }
  }

  @Test public void queueToString() throws IOException {
    QueueFile queueFile = newQueueFile();
    for (int i = 0; i < 15; i++) {
      queueFile.add(values[i]);
    }

    if (forceLegacy) {
      assertThat(queueFile.toString()).contains("zero=true, versioned=false, length=4096,"
          + " size=15,"
          + " first=Element[position=16, length=0], last=Element[position=163, length=14]}");
    } else {
      assertThat(queueFile.toString()).contains("zero=true, versioned=true, length=4096,"
          + " size=15,"
          + " first=Element[position=32, length=0], last=Element[position=179, length=14]}");
    }
  }

  /**
   * A RandomAccessFile that can break when you go to write the COMMITTED
   * status.
   */
  static final class BrokenRandomAccessFile extends RandomAccessFile {
    boolean rejectCommit = true;

    BrokenRandomAccessFile(File file, String mode)
        throws FileNotFoundException {
      super(file, mode);
    }

    @Override public void write(byte[] b, int off, int len) throws IOException {
      if (rejectCommit && getFilePointer() == 0) {
        throw new IOException("No commit for you!");
      }
      super.write(b, off, len);
    }
  }
}
