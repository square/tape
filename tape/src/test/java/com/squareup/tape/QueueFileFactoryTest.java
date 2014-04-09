package com.squareup.tape;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for QueueFileFactory.
 *
 * @author Clement Pang (me@clementpang.com)
 */
@SuppressWarnings({"ResultOfMethodCallIgnored"})
public class QueueFileFactoryTest {

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

  private File file1;
  private File file2;

  @Before
  public void setUp() throws Exception {
    file1 = File.createTempFile("test.queue", null);
    file1.delete();
    file2 = File.createTempFile("test.queue", null);
    file2.delete();
  }

  @After
  public void tearDown() throws Exception {
    file1.delete();
    file2.delete();
  }

  @Test
  public void testMigration() throws IOException {
    QueueFile oldQueueFile = QueueFileFactory.open(file1);
    for (int i = 0; i < N; i++) {
      byte[] value = values[i];
      oldQueueFile.add(value);
    }
    oldQueueFile.close();
    QueueFile newQueueFile = QueueFileFactory.openAndMigrate(file1, file2, false);
    for (int i = 0; i < N; i++) {
      byte[] expected = values[i];
      assertThat(newQueueFile.peek()).isEqualTo(expected);
      newQueueFile.remove();
    }
    oldQueueFile = QueueFileFactory.open(file1);
    assertTrue(oldQueueFile.isEmpty());
    assertTrue(newQueueFile.isEmpty());

    for (int i = 0; i < N; i++) {
      byte[] value = values[i];
      oldQueueFile.add(value);
    }
    newQueueFile = QueueFileFactory.openAndMigrate(file1, file2, true);
    for (int i = 0; i < N; i++) {
      byte[] expected = values[i];
      assertThat(newQueueFile.peek()).isEqualTo(expected);
      newQueueFile.remove();
    }
    assertFalse(file1.exists());
    assertTrue(newQueueFile.isEmpty());
  }
}
