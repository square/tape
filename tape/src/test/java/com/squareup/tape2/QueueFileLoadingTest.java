// Copyright 2012 Square, Inc.
package com.squareup.tape2;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.squareup.tape2.QueueTestUtils.EMPTY_SERIALIZED_QUEUE;
import static com.squareup.tape2.QueueTestUtils.FRESH_SERIALIZED_QUEUE;
import static com.squareup.tape2.QueueTestUtils.ONE_ENTRY_SERIALIZED_QUEUE;
import static com.squareup.tape2.QueueTestUtils.TRUNCATED_EMPTY_SERIALIZED_QUEUE;
import static com.squareup.tape2.QueueTestUtils.TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE;
import static com.squareup.tape2.QueueTestUtils.copyTestFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public final class QueueFileLoadingTest {

  private File testFile;

  @After public void cleanup() {
    assertTrue("Failed to delete test file " + testFile.getPath(), testFile.delete());
  }

  @Test public void testMissingFileInitializes() throws Exception {
    testFile = File.createTempFile(FRESH_SERIALIZED_QUEUE, "test");
    assertTrue(testFile.delete());
    assertFalse(testFile.exists());
    QueueFile queue = new QueueFile.Builder(testFile).build();
    assertEquals(0, queue.size());
    assertTrue(testFile.exists());
    queue.close();
  }

  @Test public void testEmptyFileInitializes() throws Exception {
    testFile = copyTestFile(EMPTY_SERIALIZED_QUEUE);
    QueueFile queue = new QueueFile.Builder(testFile).build();
    assertEquals(0, queue.size());
    queue.close();
  }

  @Test public void testSingleEntryFileInitializes() throws Exception {
    testFile = copyTestFile(ONE_ENTRY_SERIALIZED_QUEUE);
    QueueFile queue = new QueueFile.Builder(testFile).build();
    assertEquals(1, queue.size());
    queue.close();
  }

  @Test(expected = IOException.class)
  public void testTruncatedEmptyFileThrows() throws Exception {
    testFile = copyTestFile(TRUNCATED_EMPTY_SERIALIZED_QUEUE);
    new QueueFile.Builder(testFile).build();
  }

  @Test(expected = IOException.class)
  public void testTruncatedOneEntryFileThrows() throws Exception {
    testFile = copyTestFile(TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE);
    new QueueFile.Builder(testFile).build();
  }

  @Test(expected = IOException.class)
  public void testCreateWithReadOnlyFileThrowsException() throws Exception {
    testFile = copyTestFile(TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE);
    assertTrue(testFile.setWritable(false));

    // Should throw an exception.
    new QueueFile.Builder(testFile).build();
  }

  @Test(expected = IOException.class)
  public void testAddWithReadOnlyFileMissesMonitor() throws Exception {
    testFile = copyTestFile(EMPTY_SERIALIZED_QUEUE);

    QueueFile qf = new QueueFile.Builder(testFile).build();

    // Should throw an exception.
    FileObjectQueue<String> queue =
        new FileObjectQueue<>(qf, new FileObjectQueue.Converter<String>() {
          @Override public String from(byte[] bytes) throws IOException {
            return null;
          }

          @Override public void toStream(String o, OutputStream bytes)
              throws IOException {
            throw new IOException("fake Permission denied");
          }
        });

     // Should throw an exception.
     try {
        queue.add("trouble");
     } finally {
       queue.close();
     }
  }
}
