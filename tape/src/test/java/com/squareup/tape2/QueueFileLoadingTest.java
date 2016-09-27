// Copyright 2012 Square, Inc.
package com.squareup.tape2;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import static com.squareup.tape2.QueueTestUtils.EMPTY_SERIALIZED_QUEUE;
import static com.squareup.tape2.QueueTestUtils.FRESH_SERIALIZED_QUEUE;
import static com.squareup.tape2.QueueTestUtils.ONE_ENTRY_SERIALIZED_QUEUE;
import static com.squareup.tape2.QueueTestUtils.TRUNCATED_EMPTY_SERIALIZED_QUEUE;
import static com.squareup.tape2.QueueTestUtils.TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE;
import static com.squareup.tape2.QueueTestUtils.UndeletableFile;
import static com.squareup.tape2.QueueTestUtils.copyTestFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueueFileLoadingTest {

  private File testFile;

  @After public void cleanup() {
    assertTrue("Failed to delete test file " + testFile.getPath(), testFile.delete());
  }

  @Test public void testMissingFileInitializes() throws Exception {
    testFile = File.createTempFile(FRESH_SERIALIZED_QUEUE, "test");
    assertTrue(testFile.delete());
    assertFalse(testFile.exists());
    QueueFile queue = new QueueFile(testFile);
    assertEquals(0, queue.size());
    assertTrue(testFile.exists());
  }

  @Test public void testEmptyFileInitializes() throws Exception {
    testFile = copyTestFile(EMPTY_SERIALIZED_QUEUE);
    QueueFile queue = new QueueFile(testFile);
    assertEquals(0, queue.size());
  }

  @Test public void testSingleEntryFileInitializes() throws Exception {
    testFile = copyTestFile(ONE_ENTRY_SERIALIZED_QUEUE);
    QueueFile queue = new QueueFile(testFile);
    assertEquals(1, queue.size());
  }

  @Test(expected = IOException.class)
  public void testTruncatedEmptyFileThrows() throws Exception {
    testFile = copyTestFile(TRUNCATED_EMPTY_SERIALIZED_QUEUE);
    new QueueFile(testFile);
  }

  @Test(expected = IOException.class)
  public void testTruncatedOneEntryFileThrows() throws Exception {
    testFile = copyTestFile(TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE);
    new QueueFile(testFile);
  }

  @Test(expected = IOException.class)
  public void testCreateWithReadOnlyFile_throwsException() throws Exception {
    testFile = copyTestFile(TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE);
    assertTrue(testFile.setWritable(false));

    File tmp = new UndeletableFile(testFile.getAbsolutePath());
    // Should throw an exception.
    new QueueFile(tmp);
  }

  @Test(expected = IOException.class)
  public void testAddWithReadOnlyFile_missesMonitor() throws Exception {
    testFile = copyTestFile(EMPTY_SERIALIZED_QUEUE);

    // Should throw an exception.
    FileObjectQueue<String> qf =
        new FileObjectQueue<String>(testFile, new FileObjectQueue.Converter<String>() {
          @Override public String from(byte[] bytes) throws IOException {
            return null;
          }

          @Override public void toStream(String o, OutputStream bytes) throws IOException {
            throw new IOException("fake Permission denied");
          }
        });

    // Should throw an exception.
    qf.add("trouble");
  }
}
