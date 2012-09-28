// Copyright 2012 Square, Inc.
package com.squareup.tape;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

import static com.squareup.tape.QueueTestUtils.EMPTY_SERIALIZED_QUEUE;
import static com.squareup.tape.QueueTestUtils.FRESH_SERIALIZED_QUEUE;
import static com.squareup.tape.QueueTestUtils.ONE_ENTRY_SERIALIZED_QUEUE;
import static com.squareup.tape.QueueTestUtils.TRUNCATED_EMPTY_SERIALIZED_QUEUE;
import static com.squareup.tape.QueueTestUtils.TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE;
import static com.squareup.tape.QueueTestUtils.UndeletableFile;
import static com.squareup.tape.QueueTestUtils.copyTestFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SerializedConverterTest {

  private File testFile;

  @After public void cleanup() {
    assertTrue("Failed to delete test file " + testFile.getPath(), testFile.delete());
  }

  @Test public void testMissingFileInitializes() throws Exception {
    testFile = File.createTempFile(FRESH_SERIALIZED_QUEUE, "test");
    testFile.delete();
    assertFalse(testFile.exists());
    ObjectQueue<String> stringQFile = createQueue(testFile);
    assertEquals(0, stringQFile.size());
    assertTrue(testFile.exists());
  }

  @Test public void testEmptyFileInitializes() throws Exception {
    testFile = copyTestFile(EMPTY_SERIALIZED_QUEUE);
    ObjectQueue<String> stringQFile = createQueue(testFile);
    assertEquals(0, stringQFile.size());
  }

  @Test public void testSingleEntryFileInitializes() throws Exception {
    testFile = copyTestFile(ONE_ENTRY_SERIALIZED_QUEUE);
    ObjectQueue<String> stringQFile = createQueue(testFile);
    assertEquals(1, stringQFile.size());
  }

  @Test(expected = IOException.class)
  public void testTruncatedEmptyFileThrows() throws Exception {
    testFile = copyTestFile(TRUNCATED_EMPTY_SERIALIZED_QUEUE);
    createQueue(testFile);
  }

  @Test(expected = IOException.class)
  public void testTruncatedOneEntryFileThrows() throws Exception {
    testFile = copyTestFile(TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE);
    createQueue(testFile);
  }

  @Test(expected = IOException.class)
  public void testCreateWithReadOnlyFile_throwsException() throws Exception {
    testFile = copyTestFile(TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE);
    testFile.setWritable(false);

    File tmp = new UndeletableFile(testFile.getAbsolutePath());
    // Should throw an exception.
    createQueue(tmp);
  }

  @Test(expected = FileException.class)
  public void testAddWithReadOnlyFile_missesMonitor() throws Exception {
    testFile = copyTestFile(EMPTY_SERIALIZED_QUEUE);

    // Should throw an exception.
    FileObjectQueue<String> qf = new FileObjectQueue<String>(testFile, new SerializedConverter<String>() {
      @Override public void toStream(String o, OutputStream bytes) throws IOException {
        throw new IOException("fake Permission denied");
      }
    });
    // Should throw an exception.
    qf.add("trouble");
  }

  ///////////////////////////////

  private static <T extends Serializable> ObjectQueue<T> createQueue(File file) throws IOException {
    SerializedConverter<T> converter = new SerializedConverter<T>();
    return new FileObjectQueue<T>(file, converter);
  }
}
