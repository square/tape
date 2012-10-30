// Copyright 2010 Square, Inc.
package com.squareup.tape;

import com.squareup.tape.QueueFile;
import com.squareup.tape.QueueFileNative;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Logger;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

/**
 * Tests for QueueFileNative.
 */
@SuppressWarnings({"ResultOfMethodCallIgnored"})
public class QueueFileNativeTest {
  private static final Logger logger =
      Logger.getLogger(QueueFileNativeTest.class.getName());


  @Before public void setUp() throws Exception {
//    file = File.createTempFile("test.queue", null);
//    file.delete();
  }

  @After public void tearDown() throws Exception {
//    file.delete();
  }

  @Test public void testAddOneElement() throws IOException {
// TODO(jochen): test is disabled until I can figure out how to get the native lib to load on non-ARM platform.
//               note that I build an x86 lib in jni (set Application.mk to build x86) but that library did also
//               not load.
    System.out.println(System.getProperty("java.library.path") + "############## LIBRARY PATH #############");
//    QueueFile queue = new QueueFileNative("test.queue");
//    queue.close();
  }
}
