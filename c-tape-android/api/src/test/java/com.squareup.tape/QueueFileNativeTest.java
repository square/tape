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
    System.out.println(System.getProperty("java.library.path") + "############## LIBRARY PATH #############");
    QueueFile queue = new QueueFileNative("/invalid2@#$123/ - - .");
    queue.close();
  }
}
