package com.squareup.tape;

import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.SmallTest;
import com.squareup.tape.QueueFileNative;

import java.io.IOException;

public class NativeQueueTests extends AndroidTestCase {

  public NativeQueueTests() {
    super();
  }

  private QueueFileNative queue;
  @Override
  protected void setUp() throws IOException {
    queue = new QueueFileNative("queue.test");
  }

  @SmallTest
  public void testPreconditions() throws IOException {
    assertTrue(queue != null);
    queue.close();
  }
}