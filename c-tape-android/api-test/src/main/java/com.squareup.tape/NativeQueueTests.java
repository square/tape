package com.squareup.tape;

import android.content.Context;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.SmallTest;
import android.util.Log;
import com.squareup.tape.QueueFileNative;

import java.io.File;
import java.io.IOException;

/**
 * Unit test to make sure native library is callable. Tests mostly the forEach
 * and peek with ElementReader as that's implemented in Java. Rigorous testing
 * of the C code is done in native code.
 */
public class NativeQueueTests extends AndroidTestCase {

  public NativeQueueTests() {
    super();
  }

  private QueueFileNative queue;
  private String queueFilename;
  private byte[] testData;

  @Override
  protected void setUp() throws IOException {
    queueFilename = getContext().getFilesDir().toString() + "unittest.queue";
    queue = new QueueFileNative(queueFilename);
    assertTrue(queue != null);
    testData = new byte[100];
    for (byte i = 0; i < 100; i++) {
      testData[i] = i;
    }
  }

  @Override
  protected void tearDown() throws IOException {
    queue.close();
  }

  
  @SmallTest
  public void testEmptyFileSize() throws IOException {
    assertTrue(queue.size() == 0);
  }

  @SmallTest
  public void testBadFileNameException() {
    try {
      QueueFileNative badQueue = new QueueFileNative("hello");
      Log.e("NativeQueueTests", "should throw exception");
      fail();
    } catch (IOException e) {
      Log.e("NativeQueueTests", "Exception caught.");
      // expected
    }
  }

// ###jochen
//  @SmallTest
//  public void testSimpleAdd() throws IOException {
//    queue.add(testData);
//    assertTrue(queue.size() == 1);
//    byte retval[] = queue.peek();
//    assertTrue(retval.equals(testData));
//    queue.remove();
//    assertTrue(queue.size() == 0);
//  }

  
}