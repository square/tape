package com.squareup.tape;

import android.content.Context;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.MediumTest;
import android.test.suitebuilder.annotation.SmallTest;
import android.util.Log;
import com.squareup.tape.QueueFile.ElementReader;
import com.squareup.tape.QueueFileNative;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

/**
 * Unit tests for the API calls. Rigorous testing of Queue functionality is 
 * done in native tests.
 */
@MediumTest
public class QueueFileNativeTests extends AndroidTestCase {

  private QueueFileNative queueFile;
  private String queueFilename;
  private byte[] testData;

  @Override
  protected void setUp() throws IOException {
    queueFilename = getContext().getFilesDir().toString() + "unittest.queue";
    queueFile = new QueueFileNative(queueFilename);
    queueFile.clear();
    assertNotNull(queueFile);
    testData = new byte[100];
    for (byte i = 0; i < 100; i++) {
      testData[i] = i;
    }
  }

  @Override
  protected void tearDown() throws IOException {
    queueFile.close();
  }

  
  public void testEmptyQueueSize() throws IOException {
    assertEquals(0, queueFile.size());
  }

  public void testBadFileNameException() {
    try {
      QueueFileNative badQueue = new QueueFileNative("/// ~! !@#$$#%$%&&%^*");
      fail("should not be able to create queue with invalid file name");
    } catch (IOException expected) {
      // expected
    }
  }

//  @SmallTest
//  public void testDoubleClose() throws IOException {
//    QueueFileNative newQueue = new QueueFileNative(queueFilename + "2");
//    newQueue.close();
//    try {
//      newQueue.close();
//      fail();
//    } catch (IOException e) {
//      // expected
//    }
//  }
  
  public void testSimpleAddAndPeek() throws IOException {
    queueFile.add(testData);
    assertEquals(1, queueFile.size());
    byte retval[] = queueFile.peek();
    assertThat(retval).isEqualTo(testData);
    queueFile.remove();
    assertEquals(0, queueFile.size());
  }

  public void testPeekWithElementReader() throws IOException {
    final byte[] a = {1, 2};
    queueFile.add(a);
    final byte[] b = {3, 4, 5};
    queueFile.add(b);
    final AtomicInteger callbacks = new AtomicInteger(0);

    queueFile.peek(new QueueFile.ElementReader() {
      @Override public void read(InputStream in, int length) throws IOException {
        assertThat(length).isEqualTo(2);
        byte[] actual = new byte[length];
        in.read(actual);
        assertThat(actual).isEqualTo(a);
        callbacks.incrementAndGet();
      }
    });

    queueFile.peek(new QueueFile.ElementReader() {
      @Override public void read(InputStream in, int length) throws IOException {
        assertThat(length).isEqualTo(2);
        assertThat(in.read()).isEqualTo(1);
        assertThat(in.read()).isEqualTo(2);
        assertThat(in.read()).isEqualTo(-1);
        callbacks.incrementAndGet();
      }
    });

    queueFile.remove();

    queueFile.peek(new QueueFile.ElementReader() {
      @Override public void read(InputStream in, int length) throws IOException {
        assertThat(length).isEqualTo(3);
        byte[] actual = new byte[length];
        in.read(actual);
        assertThat(actual).isEqualTo(b);
        callbacks.incrementAndGet();
      }
    });

    assertThat(callbacks.get()).isEqualTo(3);
    assertThat(queueFile.peek()).isEqualTo(b);
    assertThat(queueFile.size()).isEqualTo(1);
  }
  
  public void testForEach() throws IOException {
    final byte[] a = {1, 2};
    queueFile.add(a);
    final byte[] b = {3, 4, 5};
    queueFile.add(b);
    assertThat(queueFile.size()).isEqualTo(2);

    final int[] iteration = new int[]{0};
    ElementReader elementReader = new ElementReader() {
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
    assertEquals(2, iteration[0]);
  }
  
}