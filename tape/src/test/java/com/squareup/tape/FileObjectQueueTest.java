// Copyright 2014 Square, Inc.
package com.squareup.tape;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.squareup.tape.QueueElementObserver.NO_OP_OBSERVER;
import static com.squareup.tape.QueueFile.MAX_ELEMENT_SIZE;
import static com.squareup.tape.QueueTestUtils.ONE_ENTRY_SERIALIZED_QUEUE;
import static com.squareup.tape.QueueTestUtils.copyTestFile;
import static org.fest.assertions.Assertions.assertThat;

public class FileObjectQueueTest {
  private static final long TIMEOUT_MS = 200;
  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private File parent;
  private FileObjectQueue<String> queue;

  @Before public void setUp() throws IOException {
    parent = folder.getRoot();
    File file = new File(parent, "queue-file");
    queue = new FileObjectQueue<String>(file, new SerializedConverter<String>());
    queue.add("one");
    queue.add("two");
    queue.add("three");
  }

  @After public void cleanUp() {
    queue.close();
  }

  @Test public void peekMultiple() {
    List<String> peek = queue.peek(3);
    assertThat(peek).containsExactly("one", "two", "three");
  }

  @Test public void allElementOperationsCall() throws IOException, InterruptedException {
    File file = new File(parent, "queue-file-alpha");
    CountDownLatch latch = new CountDownLatch(6);
    queue = new FileObjectQueue<String>(file,
        new SerializedConverter<String>(),
        MAX_ELEMENT_SIZE,
        (observation) -> latch.countDown());
    queue.add("one");
    queue.add("two");
    queue.add("three");
    List<String> peek = queue.peek(3);
    assertThat(peek).containsExactly("one", "two", "three");
    latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Test public void tryReadingInFileWithElementOverMaxCalls()
      throws IOException, InterruptedException {
    File file = copyTestFile(ONE_ENTRY_SERIALIZED_QUEUE);
    CountDownLatch latch = new CountDownLatch(1);
    queue = new FileObjectQueue<String>(file,
        new SerializedConverter<String>(),
        10,
        (observation) -> {
          if (observation.getElementSize() > observation.getMaxElementSize()) {
            latch.countDown();
          }
        });
    latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Test public void tryAddingElementInFileWithElementOverMaxAfterInitCalls()
      throws IOException, InterruptedException {
    File file = new File(parent, "queue-file1");
    CountDownLatch latch = new CountDownLatch(1);
    queue = new FileObjectQueue<String>(file,
        new SerializedConverter<String>(),
        10,
        (observation) -> {
          if (!observation.isInit() && observation.isAdd() &&
              observation.getElementSize() > observation.getMaxElementSize()) {
            latch.countDown();
          }
        });
    queue.add("This String is over 10 bytes easily");
    latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Test public void tryReadingElementInFileWithElementOverMaxAfterInitThrows()
      throws IOException, InterruptedException {
    File file = new File(parent, "queue-file2");
    CountDownLatch latch = new CountDownLatch(2);
    queue = new FileObjectQueue<String>(file,
        new SerializedConverter<String>(),
        10,
        (observation) -> {
          if (!observation.isInit() &&
              observation.getElementSize() > observation.getMaxElementSize()) {
            latch.countDown();
          }
        });
    queue.add("This String is over 10 bytes easily");
    queue.peek();
    latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Test public void testAddAndPeekIgnoredWithDefault() throws IOException {
    File parent = folder.getRoot();
    File file = new File(parent, "queue-file4");
    queue = new FileObjectQueue<String>(file,
        new SerializedConverter<String>(),
        10,
        NO_OP_OBSERVER);
    queue.add("This String is over 10 bytes easily");
    queue.peek();
    assertThat(queue.size()).isEqualTo(1);
  }

  @Test public void testReadHeaderIgnoredWithDefault() throws IOException {
    File file = copyTestFile(ONE_ENTRY_SERIALIZED_QUEUE);
    queue = new FileObjectQueue<String>(file,
        new SerializedConverter<String>(),
        10,
        NO_OP_OBSERVER);
    queue.peek();
    assertThat(queue.size()).isEqualTo(1);
  }

  @Test public void getsAllAsList() {
    List<String> peek = queue.asList();
    assertThat(peek).containsExactly("one", "two", "three");
  }

  @Test public void peekMaxCanExceedQueueDepth() {
    List<String> peek = queue.peek(6);
    assertThat(peek).hasSize(3);
  }

  @Test public void peekMaxCanBeSmallerThanQueueDepth() {
    List<String> peek = queue.peek(2);
    assertThat(peek).containsExactly("one", "two");
  }

  @Test public void checkForIntegritySanityCheckDoesNotThrow() throws IOException {
    queue.checkQueueIntegrity();
  }
}
