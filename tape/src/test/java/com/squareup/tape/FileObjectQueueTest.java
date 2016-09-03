// Copyright 2014 Square, Inc.
package com.squareup.tape;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.fest.assertions.Assertions.assertThat;

public class FileObjectQueueTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private FileObjectQueue<String> queue;

  @Before public void setUp() throws IOException {
    File parent = folder.getRoot();
    File file = new File(parent, "queue-file");
    queue = new FileObjectQueue<String>(file, new SerializedConverter<String>());
    queue.add("one");
    queue.add("two");
    queue.add("three");
  }

  @Test public void peekMultiple() throws IOException {
    List<String> peek = queue.peek(3);
    assertThat(peek).containsExactly("one", "two", "three");
  }

  @Test public void getsAllAsList() throws IOException {
    List<String> peek = queue.asList();
    assertThat(peek).containsExactly("one", "two", "three");
  }

  @Test public void peekMaxCanExceedQueueDepth() throws IOException {
    List<String> peek = queue.peek(6);
    assertThat(peek).hasSize(3);
  }

  @Test public void clear() throws IOException {
    queue.clear();
    assertThat(queue.size()).isEqualTo(0);
  }

  @Test public void peekMaxCanBeSmallerThanQueueDepth() throws IOException {
    List<String> peek = queue.peek(2);
    assertThat(peek).containsExactly("one", "two");
  }
}
