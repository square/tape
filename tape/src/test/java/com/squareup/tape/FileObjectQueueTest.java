// Copyright 2014 Square, Inc.
package com.squareup.tape;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.fest.assertions.Assertions.assertThat;

public class FileObjectQueueTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private FileObjectQueue<Pojo> queue;

  @Before public void setUp() throws IOException {
    File parent = folder.getRoot();
    File file = new File(parent, "queue-file");
    queue = new FileObjectQueue<Pojo>(file, new PojoConverter());
    queue.add(new Pojo("one"));
    queue.add(new Pojo("two"));
    queue.add(new Pojo("three"));
  }

  @Test public void peekMultiple() throws IOException {
    List<Pojo> peek = queue.peek(3);
    assertThat(peek).containsExactly(new Pojo("one"), new Pojo("two"), new Pojo("three"));
  }

  @Test public void getsAllAsList() throws IOException {
    List<Pojo> peek = queue.asList();
    assertThat(peek).containsExactly(new Pojo("one"), new Pojo("two"), new Pojo("three"));
  }

  @Test public void peekMaxCanExceedQueueDepth() throws IOException {
    List<Pojo> peek = queue.peek(6);
    assertThat(peek).hasSize(3);
  }

  @Test public void clear() throws IOException {
    queue.clear();
    assertThat(queue.size()).isEqualTo(0);
  }

  @Test public void peekMaxCanBeSmallerThanQueueDepth() throws IOException {
    List<Pojo> peek = queue.peek(2);
    assertThat(peek).containsExactly(new Pojo("one"), new Pojo("two"));
  }

  private static class Pojo {
    private String text;

    private Pojo(String text) {
      this.text = text;
    }

    private String getText() {
      return text;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Pojo pojo = (Pojo) o;

      return text != null ? text.equals(pojo.text) : pojo.text == null;

    }

    @Override
    public int hashCode() {
      return text != null ? text.hashCode() : 0;
    }
  }

  private class PojoConverter implements FileObjectQueue.Converter<Pojo> {

    @Override
    public Pojo from(byte[] bytes) throws IOException {
      String text = new String(bytes, "UTF-8");
      return new Pojo(text);
    }

    @Override
    public void toStream(Pojo o, OutputStream bytes) throws IOException {
      bytes.write(o.getText().getBytes("UTF-8"));
    }
  }
}
