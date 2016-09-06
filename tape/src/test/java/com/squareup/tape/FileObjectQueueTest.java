// Copyright 2014 Square, Inc.
package com.squareup.tape;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

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

  @Test public void listenerOnAddInvokedForExistingEntries() throws IOException {
    final List<Pojo> saw = new ArrayList<Pojo>();
    queue.setListener(new ObjectQueue.Listener<Pojo>() {
      @Override public void onAdd(ObjectQueue<Pojo> queue, Pojo entry) {
        saw.add(entry);
      }

      @Override public void onRemove(ObjectQueue<Pojo> queue) {
        fail("onRemove should not be invoked");
      }
    });
    assertThat(saw).containsExactly(new Pojo("one"), new Pojo("two"), new Pojo("three"));
  }

  @Test public void listenerOnRemoveInvokedForRemove() throws IOException {
    final AtomicInteger count = new AtomicInteger();
    queue.setListener(new ObjectQueue.Listener<Pojo>() {
      @Override public void onAdd(ObjectQueue<Pojo> queue, Pojo entry) {
      }

      @Override public void onRemove(ObjectQueue<Pojo> queue) {
        count.getAndIncrement();
      }
    });
    queue.remove();
    assertThat(count.get()).isEqualTo(1);
  }

  @Test public void listenerOnRemoveInvokedForRemoveN() throws IOException {
    final AtomicInteger count = new AtomicInteger();
    queue.setListener(new ObjectQueue.Listener<Pojo>() {
      @Override public void onAdd(ObjectQueue<Pojo> queue, Pojo entry) {
      }

      @Override public void onRemove(ObjectQueue<Pojo> queue) {
        count.getAndIncrement();
      }
    });
    queue.remove(2);
    assertThat(count.get()).isEqualTo(2);
  }

  @Test public void listenerOnRemoveInvokedForClear() throws IOException {
    final AtomicInteger count = new AtomicInteger();
    queue.setListener(new ObjectQueue.Listener<Pojo>() {
      @Override public void onAdd(ObjectQueue<Pojo> queue, Pojo entry) {
      }

      @Override public void onRemove(ObjectQueue<Pojo> queue) {
        count.getAndIncrement();
      }
    });
    queue.clear();
    assertThat(count.get()).isEqualTo(3);
  }

  @Test public void testIterator() throws IOException {
    final List<Pojo> saw = new ArrayList<Pojo>();
    for (Pojo pojo : queue) {
      saw.add(pojo);
    }
    assertThat(saw).containsExactly(new Pojo("one"), new Pojo("two"), new Pojo("three"));
  }

  @Test public void testIteratorNextThrowsWhenEmpty() throws IOException {
    queue.clear();
    Iterator iterator = queue.iterator();

    try {
      iterator.next();
      fail();
    } catch (NoSuchElementException ignored) {
    }
  }

  @Test public void testIteratorNextThrowsWhenExhausted() throws IOException {
    Iterator iterator = queue.iterator();
    iterator.next();
    iterator.next();
    iterator.next();

    try {
      iterator.next();
      fail();
    } catch (NoSuchElementException ignored) {
    }
  }

  @Test public void testIteratorRemove() throws IOException {
    Iterator iterator = queue.iterator();

    iterator.next();
    iterator.remove();
    assertThat(queue.asList()).containsExactly(new Pojo("two"), new Pojo("three"));

    iterator.next();
    iterator.remove();
    assertThat(queue.asList()).containsExactly(new Pojo("three"));
  }

  @Test public void testIteratorRemoveDisallowsConcurrentModification() throws IOException {
    Iterator iterator = queue.iterator();
    iterator.next();
    queue.remove();

    try {
      iterator.remove();
      fail();
    } catch (ConcurrentModificationException ignored) {
    }
  }

  @Test public void testIteratorHasNextDisallowsConcurrentModification() throws IOException {
    Iterator iterator = queue.iterator();
    iterator.next();
    queue.remove();

    try {
      iterator.hasNext();
      fail();
    } catch (ConcurrentModificationException ignored) {
    }
  }

  @Test public void testIteratorDisallowsConcurrentModificationWithClear() throws IOException {
    Iterator iterator = queue.iterator();
    iterator.next();
    queue.clear();

    try {
      iterator.hasNext();
      fail();
    } catch (ConcurrentModificationException ignored) {
    }
  }

  @Test public void testIteratorOnlyRemovesFromHead() throws IOException {
    Iterator iterator = queue.iterator();
    iterator.next();
    iterator.next();

    try {
      iterator.remove();
      fail();
    } catch (UnsupportedOperationException ex) {
      assertThat(ex).hasMessage("Removal is only permitted from the head.");
    }
  }
}
