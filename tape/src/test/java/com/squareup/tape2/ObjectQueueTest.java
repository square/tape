package com.squareup.tape2;

import com.squareup.burst.BurstJUnit4;
import com.squareup.burst.annotation.Burst;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import okio.BufferedSink;
import okio.BufferedSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

@RunWith(BurstJUnit4.class)
public class ObjectQueueTest {
  public enum QueueFactory {
    FILE() {
      @Override
      public <T> ObjectQueue<T> create(QueueFile queueFile, FileObjectQueue.Converter<T> converter)
          throws IOException {
        return ObjectQueue.create(queueFile, converter);
      }
    },
    MEMORY() {
      @Override
      public <T> ObjectQueue<T> create(QueueFile file, FileObjectQueue.Converter<T> converter) {
        return ObjectQueue.createInMemory();
      }
    };

    public abstract <T> ObjectQueue<T> create(QueueFile queueFile,
        FileObjectQueue.Converter<T> converter) throws IOException;
  }

  @Rule public TemporaryFolder folder = new TemporaryFolder();
  @Burst QueueFactory factory;
  ObjectQueue<String> queue;

  @Before public void setUp() throws IOException {
    File parent = folder.getRoot();
    File file = new File(parent, "object-queue");
    QueueFile queueFile = new QueueFile.Builder(file).build();

    queue = factory.create(queueFile, new StringConverter());
    queue.add("one");
    queue.add("two");
    queue.add("three");
  }

  @Test public void size() throws IOException {
    assertThat(queue.size()).isEqualTo(3);
  }

  @Test public void peek() throws IOException {
    assertThat(queue.peek()).isEqualTo("one");
  }

  @Test public void peekMultiple() throws IOException {
    assertThat(queue.peek(2)).containsExactly("one", "two");
  }

  @Test public void peekMaxCanExceedQueueDepth() throws IOException {
    assertThat(queue.peek(6)).containsExactly("one", "two", "three");
  }

  @Test public void asList() throws IOException {
    assertThat(queue.asList()).containsExactly("one", "two", "three");
  }

  @Test public void remove() throws IOException {
    queue.remove();

    assertThat(queue.asList()).containsExactly("two", "three");
  }

  @Test public void removeMultiple() throws IOException {
    queue.remove(2);

    assertThat(queue.asList()).containsExactly("three");
  }

  @Test public void clear() throws IOException {
    queue.clear();

    assertThat(queue.size()).isEqualTo(0);
  }

  @Test public void isEmpty() throws IOException {
    assertThat(queue.isEmpty()).isFalse();

    queue.clear();

    assertThat(queue.isEmpty()).isTrue();
  }

  @Test public void testIterator() throws IOException {
    final List<String> saw = new ArrayList<>();
    for (String pojo : queue) {
      saw.add(pojo);
    }
    assertThat(saw).containsExactly("one", "two", "three");
  }

  @Test public void testIteratorNextThrowsWhenEmpty() throws IOException {
    queue.clear();
    Iterator<String> iterator = queue.iterator();

    try {
      iterator.next();
      fail();
    } catch (NoSuchElementException ignored) {
    }
  }

  @Test public void testIteratorNextThrowsWhenExhausted() throws IOException {
    Iterator<String> iterator = queue.iterator();
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
    Iterator<String> iterator = queue.iterator();

    iterator.next();
    iterator.remove();
    assertThat(queue.asList()).containsExactly("two", "three");

    iterator.next();
    iterator.remove();
    assertThat(queue.asList()).containsExactly("three");
  }

  @Test public void testIteratorRemoveDisallowsConcurrentModification() throws IOException {
    Iterator<String> iterator = queue.iterator();
    iterator.next();
    queue.remove();

    try {
      iterator.remove();
      fail();
    } catch (ConcurrentModificationException ignored) {
    }
  }

  @Test public void testIteratorHasNextDisallowsConcurrentModification() throws IOException {
    Iterator<String> iterator = queue.iterator();
    iterator.next();
    queue.remove();

    try {
      iterator.hasNext();
      fail();
    } catch (ConcurrentModificationException ignored) {
    }
  }

  @Test public void testIteratorDisallowsConcurrentModificationWithClear() throws IOException {
    Iterator<String> iterator = queue.iterator();
    iterator.next();
    queue.clear();

    try {
      iterator.hasNext();
      fail();
    } catch (ConcurrentModificationException ignored) {
    }
  }

  @Test public void testIteratorOnlyRemovesFromHead() throws IOException {
    Iterator<String> iterator = queue.iterator();
    iterator.next();
    iterator.next();

    try {
      iterator.remove();
      fail();
    } catch (UnsupportedOperationException ex) {
      assertThat(ex).hasMessageThat().isEqualTo("Removal is only permitted from the head.");
    }
  }

  static final class StringConverter implements FileObjectQueue.Converter<String> {
    @Override public String from(BufferedSource source) throws IOException {
      return source.readUtf8();
    }

    @Override public void toStream(String s, BufferedSink sink) throws IOException {
      sink.writeUtf8(s);
    }
  }
}
