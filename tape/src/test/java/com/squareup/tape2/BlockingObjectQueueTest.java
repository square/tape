package com.squareup.tape2;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BlockingObjectQueueTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private File file;

  @Before public void setUp() throws Exception {
    File parent = folder.getRoot();
    file = new File(parent, "blocking-queue-file");
  }

  private QueueFile newQueueFile() throws IOException {
    return new QueueFile.Builder(file).build();
  }

  private BlockingObjectQueue<byte[]> newQueue() throws IOException {
    return BlockingObjectQueue.create(newQueueFile());
  }

  @Test public void add() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertEquals(0, q.size());
    q.add(new byte[]{1, 2});
    assertArrayEquals(new byte[]{1, 2}, q.peek());
    assertEquals(1, q.size());
  }

  @Test public void offer() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertEquals(0, q.size());
    q.offer(new byte[]{1, 2});
    assertArrayEquals(new byte[]{1, 2}, q.peek());
    assertEquals(1, q.size());
  }

  @Test public void put() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertEquals(0, q.size());
    q.put(new byte[]{1, 2});
    assertArrayEquals(new byte[]{1, 2}, q.peek());
    assertEquals(1, q.size());
  }

  @Test public void offer1() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertEquals(0, q.size());
    q.offer(new byte[]{1, 2}, 1, TimeUnit.SECONDS);
    assertArrayEquals(new byte[]{1, 2}, q.peek());
    assertEquals(1, q.size());
  }

  @Test public void takeEmpty() throws IOException, InterruptedException {
    final BlockingObjectQueue<byte[]> q = newQueue();
    Thread t = new Thread(new Runnable() {
      @Override public void run() {
        try {
          q.take();
        } catch (InterruptedException ignored) {
          return;
        }
        Assert.fail("The take operation should have been interrupted!");
      }
    });
    t.start();
    Thread.sleep(1000);
    t.interrupt();
  }

  @Test public void takeNonEmpty() throws IOException, InterruptedException {
    BlockingObjectQueue<byte[]> q = newQueue();
    q.put(new byte[]{1, 2});
    assertArrayEquals(new byte[]{1, 2}, q.take());
  }

  @Test public void pollEmpty() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertNull(q.poll());
  }

  @Test public void pollNonEmpty() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    q.put(new byte[]{1, 2});
    assertArrayEquals(new byte[]{1, 2}, q.poll());
  }

  @Test public void remainingCapacity() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertEquals(0, q.size());
    assertEquals(Integer.MAX_VALUE, q.remainingCapacity());
    q.add(new byte[]{});
    assertEquals(1, q.size());
    assertEquals(Integer.MAX_VALUE, q.remainingCapacity());
  }

  @Test public void remove() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertEquals(0, q.size());
    assertTrue(q.add(new byte[]{1}));
    assertEquals(1, q.size());
    assertTrue(q.add(new byte[]{2}));
    assertEquals(2, q.size());
    assertArrayEquals(new byte[]{1}, q.remove());
    assertEquals(1, q.size());
    assertArrayEquals(new byte[]{2}, q.remove());
    assertEquals(0, q.size());
  }

  @Test public void contains() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertEquals(0, q.size());
    assertTrue(q.add(new byte[]{1}));
    assertEquals(1, q.size());
    assertTrue(q.add(new byte[]{2}));
    assertEquals(2, q.size());

    assertTrue(q.contains(new byte[]{1}));
    assertTrue(q.contains(new byte[]{2}));
    assertFalse(q.contains(new byte[]{3}));
  }

  @Test public void drainTo() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    List<byte[]> ref = Arrays.asList(new byte[]{1}, new byte[]{2});
    assertTrue(q.addAll(ref));
    assertEquals(ref.size(), q.size());

    List<byte[]> out = new ArrayList<>();
    assertEquals(ref.size(), q.drainTo(out));
    assertEquals(ref.size(), out.size());
    assertTrue(q.isEmpty());
    assertArrayEquals(ref.toArray(), out.toArray());
  }

  @Test public void drainTo1() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    List<byte[]> ref = Arrays.asList(new byte[]{1}, new byte[]{2}, new byte[]{3});
    assertTrue(q.addAll(ref));
    assertEquals(ref.size(), q.size());

    int drain = ref.size() - 1;
    List<byte[]> out = new ArrayList<>();
    assertEquals(drain, q.drainTo(out, drain));
    assertEquals(drain, out.size());
    assertEquals(1, q.size());
    assertArrayEquals(ref.subList(0, drain).toArray(), out.toArray());
  }

  @Test public void drainTo1TooSmall() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    List<byte[]> ref = Arrays.asList(new byte[]{1}, new byte[]{2});
    assertTrue(q.addAll(ref));
    assertEquals(ref.size(), q.size());

    List<byte[]> out = new ArrayList<>();
    assertEquals(ref.size(), q.drainTo(out, Integer.MAX_VALUE));
    assertEquals(ref.size(), out.size());
    assertTrue(q.isEmpty());
    assertArrayEquals(ref.toArray(), out.toArray());
  }

  @Test public void remove1() throws IOException {
    byte[] o1 = {1};
    byte[] o2 = {2};

    BlockingObjectQueue<byte[]> q = newQueue();
    assertEquals(0, q.size());
    assertTrue(q.add(o1));
    assertEquals(1, q.size());
    assertTrue(q.add(o2));
    assertEquals(2, q.size());

    assertFalse(q.remove(new byte[]{3}));
    assertEquals(2, q.size());
    assertTrue(q.remove(o1));
    assertEquals(1, q.size());
    assertTrue(q.remove(o2));
    assertEquals(0, q.size());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void remove1NotHead() throws IOException {
    byte[] o1 = {1};
    byte[] o2 = {2};

    BlockingObjectQueue<byte[]> q = newQueue();
    assertEquals(0, q.size());
    assertTrue(q.add(o1));
    assertEquals(1, q.size());
    assertTrue(q.add(o2));
    assertEquals(2, q.size());

    assertTrue(q.remove(o2));
  }

  @Test(expected = NoSuchElementException.class)
  public void poll1Empty() throws IOException, InterruptedException {
    BlockingObjectQueue<byte[]> q = newQueue();
    q.poll(1, TimeUnit.SECONDS);
  }

  @Test public void poll1NonEmpty() throws IOException, InterruptedException {
    BlockingObjectQueue<byte[]> q = newQueue();
    q.put(new byte[]{1, 2});
    assertArrayEquals(new byte[]{1, 2}, q.poll(1, TimeUnit.SECONDS));
  }

  @Test(expected = NoSuchElementException.class)
  public void elementEmpty() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    q.element();
  }

  @Test public void elementNonEmpty() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertTrue(q.add(new byte[]{1}));
    assertEquals(1, q.size());
    assertArrayEquals(new byte[]{1}, q.element());
  }

  @Test public void peekEmpty() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertNull(q.peek());
  }

  @Test public void peekNonEmpty() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertTrue(q.add(new byte[]{1}));
    assertEquals(1, q.size());
    assertArrayEquals(new byte[]{1}, q.peek());
  }

  @Test(expected = NoSuchElementException.class)
  public void peek1Empty() throws IOException, InterruptedException {
    BlockingObjectQueue<byte[]> q = newQueue();
    q.peek(1, TimeUnit.SECONDS);
  }

  @Test public void peek1NonEmpty() throws IOException, InterruptedException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertTrue(q.add(new byte[]{1}));
    assertEquals(1, q.size());
    assertArrayEquals(new byte[]{1}, q.peek(1, TimeUnit.SECONDS));
  }

  @Test public void size() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertEquals(0, q.size());
    assertTrue(q.add(new byte[]{1}));
    assertEquals(1, q.size());

    assertTrue(q.add(new byte[]{2}));
    assertEquals(2, q.size());
  }

  @Test public void isEmpty() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertTrue(q.isEmpty());
    assertTrue(q.add(new byte[]{1}));
    assertFalse(q.isEmpty());
    assertNotNull(q.remove());
    assertTrue(q.isEmpty());
  }

  @Test public void iterator() throws IOException {
    byte[][] ref = {{1}, {2}, {3}};
    BlockingObjectQueue<byte[]> q = newQueue();
    q.addAll(Arrays.asList(ref));

    int i = 0;
    Iterator<byte[]> it = q.iterator();
    while (it.hasNext()) {
      assertArrayEquals(ref[i++], it.next());
    }

    assertEquals(ref.length, i);
  }

  @Test public void toArray() throws IOException {
    byte[][] bytes = {{1}, {2}};
    BlockingObjectQueue<byte[]> q = newQueue();
    assertTrue(q.add(new byte[]{1}));
    assertTrue(q.add(new byte[]{2}));

    assertArrayEquals(bytes, q.toArray());
  }

  @Test public void toArray1() throws IOException {
    byte[][] bytes = {{1}, {2}};
    BlockingObjectQueue<byte[]> q = newQueue();
    assertTrue(q.add(new byte[]{1}));
    assertTrue(q.add(new byte[]{2}));

    assertArrayEquals(bytes, q.toArray(new byte[0][]));
  }

  @Test public void containsAll() throws IOException {
    List<byte[]> bytes = Arrays.asList(new byte[]{1}, new byte[]{2});
    BlockingObjectQueue<byte[]> q = newQueue();
    assertFalse(q.containsAll(bytes));
    assertTrue(q.add(new byte[]{1}));
    assertEquals(1, q.size());
    assertFalse(q.containsAll(bytes));
    assertTrue(q.add(new byte[]{2}));
    assertEquals(2, q.size());
    assertTrue(q.containsAll(bytes));
    assertTrue(q.add(new byte[]{3}));
    assertEquals(3, q.size());
    assertTrue(q.containsAll(bytes));
  }

  @Test public void addAll() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertTrue(q.addAll(Arrays.asList(new byte[]{1}, new byte[]{2})));
    assertArrayEquals(new byte[]{1}, q.remove());
    assertArrayEquals(new byte[]{2}, q.remove());
    assertTrue(q.isEmpty());
  }

  @Test public void removeAll() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertTrue(q.add(new byte[]{1}));
    assertTrue(q.add(new byte[]{2}));
    assertTrue(q.add(new byte[]{3}));

    List<byte[]> remove = Arrays.asList(new byte[]{1}, new byte[]{2});
    assertTrue(q.removeAll(remove));
    assertArrayEquals(new byte[]{3}, q.remove());
    assertTrue(q.isEmpty());
  }

  @Test public void retainAll() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertTrue(q.add(new byte[]{1}));
    assertTrue(q.add(new byte[]{2}));
    assertTrue(q.add(new byte[]{3}));

    List<byte[]> retain = Arrays.asList(new byte[]{2}, new byte[]{3});
    assertTrue(q.retainAll(retain));
    assertArrayEquals(new byte[]{2}, q.remove());
    assertArrayEquals(new byte[]{3}, q.remove());
    assertTrue(q.isEmpty());
  }

  @Test public void clear() throws IOException {
    BlockingObjectQueue<byte[]> q = newQueue();
    assertTrue(q.add(new byte[]{1}));
    assertTrue(q.add(new byte[]{2}));
    assertEquals(2, q.size());
    q.clear();
    assertEquals(0, q.size());
  }
}
