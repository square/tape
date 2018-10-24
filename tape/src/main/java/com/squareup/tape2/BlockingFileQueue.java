package com.squareup.tape2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is a simple and unbounded @{@link BlockingQueue} implementation as a wrapper around a @{@link QueueFile}.
 * Thread safety is implemented using a single lock around all operations to the backing @{@link QueueFile}.
 */
public class BlockingFileQueue implements BlockingQueue<byte[]> {

  private final Lock lock = new ReentrantLock();

  private final Condition nonEmpty = lock.newCondition();

  private final QueueFile queue;

  /**
   * Creates a new @{@link BlockingQueue} of type {@code byte[]} backed by the given @{@link QueueFile}.
   *
   * @param queue the queue file which should not be shared to other places
   * */
  public BlockingFileQueue(QueueFile queue) {
    this.queue = queue;
  }

  @Override public boolean add(byte[] bytes) {
    lock.lock();
    try {
      queue.add(bytes);
      nonEmpty.signal();
      return true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override public boolean offer(byte[] bytes) {
    return add(bytes);
  }

  @Override public void put(byte[] bytes) {
    add(bytes);
  }

  @Override public boolean offer(byte[] bytes, long timeout, TimeUnit unit) {
    return add(bytes);
  }

  @Override public byte[] take() throws InterruptedException {
    lock.lock();
    try {
      while (queue.isEmpty()) {
        nonEmpty.await();
      }
      byte[] peek = queue.peek();
      if (peek == null) {
        throw new IllegalStateException("Queue empty!");
      }
      queue.remove();
      return peek;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override public byte[] poll(long timeout, TimeUnit unit) throws InterruptedException {
    lock.lock();
    try {
      long timeoutNanos = unit.toNanos(timeout);
      while (queue.isEmpty() && timeoutNanos > 0) {
        timeoutNanos = nonEmpty.awaitNanos(timeoutNanos);
      }
      byte[] peek = queue.peek();
      if (peek == null) {
        throw new NoSuchElementException();
      }
      queue.remove();
      return peek;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override public int remainingCapacity() {
    return Integer.MAX_VALUE; // as per BlockingQueue javadoc for unbounded queues
  }

  /** The backing @{@link QueueFile} only supports removing the head, so this will only work if head matches. */
  @Override public boolean remove(Object o) {
    if (o == null) {
      return false;
    }
    if (o.getClass() != byte[].class && o.getClass() != Byte[].class) {
      return false;
    }
    byte[] remove = (byte[])o;
    lock.lock();
    try {
      if (queue.isEmpty()) {
        return false;
      }
      Iterator<byte[]> it = queue.iterator();
      while (it.hasNext()) {
        if (Arrays.equals(it.next(), remove)) {
          it.remove();
          return true;
        }
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  @Override public boolean contains(Object o) {
    if (o == null) {
      return false;
    }
    if (o.getClass() != byte[].class && o.getClass() != Byte[].class) {
      return false;
    }
    byte[] check = (byte[])o;
    lock.lock();
    try {
      for (byte[] entry : queue) {
        if (Arrays.equals(entry, check)) {
          return true;
        }
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  @Override public int drainTo(Collection<? super byte[]> c) {
    lock.lock();
    try {
      int size = queue.size();
      Iterator<byte[]> it = queue.iterator();
      while (it.hasNext()) {
        c.add(it.next());
        it.remove();
      }
      return size;
    } finally {
      lock.unlock();
    }
  }

  @Override public int drainTo(Collection<? super byte[]> c, int maxElements) {
    if (maxElements == 0) {
      return 0;
    }
    lock.lock();
    try {
      Iterator<byte[]> it = queue.iterator();
      int i = 0;
      while (it.hasNext() && i < maxElements) {
        c.add(it.next());
        it.remove();
        i++;
      }
      return i;
    } finally {
      lock.unlock();
    }
  }

  @Override public byte[] remove() {
    lock.lock();
    try {
      if (queue.isEmpty()) {
        throw new NoSuchElementException();
      }
      byte[] peek = queue.peek();
      queue.remove();
      return peek;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override public byte[] poll() {
    lock.lock();
    try {
      if (queue.isEmpty()) {
        return null;
      }
      byte[] peek = queue.peek();
      queue.remove();
      return peek;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override public byte[] element() {
    lock.lock();
    try {
      if (queue.isEmpty()) {
        throw new NoSuchElementException();
      }
      return queue.peek();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override public byte[] peek() {
    lock.lock();
    try {
      if (queue.isEmpty()) {
        return null;
      }
      return queue.peek();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * This overload is an addition to the @{@link BlockingQueue} interface similar to the {@link #poll(long, TimeUnit)}
   * method, a blocking peek operation with a timeout.
   *
   * @param timeout the timeout
   * @param unit the time unit of the timeout
   * @return the head of this queue, or {@code null} if this queue is empty
   * @see #peek() for more information
   * @throws InterruptedException if interrupted while waiting
   */
  public byte[] peek(long timeout, TimeUnit unit) throws InterruptedException {
    lock.lock();
    try {
      long timeoutNanos = unit.toNanos(timeout);
      while (queue.isEmpty() && timeoutNanos > 0) {
        timeoutNanos = nonEmpty.awaitNanos(timeoutNanos);
      }
      byte[] peek = queue.peek();
      if (peek == null) {
        throw new NoSuchElementException();
      }
      return peek;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override public int size() {
    lock.lock();
    try {
      return queue.size();
    } finally {
      lock.unlock();
    }
  }

  @Override public boolean isEmpty() {
    lock.lock();
    try {
      return queue.isEmpty();
    } finally {
      lock.unlock();
    }
  }

  @Override public Iterator<byte[]> iterator() {
    lock.lock();
    try {
      return queue.iterator();
    } finally {
      lock.unlock();
    }
  }

  @Override public Object[] toArray() {
    lock.lock();
    try {
      Object[] out = new Object[queue.size()];
      int i = 0;
      for (byte[] e : queue) {
        out[i++] = e;
      }
      return out;
    } finally {
      lock.unlock();
    }
  }

  @Override @SuppressWarnings("unchecked") public <T> T[] toArray(T[] a) {
    return (T[]) toArray();
  }

  @Override public boolean containsAll(Collection<?> c) {
    lock.lock();
    try {
      for (Object e : c) {
        if (!contains(e)) {
          return false;
        }
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  @Override public boolean addAll(Collection<? extends byte[]> c) {
    if (c.isEmpty()) {
      return false;
    }
    lock.lock();
    try {
      for (byte[] e : c) {
        queue.add(e);
      }
      nonEmpty.signal();
      return true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  /** The backing @{@link QueueFile} only supports removing the head, so this will only work if head matches. */
  @Override public boolean removeAll(Collection<?> c) {
    lock.lock();
    boolean changed = false;
    try {
      for (Object e : c) {
        if (remove(e)) {
          changed = true;
        }
      }
      return changed;
    } finally {
      lock.unlock();
    }
  }

  private static boolean arrayContained(Collection<?> haystack, byte[] needle) {
    for (Object o : haystack) {
      byte[] byteArray = (byte[])o;
      if (Arrays.equals(byteArray, needle)) {
        return true;
      }
    }
    return false;
  }

  /** The backing @{@link QueueFile} only supports removing the head, so this will only work if head matches. */
  @Override public boolean retainAll(Collection<?> c) {
    lock.lock();
    if (c.isEmpty()) {
      if (queue.isEmpty()) {
        return false;
      } else {
        clear();
        return true;
      }
    }
    try {
      boolean changed = false;
      Iterator<byte[]> it = queue.iterator();
      while (it.hasNext()) {
        if (!arrayContained(c, it.next())) {
          it.remove();
          changed = true;
        }
      }
      return changed;
    } finally {
      lock.unlock();
    }
  }

  @Override public void clear() {
    lock.lock();
    try {
      queue.clear();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }
}
