package com.squareup.tape2;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is a simple and unbounded {@link BlockingQueue} implementation as a wrapper
 * around a {@link QueueFile}. Thread safety is implemented using a single lock around all
 * operations to the backing {@link QueueFile}.
 *
 * @param <E> the element type
 */
public class BlockingObjectQueue<E> implements BlockingQueue<E>, Closeable {

  private final Lock lock = new ReentrantLock();

  private final Condition nonEmpty = lock.newCondition();

  private final ObjectQueue<E> queue;

  public BlockingObjectQueue(ObjectQueue<E> queue) {
    this.queue = queue;
  }

  public ObjectQueue<E> queue() {
    return queue;
  }

  /**
   * Creates a new {@link BlockingQueue} of type {@code T} backed by the given
   * {@link ObjectQueue} of type {@code T}.
   *
   * @param qf the queue file which should not be shared to other places
   * @param conv the converter used to convert from and to byte arrays
   * @param <T> the element type
   * @return a BlockObjectQueue implementation
   */
  public static <T> BlockingObjectQueue<T> create(QueueFile qf, ObjectQueue.Converter<T> conv) {
    return new BlockingObjectQueue<>(new FileObjectQueue<>(qf, conv));
  }

  /**
   * Creates a new {@link BlockingQueue} of type {@code T} backed by the given
   * {@link ObjectQueue} of type {@code T}.
   *
   * @param qf the queue file which should not be shared to other places
   * @return a BlockObjectQueue implementation
   */
  public static BlockingObjectQueue<byte[]> create(QueueFile qf) {
    return new BlockingObjectQueue<>(qf);
  }

  @Override public boolean add(E element) {
    lock.lock();
    try {
      queue.add(element);
      nonEmpty.signal();
      return true;
    } catch (IOException e) {
      QueueFile.<Error>getSneakyThrowable(e);
      return false;
    } finally {
      lock.unlock();
    }
  }

  @Override public boolean offer(E element) {
    return add(element);
  }

  @Override public void put(E element) {
    add(element);
  }

  @Override public boolean offer(E element, long timeout, TimeUnit unit) {
    return add(element);
  }

  @Override public E take() throws InterruptedException {
    lock.lock();
    try {
      while (queue.isEmpty()) {
        nonEmpty.await();
      }
      E peek = queue.peek();
      if (peek == null) {
        // this won't happen unless the backing queue has been shared.
        throw new NoSuchElementException();
      }
      queue.remove();
      return peek;
    } catch (IOException e) {
      QueueFile.<Error>getSneakyThrowable(e);
      return null;
    } finally {
      lock.unlock();
    }
  }

  @Override public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    lock.lock();
    try {
      long timeoutNanos = unit.toNanos(timeout);
      while (queue.isEmpty() && timeoutNanos > 0) {
        timeoutNanos = nonEmpty.awaitNanos(timeoutNanos);
      }
      E peek = queue.peek();
      if (peek == null) {
        // this won't happen unless the backing queue has been shared.
        throw new NoSuchElementException();
      }
      queue.remove();
      return peek;
    } catch (IOException e) {
      QueueFile.<Error>getSneakyThrowable(e);
      return null;
    } finally {
      lock.unlock();
    }
  }

  @Override public int remainingCapacity() {
    return Integer.MAX_VALUE; // as per BlockingQueue javadoc for unbounded queues
  }

  /**
   * The underlying {@link QueueFile} only supports removing the head, so this will only work if
   * head matches.
   * */
  @Override public boolean remove(Object o) {
    lock.lock();
    try {
      if (queue.isEmpty()) {
        return false;
      }
      Iterator<E> it = queue.iterator();
      while (it.hasNext()) {
        if (Objects.deepEquals(it.next(), o)) {
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
    lock.lock();
    try {
      for (E entry : queue) {
        if (Objects.deepEquals(entry, o)) {
          return true;
        }
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  @Override public int drainTo(Collection<? super E> c) {
    lock.lock();
    try {
      int size = queue.size();
      Iterator<E> it = queue.iterator();
      while (it.hasNext()) {
        c.add(it.next());
        it.remove();
      }
      return size;
    } finally {
      lock.unlock();
    }
  }

  @Override public int drainTo(Collection<? super E> c, int maxElements) {
    if (maxElements == 0) {
      return 0;
    }
    lock.lock();
    try {
      Iterator<E> it = queue.iterator();
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

  @Override public E remove() {
    lock.lock();
    try {
      if (queue.isEmpty()) {
        throw new NoSuchElementException();
      }
      E peek = queue.peek();
      queue.remove();
      return peek;
    } catch (IOException e) {
      QueueFile.<Error>getSneakyThrowable(e);
      return null;
    } finally {
      lock.unlock();
    }
  }

  @Override public E poll() {
    lock.lock();
    try {
      if (queue.isEmpty()) {
        return null;
      }
      E peek = queue.peek();
      queue.remove();
      return peek;
    } catch (IOException e) {
      QueueFile.<Error>getSneakyThrowable(e);
      return null;
    } finally {
      lock.unlock();
    }
  }

  @Override public E element() {
    lock.lock();
    try {
      if (queue.isEmpty()) {
        throw new NoSuchElementException();
      }
      return queue.peek();
    } catch (IOException e) {
      QueueFile.<Error>getSneakyThrowable(e);
      return null;
    } finally {
      lock.unlock();
    }
  }

  @Override public E peek() {
    lock.lock();
    try {
      if (queue.isEmpty()) {
        return null;
      }
      return queue.peek();
    } catch (IOException e) {
      QueueFile.<Error>getSneakyThrowable(e);
      return null;
    } finally {
      lock.unlock();
    }
  }

  /**
   * This overload is an addition to the {@link BlockingQueue} interface similar to the
   * {@link #poll(long, TimeUnit)} method, a blocking peek operation with a timeout.
   *
   * @param timeout the timeout
   * @param unit the time unit of the timeout
   * @return the head of this queue, or {@code null} if this queue is empty
   * @see #peek() for more information
   * @throws InterruptedException if interrupted while waiting
   */
  public E peek(long timeout, TimeUnit unit) throws InterruptedException {
    lock.lock();
    try {
      long timeoutNanos = unit.toNanos(timeout);
      while (queue.isEmpty() && timeoutNanos > 0) {
        timeoutNanos = nonEmpty.awaitNanos(timeoutNanos);
      }
      E peek = queue.peek();
      if (peek == null) {
        // this won't happen unless the backing queue has been shared.
        throw new NoSuchElementException();
      }
      return peek;
    } catch (IOException e) {
      QueueFile.<Error>getSneakyThrowable(e);
      return null;
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

  @Override public Iterator<E> iterator() {
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
      for (E e : queue) {
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

  @Override public boolean addAll(Collection<? extends E> c) {
    if (c.isEmpty()) {
      return false;
    }
    lock.lock();
    try {
      for (E e : c) {
        queue.add(e);
      }
      nonEmpty.signal();
      return true;
    } catch (IOException e) {
      QueueFile.<Error>getSneakyThrowable(e);
      return false;
    } finally {
      lock.unlock();
    }
  }

  /**
   * The underlying {@link QueueFile} only supports removing the head, so this will only work if
   * head matches.
   * */
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

  private static boolean contains(Iterable<?> haystack, Object needle) {
    for (Object o : haystack) {
      if (Objects.deepEquals(o, needle)) {
        return true;
      }
    }
    return false;
  }

  /**
   * The underlying {@link QueueFile} only supports removing the head, so this will only work if
   * head matches.
   * */
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
      Iterator<E> it = queue.iterator();
      while (it.hasNext()) {
        if (!contains(c, it.next())) {
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
      QueueFile.<Error>getSneakyThrowable(e);
    } finally {
      lock.unlock();
    }
  }

  @Override public void close() throws IOException {
    queue.close();
  }
}
