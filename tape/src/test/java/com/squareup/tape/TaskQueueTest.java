package com.squareup.tape;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TaskQueueTest {
  private TaskQueue<Task<String>> taskQueue =
      new TaskQueue<Task<String>>(new InMemoryObjectQueue<Task<String>>());

  @SuppressWarnings("unchecked")
  private final Task<String> task = mock(Task.class);

  @SuppressWarnings("unchecked")
  @Test
  public void correctTaskQueueInstanceIsPassedToTheListener() {
    ObjectQueue.Listener<Task<String>> mockListener = mock(ObjectQueue.Listener.class);

    // Register a listener on the taskQueue instance.
    taskQueue.setListener(mockListener);
    taskQueue.add(task);

    // Expect the first argument to be the same instance we registered with.
    verify(mockListener).onAdd(taskQueue, task);

    taskQueue.remove();

    // Same story with remove; the taskQueue should be the one we registered with.
    verify(mockListener).onRemove(taskQueue);
  }
}
