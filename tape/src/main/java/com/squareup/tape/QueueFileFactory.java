package com.squareup.tape;

import java.io.File;
import java.io.IOException;

/**
 * Factory for QueueFiles.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public abstract class QueueFileFactory {

  /**
   * Open a queue file that only supports up to 2G.
   *
   * @param file File to use for queue file. Will create and initialize if necessary.
   * @return The Queue File.
   * @throws IOException Exceptions when creating the queue file.
   */
  public static QueueFile open(File file) throws IOException {
    return new QueueFileImpl(file);
  }

  /**
   * Open a queue file that can span more than 2G (limited by the operating system).
   *
   * @param file File to use for queue file. Will create and initialize if necessary.
   * @return The Queue File.
   * @throws IOException Exceptions when creating the queue file.
   */
  public static QueueFile openLarge(File file) throws IOException {
    return new BigQueueFileImpl(file);
  }

  /**
   * Migrate a queue file that supports up to 2G file size, reading its contents and writing them to a new
   * queue file that can span up to what's allowed by the operating system.
   * <p/>
   * The files must not be the same and new file can already exist as elements are moved one-by-one and if the process
   * crashes, the migration process will continue. There is a potential of copying the same element twice if the element
   * did in fact finish writing to the new queue but cannot be removed from the old one.
   * <p/>
   * If the old file does not exist, no migration will happen. After migration, the old file can be safely deleted.
   *
   * @param oldFile Old file, must be one that's opened by {@link #open(java.io.File)}. If it does not exist, migration
   *                will not happen.
   * @param newFile New file.
   * @return New queue file that's backed by {@link BigQueueFileImpl}.
   * @throws IOException
   */
  public static QueueFile openAndMigrate(File oldFile, File newFile, boolean deleteWhenDone) throws IOException {
    if (!oldFile.exists()) {
      return openLarge(newFile);
    }
    QueueFile oldQueue = open(oldFile);
    QueueFile newQueue = openLarge(newFile);
    while (!oldQueue.isEmpty()) {
      newQueue.add(oldQueue.peek());
      oldQueue.remove();
    }
    if (deleteWhenDone) oldFile.delete();
    return newQueue;
  }
}
