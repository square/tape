package com.squareup.tape;

import java.io.IOException;

public interface QueueElementObserver {

  /**
   * Observe elements flowing through the Tape queue, in order to make handling decisions.
   * Specifically, this callback will be called anytime a queue element is:
   * 1) Read from the file (Including during object initialization).
   * or
   * 2) Added to the queue.
   *
   * See [QueueElementObservation] for the information reported.
   *
   * @param observation - Encapsulated data about the element in the queue and the state of the
   * queue. See [QueueElementObservation].
   * @throws IOException - If observer decides to throw an exception to stop processing.
   */
  void observeElement(
      QueueElementObservation observation) throws IOException;

  QueueElementObserver NO_OP_OBSERVER = (irrelevant) -> {
  };

  class QueueElementObservation {
    private final int elementSize;
    private final int maxElementSize;
    private final int fileContentLength;
    private final boolean isAdd;
    private final boolean isInit;

    /**
     * This class represents data we collect about the queue element under observation as well as
     * the state of the queue.
     *
     * @param elementSize - The size of the element under observation.
     * @param maxElementSize - The queue's maximum element size.
     * @param fileContentLength - The current file header's reported length less the file header.
     * @param isAdd - Whether or not the element is currently being added to the queue (as opposed
     * to being read from the queue).
     * @param isInit - Whether or not the queue object is currently being initialized from a file on
     * disk. During initialization the first and last elements as pointed to by the
     * file header are read into the object, so these reads are observed in a special
     * state.
     */
    public QueueElementObservation(
        int elementSize,
        int maxElementSize,
        int fileContentLength,
        boolean isAdd,
        boolean isInit
    ) {
      this.elementSize = elementSize;
      this.maxElementSize = maxElementSize;
      this.fileContentLength = fileContentLength;
      this.isAdd = isAdd;
      this.isInit = isInit;
    }

    public int getElementSize() {
      return elementSize;
    }

    public int getMaxElementSize() {
      return maxElementSize;
    }

    public int getFileContentLength() {
      return fileContentLength;
    }

    public boolean isAdd() {
      return isAdd;
    }

    public boolean isInit() {
      return isInit;
    }
  }
}
