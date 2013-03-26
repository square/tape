/*
 * Copyright (C) 2012 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/queue.h>

#include "minunit.h"

#include "../logutil.h"
#include "../queuefile.h"
#include "../types.h"
#include "../fileio.h"

/**
 * Takes up 33401 bytes in the queue (N*(N+1)/2+4*N). Picked 254 instead of
 * 255 so that the number of bytes isn't a multiple of 4.
 */
#define N 254
static byte* values[N];

#define TEST_QUEUE_FILENAME "test.queue"
static QueueFile* queue;
int tests_run = 0;


typedef STAILQ_HEAD(listHead_t, listEntry_t) listHead;
static void _assertPeekCompare(QueueFile *queue, const byte* data,
                               uint32_t length);
static void _assertPeekCompareRemove(QueueFile *queue, const byte* data,
                                     uint32_t length);
static void _assertPeekCompareRemoveDequeue(QueueFile *queue,
                                            listHead *expectqueue);

static void mu_setup() {
  int i;
  for (i = 0; i < N; i++) {
    values[i] = malloc((size_t) i);
    // Example: values[3] = { 3, 2, 1 }
    int ii;
    for (ii = 0; ii < i; ii++) values[i][ii] = (byte) (i - ii);
  }

  // Default case is start with a clean queue.
  remove(TEST_QUEUE_FILENAME);
  queue = QueueFile_new(TEST_QUEUE_FILENAME);
  mu_assert_notnull(queue);
}

static void mu_teardown() {
  QueueFile_closeAndFree(queue);
  int i;
  for (i = 0; i < N; i++) {
    free(values[i]);
  }
}

static void testSimpleAddOneElement() {
  byte* expected = values[253];
  QueueFile_add(queue, expected, 0, 253);
  _assertPeekCompare(queue, expected, 253);
}

static void testAddOneElement() {
  byte* expected = values[253];
  QueueFile_add(queue, expected, 0, 253);
  _assertPeekCompare(queue, expected, 253);
  QueueFile_closeAndFree(queue);
  queue = QueueFile_new(TEST_QUEUE_FILENAME);
  _assertPeekCompare(queue, expected, 253);
}


// stuct for test queue.
struct listEntry_t {
  byte *data;
  uint32_t length;
  STAILQ_ENTRY(listEntry_t) next_entry;
};

struct listEntry_t* listEntry_new(byte *argdata, uint32_t arglen) {
  struct listEntry_t* retval = malloc(sizeof(struct listEntry_t));
  mu_assert_notnull(retval);
  retval->data = argdata;
  retval->length = arglen;
  return retval;
};

static void testAddAndRemoveElements() {
  QueueFile_closeAndFree(queue);
  time_t start = time(NULL);

  listHead expect = STAILQ_HEAD_INITIALIZER(expect);
  struct listEntry_t* entry;

  int round;
  for (round = 0; round < 5; round++) {
    queue = QueueFile_new(TEST_QUEUE_FILENAME);
    int i;
    for (i = 0; i < N; i++) {
      QueueFile_add(queue, values[i], 0, (uint32_t) i);
      entry = listEntry_new(values[i], (uint32_t) i);
      STAILQ_INSERT_TAIL(&expect, entry, next_entry);
    }

    // Leave N elements in round N, 15 total for 5 rounds. Removing all the
    // elements would be like starting with an empty queue.
    for (i = 0; i < N - round - 1; i++) {
      _assertPeekCompareRemoveDequeue(queue, &expect);
    }
    QueueFile_closeAndFree(queue);
  }

  // Remove and validate remaining 15 elements.
  queue = QueueFile_new(TEST_QUEUE_FILENAME);
  mu_assert(QueueFile_size(queue) == 15);

  int expectCount = 0;
  STAILQ_FOREACH(entry, &expect, next_entry) {
    ++expectCount;
  }
  mu_assert(expectCount == 15);

  while (!STAILQ_EMPTY(&expect)) {
    _assertPeekCompareRemoveDequeue(queue, &expect);
  }

  time_t stop = time(NULL);
  LOG(LINFO, "Ran in %lf seconds.", difftime(stop, start));
}

static void testFileLength() {
  mu_assert(FileIo_getLength(_for_testing_QueueFile_getFhandle(queue)) ==
            QueueFile_getFileLength(queue));
}

/** Tests queue expansion when the data crosses EOF. */
static void testSplitExpansion() {
  // This should result in 3560 bytes.
  int max = 80;

  listHead expect = STAILQ_HEAD_INITIALIZER(expect);
  struct listEntry_t* entry;

  int i;
  for (i = 0; i < max; i++) {
    QueueFile_add(queue, values[i], 0, (uint32_t) i);
    entry = listEntry_new(values[i], (uint32_t) i);
    STAILQ_INSERT_TAIL(&expect, entry, next_entry);
  }

  // Remove all but 1.
  for (i = 1; i < max; i++) {
    _assertPeekCompareRemoveDequeue(queue, &expect);
  }

  off_t flen1 = FileIo_getLength(_for_testing_QueueFile_getFhandle(queue));

  // This should wrap around before expanding.
  for (i = 0; i < N; i++) {
    QueueFile_add(queue, values[i], 0, (uint32_t) i);
    entry = listEntry_new(values[i], (uint32_t) i);
    STAILQ_INSERT_TAIL(&expect, entry, next_entry);
  }

  while (!STAILQ_EMPTY(&expect)) {
    _assertPeekCompareRemoveDequeue(queue, &expect);
  }

  off_t flen2 = FileIo_getLength(_for_testing_QueueFile_getFhandle(queue));
  mu_assertm(flen1 == flen2, "file size should remain same");
}

#define ARR_A_SIZE 5
static const byte arrA[ARR_A_SIZE] = {1, 2, 3, 4, 5};
#define ARR_B_SIZE 3
static const byte arrB[ARR_B_SIZE] = {3, 4, 5};

static int forEachIterationCount = 0;
static bool forEachReader(QueueFile_ElementStream* stream, uint32_t length) {
  if (forEachIterationCount == 0) {
    mu_assert(length == ARR_A_SIZE);
    mu_assert_notnull(stream);

    // Read in small chunks to test reader.
    byte actual[ARR_A_SIZE];
    int elementIteration = 0;
    uint32_t expectedRemaining[] = { 3, 1, 0 };
    uint32_t remaining = ARR_A_SIZE;
    do {
      // i.e. read past end i.e. 3 reads of 2 > 5
      mu_assert(QueueFile_readElementStream(stream, actual + ARR_A_SIZE -
                                            remaining, 2, &remaining) > 0);
      mu_assert(expectedRemaining[elementIteration] == remaining);
      ++elementIteration;
    } while (remaining > 0 && elementIteration < 4);
    mu_assert(elementIteration == 3);
    mu_assert_memcmp(actual, arrA, ARR_A_SIZE);
  } else if (forEachIterationCount == 1) {
    mu_assert(length == ARR_B_SIZE);
    mu_assert_notnull(stream);
    byte actual[ARR_B_SIZE];
    uint32_t remaining;
    mu_assert(QueueFile_readElementStream(stream,
        actual, ARR_B_SIZE, &remaining) == ARR_B_SIZE);
    mu_assert(remaining == 0);
    mu_assert_memcmp(actual, arrB, ARR_B_SIZE);
  } else {
    mu_assertm(false, "Should never iterate beyond 2");
  }
  forEachIterationCount++;
  return true;
}

static void testForEach() {
  mu_assert(QueueFile_add(queue, arrA, 0, ARR_A_SIZE));
  mu_assert(QueueFile_add(queue, arrB, 0, ARR_B_SIZE));

  mu_assert(QueueFile_forEach(queue, forEachReader));
  _assertPeekCompare(queue, arrA, ARR_A_SIZE);
  mu_assertm(forEachIterationCount == 2, "expected 2 iterations");
}


static bool peekReaderAblock(QueueFile_ElementStream* stream, uint32_t length) {
  mu_assert(length == ARR_A_SIZE);
  byte actual[length];
  uint32_t remaining;
  mu_assert(QueueFile_readElementStream(stream, actual, length, &remaining) ==
            length);
  mu_assert(remaining == 0);
  mu_assert_memcmp(actual, arrA, length);
  return true;
}

static bool peekReaderAbytewise(QueueFile_ElementStream* stream, uint32_t length) {
  mu_assert(length == ARR_A_SIZE);
  mu_assert(QueueFile_readElementStreamNextByte(stream) == 1);
  mu_assert(QueueFile_readElementStreamNextByte(stream) == 2);
  mu_assert(QueueFile_readElementStreamNextByte(stream) == 3);
  mu_assert(QueueFile_readElementStreamNextByte(stream) == 4);
  mu_assert(QueueFile_readElementStreamNextByte(stream) == 5);
  mu_assert(QueueFile_readElementStreamNextByte(stream) == -1);
  return true;
}

static bool peekReaderBblock(QueueFile_ElementStream* stream, uint32_t length) {
  mu_assert(length == ARR_B_SIZE);
  byte actual[length];
  uint32_t remaining;
  mu_assert(QueueFile_readElementStream(stream, actual, length, &remaining) ==
            length);
  mu_assert(remaining == 0);
  mu_assert_memcmp(actual, arrB, length);
  return true;
}

static void testPeekWithElementReader() {
  mu_assert(QueueFile_add(queue, arrA, 0, ARR_A_SIZE));
  mu_assert(QueueFile_add(queue, arrB, 0, ARR_B_SIZE));

  QueueFile_peekWithElementReader(queue, peekReaderAblock); // ignore response.
  QueueFile_peekWithElementReader(queue, peekReaderAbytewise); // ignore response.

  mu_assert(QueueFile_remove(queue));

  QueueFile_peekWithElementReader(queue, peekReaderBblock); // ignore response.

  mu_assert(QueueFile_size(queue) == 1);
  _assertPeekCompare(queue, arrB, ARR_B_SIZE);
}

/**
 * Exercise a bug where wrapped elements were getting corrupted when the
 * QueueFile was forced to expand in size and a portion of the final Element
 * had been wrapped into space at the beginning of the file.
 */
static void testFileExpansionDoesntCorruptWrappedElements() {

  // Create test data - 1k blocks marked consecutively 1, 2, 3, 4 and 5.
  uint32_t valuesCount = 5;
  uint32_t valuesLength = 1024;
  byte* values[valuesCount];
  uint32_t blockNum;
  for (blockNum = 0; blockNum < valuesCount; blockNum++) {
    values[blockNum] = malloc((size_t) valuesLength);
    uint32_t i;
    for (i = 0; i < valuesLength; i++) {
      values[blockNum][i] = (byte) (blockNum + 1);
    }
  }

  // First, add the first two blocks to the queue, remove one leaving a
  // 1K space at the beginning of the buffer.
  mu_assert(QueueFile_add(queue, values[0], 0, valuesLength));
  mu_assert(QueueFile_add(queue, values[1], 0, valuesLength));
  mu_assert(QueueFile_remove(queue));

  // The trailing end of block "4" will be wrapped to the start of the buffer.
  mu_assert(QueueFile_add(queue, values[2], 0, valuesLength));
  mu_assert(QueueFile_add(queue, values[3], 0, valuesLength));

  // Cause buffer to expand as there isn't space between the end of block "4"
  // and the start of block "2".  Internally the queue should cause block "4"
  // to be contiguous, but there was a bug where that wasn't happening.
  mu_assert(QueueFile_add(queue, values[4], 0, valuesLength));

  // Make sure values are not corrupted, specifically block "4" that wasn't
  // being made contiguous in the version with the bug.
  uint32_t i;
  for (i = 1; i < valuesCount; i++) { // start at 1!
    uint32_t length;
    byte* value = QueueFile_peek(queue, &length);
    mu_assert(length == valuesLength);
    mu_assert(QueueFile_remove(queue));

    uint32_t j;
    for (j = 0; j < length; j++) {
      mu_assert(value[j] == i + 1);
    }
    free(value);
  }

  for (blockNum = 0; blockNum < valuesCount; blockNum++) {
    free(values[blockNum]);
  }
}

/**
 * Exercise a bug where wrapped elements were getting corrupted when the
 * QueueFile was forced to expand in size and a portion of the final Element
 * had been wrapped into space at the beginning of the file - if multiple
 * Elements have been written to empty buffer space at the start does the
 * expansion correctly update all their positions?
 */
static void testFileExpansionCorrectlyMovesElements() {

  // Create test data - 1k blocks marked consecutively 1, 2, 3, 4 and 5.
  uint32_t valuesCount = 5;
  uint32_t valuesLength = 1024;
  byte* values[valuesCount];
  uint32_t blockNum;
  for (blockNum = 0; blockNum < valuesCount; blockNum++) {
    values[blockNum] = malloc((size_t) valuesLength);
    uint32_t i;
    for (i = 0; i < valuesLength; i++) {
      values[blockNum][i] = (byte) (blockNum + 1);
    }
  }

  // smaller data elements
  uint32_t smallerCount = 3;
  uint32_t smallerLength = 256;
  byte* smaller[smallerCount];
  for (blockNum = 0; blockNum < smallerCount; blockNum++) {
    smaller[blockNum] = malloc((size_t) smallerLength);
    uint32_t i;
    for (i = 0; i < smallerLength; i++) {
      smaller[blockNum][i] = (byte) (blockNum + 6);
    }
  }

  // First, add the first two blocks to the queue, remove one leaving a
  // 1K space at the beginning of the buffer.
  mu_assert(QueueFile_add(queue, values[0], 0, valuesLength));
  mu_assert(QueueFile_add(queue, values[1], 0, valuesLength));
  mu_assert(QueueFile_remove(queue));

  // The trailing end of block "4" will be wrapped to the start of the buffer.
  mu_assert(QueueFile_add(queue, values[2], 0, valuesLength));
  mu_assert(QueueFile_add(queue, values[3], 0, valuesLength));

  // Now fill in some space with smaller blocks, none of which will cause
  // an expansion.
  mu_assert(QueueFile_add(queue, smaller[0], 0, smallerLength));
  mu_assert(QueueFile_add(queue, smaller[1], 0, smallerLength));
  mu_assert(QueueFile_add(queue, smaller[2], 0, smallerLength));

  // Cause buffer to expand as there isn't space between the end of the
  // smaller block "8" and the start of block "2".  Internally the queue
  // should cause all of tbe smaller blocks, and the trailing end of
  // block "5" to be moved to the end of the file.
  mu_assert(QueueFile_add(queue, values[4], 0, valuesLength));

  uint32_t expectedBlockLen = 7;
  byte expectedBlockNumbers[] = {2, 3, 4, 6, 7, 8, 5};
  uint32_t expectedLengths[] = {valuesLength, valuesLength, valuesLength,
      smallerLength, smallerLength, smallerLength, valuesLength};

  // Make sure values are not corrupted, specifically block "4" that wasn't
  // being made contiguous in the version with the bug.
  uint32_t i;
  for (i = 0; i < expectedBlockLen; i++) {
    byte expectedBlockNumber = expectedBlockNumbers[i];
    uint32_t length;
    byte* value = QueueFile_peek(queue, &length);
    mu_assert(length == expectedLengths[i]);
    mu_assert(QueueFile_remove(queue));

    uint32_t j;
    for (j = 0; j < length; j++) {
      mu_assert(value[j] == expectedBlockNumber);
    }
    free(value);
  }
  mu_assert(QueueFile_isEmpty(queue));

  for (blockNum = 0; blockNum < valuesCount; blockNum++) {
    free(values[blockNum]);
  }

  for (blockNum = 0; blockNum < smallerCount; blockNum++) {
    free(smaller[blockNum]);
  }
}

static void testFailedAdd() {
  mu_assert(QueueFile_add(queue, values[253], 0, 253));
  _for_testing_FileIo_failAllWrites(true);
  mu_assert(!QueueFile_add(queue, values[252], 0, 252));
  _for_testing_FileIo_failAllWrites(false);

  // Allow a subsequent add to succeed.
  mu_assert(QueueFile_add(queue, values[251], 0, 251));

  QueueFile_closeAndFree(queue);
  queue = QueueFile_new(TEST_QUEUE_FILENAME);

  mu_assert(QueueFile_size(queue) == 2);
  _assertPeekCompareRemove(queue, values[253], 253);
  _assertPeekCompareRemove(queue, values[251], 251);
}

static void testFailedRemoval() {
  mu_assert(QueueFile_add(queue, values[253], 0, 253));
  _for_testing_FileIo_failAllWrites(true);
  mu_assert(!QueueFile_remove(queue));
  _for_testing_FileIo_failAllWrites(false);

  QueueFile_closeAndFree(queue);
  queue = QueueFile_new(TEST_QUEUE_FILENAME);

  mu_assert(QueueFile_size(queue) == 1);
  _assertPeekCompareRemove(queue, values[253], 253);
  mu_assert(QueueFile_add(queue, values[99], 0, 99));
  _assertPeekCompareRemove(queue, values[99], 99);
}

static void testFailedExpansion() {
  mu_assert(QueueFile_add(queue, values[253], 0, 253));
  _for_testing_FileIo_failAllWrites(true);
  byte bigbuf[8000];
  mu_assert(!QueueFile_add(queue, bigbuf, 0, 8000));
  _for_testing_FileIo_failAllWrites(false);

  QueueFile_closeAndFree(queue);
  queue = QueueFile_new(TEST_QUEUE_FILENAME);

  mu_assert(QueueFile_size(queue) == 1);

  _assertPeekCompare(queue, values[253], 253);
  mu_assert(4096 == FileIo_getLength(_for_testing_QueueFile_getFhandle(queue)));
  mu_assert(QueueFile_add(queue, values[99], 0, 99));
  _assertPeekCompareRemove(queue, values[253], 253);
  _assertPeekCompareRemove(queue, values[99], 99);
}

static void testTransferToWithSmallBuffer() {
  uint32_t oldBufferSize = _for_testing_setTransferToCopyBufferSize(5);
  testFileExpansionDoesntCorruptWrappedElements();
  _for_testing_setTransferToCopyBufferSize(oldBufferSize);
}

int main() {
  LOG_SETDEBUGFAILLEVEL_WARN;
  mu_run_test(testSimpleAddOneElement);
  mu_run_test(testAddOneElement);
  mu_run_test(testFileLength);
  mu_run_test(testAddAndRemoveElements);
  mu_run_test(testSplitExpansion);
  mu_run_test(testFileExpansionDoesntCorruptWrappedElements);
  mu_run_test(testFileExpansionCorrectlyMovesElements);
  mu_run_test(testFailedAdd);
  mu_run_test(testFailedRemoval);
  mu_run_test(testFailedExpansion);
  mu_run_test(testForEach);
  mu_run_test(testPeekWithElementReader);
  mu_run_test(testTransferToWithSmallBuffer);

  printf("%d tests passed.\n", tests_run);
  return 0;
}



// ------------- utility methods ---------------

static void _assertPeekCompare(QueueFile *queue, const byte* data,
                               uint32_t length) {
  uint32_t qlength;
  byte* actual = QueueFile_peek(queue, &qlength);
  mu_assert(qlength == length);
  mu_assert_memcmp(data, actual, length);
  free(actual);
}

static void _assertPeekCompareRemove(QueueFile *queue, const byte* data,
                                     uint32_t length) {
  _assertPeekCompare(queue, data, length);
  mu_assert(QueueFile_remove(queue));
}

static void _assertPeekCompareRemoveDequeue(QueueFile *queue,
                                            struct listHead_t *expectqueue) {
  struct listEntry_t *entry = STAILQ_FIRST(expectqueue);
  mu_assert_notnull(entry);
  _assertPeekCompareRemove(queue, entry->data, entry->length);
  STAILQ_REMOVE_HEAD(expectqueue, next_entry);
  free(entry);
}

