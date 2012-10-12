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

#include "minunit.h"

#include "../logutil.h"
#include "../queuefile.h"
#include "../types.h"

/**
 * Takes up 33401 bytes in the queue (N*(N+1)/2+4*N). Picked 254 instead of
 * 255 so that the number of bytes isn't a multiple of 4.
 */
#define N 254
static byte* values[N];

#define TEST_QUEUE_FILENAME "test.queue"
static QueueFile* queue;
int tests_run = 0;


static void mu_setup() {
  int i;
  for (i = 0; i < N; i++) {
    values[i] = malloc((size_t)i);
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
  QueueFile_close(queue);
  free(queue);

  int i;
  for (i = 0; i < N; i++) {
    free(values[i]);
  }
}

static void testSimpleAddOneElement() {
  byte* expected = values[253];
  QueueFile_add(queue, expected, 0, 253);
  byte* actual = QueueFile_peek(queue);
  mu_assert_memcmp(expected, actual, 253);
  free(actual);
}

int main() {
  LOG_SETDEBUGFAILLEVEL_WARN;
  mu_run_test(testSimpleAddOneElement);
  printf("%d tests passed.\n", tests_run);
  return 0;
}
