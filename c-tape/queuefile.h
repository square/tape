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

#ifndef queuefile_h
#define queuefile_h

#include"types.h"

struct _QueueFile;
typedef struct _QueueFile QueueFile;

// returns NULL on error.
QueueFile* QueueFile_new(char *filename);

/**
 * Adds an element to the end of the queue.
 *
 * @param data   to copy bytes from
 * @param offset to start from in buffer
 * @param count  number of bytes to copy
 * @returns true if successful
 */
bool QueueFile_add(QueueFile *qf, const byte* data, uint32_t offset,
    uint32_t count);

/** Reads the eldest element. Returns null if the queue is empty.
 * @param returnedLength contains the size of the returned buffer.
 * CALLER MUST FREE THE RETURNED MEMORY */
byte* QueueFile_peek(QueueFile* qf, uint32_t *returnedLength);

/** Returns true if there are no entries or NULL passed. */
bool QueueFile_isEmpty(QueueFile* qf);

/** Clears this queue. Truncates the file to the initial size. */
bool QueueFile_clear(QueueFile* qf);

/** Closes the underlying file. */
bool QueueFile_close(QueueFile* qf);

/** Returns the number of elements in this queue, or 0 if NULL is passed. */
uint32_t QueueFile_size(QueueFile* qf);

/**
 * Removes the eldest element.
 * @return false if empty or NULL passed.
 */
bool QueueFile_remove(QueueFile* qf);


FILE* _for_testing_QueueFile_get_fhandle(QueueFile *qf);

#endif //queuefile_h
