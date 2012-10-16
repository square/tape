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
 * @returns false if an error occurred
 */
bool QueueFile_add(QueueFile *qf, const byte* data, uint32_t offset,
    uint32_t count);

/** Reads the eldest element. Returns null if the queue is empty.
 * @param returnedLength contains the size of the returned buffer.
 * CALLER MUST FREE THE RETURNED MEMORY */
byte* QueueFile_peek(QueueFile* qf, uint32_t *returnedLength);


struct _QueueFile_ElementStream;
typedef struct _QueueFile_ElementStream QueueFile_ElementStream;

/**
 * Read data from an element stream.
 * @param stream pointer to element stream
 * @param buffer  to copy bytes to
 * @param length  size of buffer
 * @param lengthRemaining if not null, will be set to number of bytes left.
 * @return false if an error occurred.
 *
 * *********************************************************
 * WARNING! MUST ONLY BE USED INSIDE A CALLBACK FROM FOREACH
 * as this ensures the queuefile is under mutex lock.
 * the validity of stream is only guaranteed under this callback.
 * *********************************************************
 */
bool QueueFile_readElementStream(QueueFile_ElementStream* stream, byte* buffer,
    uint32_t length, uint32_t* lengthRemaining);

/* Reads the next byte, returns as int, or -1 if the element has ended, or there
 * was an error.
 *
 * *********************************************************
 * WARNING! MUST ONLY BE USED INSIDE A CALLBACK FROM FOREACH
 * as this ensures the queuefile is under mutex lock.
 * the validity of stream is only guaranteed under this callback.
 * *********************************************************
 */
int QueueFile_readElementStreamNextByte(QueueFile_ElementStream* stream);

/**
 * Function which is called by forEach or peekWithElementReader for each element.
 * @return false to stop the iteration.
 */
typedef bool (*QueueFile_ElementReader)(QueueFile_ElementStream* stream,
    uint32_t length);


/**
 * Invokes the given reader once for the first element in the queue.
 * There will be no callback if the queue is empty.
 * @return false if an error occurred.
 */
bool QueueFile_peekWithElementReader(QueueFile* qf,
    QueueFile_ElementReader reader);


/**
 * Invokes the given reader once for each element in the queue, from eldest to
 * most recently added. Note that this is under lock.
 * There will be no callback if the queue is empty.
 * @return false if an error occurred.
 */
bool QueueFile_forEach(QueueFile* qf, QueueFile_ElementReader reader);

/** Returns true if there are no entries or NULL passed. */
bool QueueFile_isEmpty(QueueFile* qf);

/** Clears this queue. Truncates the file to the initial size.
 * @return false if an error occurred.
 */
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


FILE* _for_testing_QueueFile_getFhandle(QueueFile *qf);

#endif //queuefile_h
