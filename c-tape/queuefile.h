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


/*
 * Port of Tape project from Java. https://github.com/square/tape
 *
 * Integers are forced to be 32-bit, maximum file size supported is 2^32 (4GB).
 *
 * Original description:
 *
 * A reliable, efficient, file-based, FIFO queue. Additions and removals are
 * O(1). All operations are atomic. Writes are synchronous; data will be written
 * to disk before an operation returns. The underlying file is structured to
 * survive process and even system crashes. If an I/O exception is thrown during
 * a mutating change, the change is aborted. It is safe to continue to use a
 * {@code QueueFile} instance after an exception.
 *
 * All operations are synchronized. In a traditional queue, the remove
 * operation returns an element. In this queue, {@link #peek} and {@link
 * #remove} are used in conjunction. Use {@code peek} to retrieve the first
 * element, and then {@code remove} to remove it after successful processing. If
 * the system crashes after {@code peek} and during processing, the element will
 * remain in the queue, to be processed when the system restarts.
 *
 * NOTE: The current implementation is built
 * for file systems that support atomic segment writes (like YAFFS). Most
 * conventional file systems don't support this; if the power goes out while
 * writing a segment, the segment will contain garbage and the file will be
 * corrupt. We'll add journaling support so this class can be used with more
 * file systems later.
 *
 */


#ifndef QUEUEFILE_H_
#define QUEUEFILE_H_

#include"types.h"

struct _QueueFile;
typedef struct _QueueFile QueueFile;

/** 
 * Create new queuefile.
 * @param filename
 * @return new queuefile or NULL on error. 
 */
QueueFile* QueueFile_new(char* filename);

/** 
 * Closes the underlying file and frees all memory including
 * the pointer passed.
 * @param qf queuefile
 * @return false if an error occurred
 */
bool QueueFile_closeAndFree(QueueFile* qf);

/**
 * Adds an element to the end of the queue.
 * @param qf queuefile
 * @param data to copy bytes from
 * @param offset to start from in buffer
 * @param count number of bytes to copy
 * @return false if an error occurred
 */
bool QueueFile_add(QueueFile* qf, const byte* data, uint32_t offset,
                   uint32_t count);

/** 
 * Reads the eldest element. Returns null if the queue is empty.
 * @param qf queuefile
 * @param returnedLength contains the size of the returned buffer.
 * @return element buffer (null if queue is empty) CALLER MUST FREE THIS
 */
byte* QueueFile_peek(QueueFile* qf, uint32_t* returnedLength);


struct _QueueFile_ElementStream;
typedef struct _QueueFile_ElementStream QueueFile_ElementStream;

/**
 * Read data from an element stream.
 * @param stream pointer to element stream.
 * @param buffer  to copy bytes to.
 * @param length  size of buffer.
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

/**
 * Reads the next byte.
 * @param stream pointer to element stream.
 * @return as int, or -1 if the element has ended, or on error.
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
 * @param stream pointer to element stream.
 * @param remaining number of bytes in element.
 * @return false to stop the iteration.
 */
typedef bool (*QueueFile_ElementReaderFunc)(QueueFile_ElementStream* stream,
                                            uint32_t remaining);

/**
 * Invokes the given reader once for the first element in the queue.
 * There will be no callback if the queue is empty.
 * @param qf queuefile.
 * @param reader function pointer for callback.
 * @return false if an error occurred.
 */
bool QueueFile_peekWithElementReader(QueueFile* qf,
                                     QueueFile_ElementReaderFunc reader);

/**
 * Invokes the given reader once for each element in the queue, from eldest to
 * most recently added. Note that this is under lock.
 * There will be no callback if the queue is empty.
 * @param qf queuefile.
 * @param reader function pointer for callback.
 * @return false if an error occurred.
 */
bool QueueFile_forEach(QueueFile* qf, QueueFile_ElementReaderFunc reader);

/** Returns true if there are no entries or NULL passed. */
bool QueueFile_isEmpty(QueueFile* qf);

/** 
 * Clears this queue. Truncates the file to the initial size.
 * @param qf queuefile.
 * @param reader function pointer for callback.
 * @return false if an error occurred.
 */
bool QueueFile_clear(QueueFile* qf);

/** 
 * @param qf queuefile.
 * @return the number of elements in this queue, or 0 if NULL is passed. 
 */
uint32_t QueueFile_size(QueueFile* qf);

/**
 * Removes the eldest element.
 * @param qf queuefile.
 * @return false if empty or NULL passed.
 */
bool QueueFile_remove(QueueFile* qf);


FILE* _for_testing_QueueFile_getFhandle(QueueFile* qf);

#endif //queuefile_h
