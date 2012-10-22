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

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fileio.h"
#include "logutil.h"
#include "queuefile.h"

/*
 * Port of Tape project from Java. https://github.com/square/tape
 *
 * Original description:
 *
 * A reliable, efficient, file-based, FIFO queue. Additions and removals are
 * O(1). All operations are atomic. Writes are synchronous; data will be written
 * to disk before an operation returns. The underlying file is structured to
 * survive process and even system crashes. If an I/O exception is thrown during
 * a mutating change, the change is aborted. It is safe to continue to use a
 * {@code QueueFile} instance after an exception.
 * <p/>
 * <p>All operations are synchronized. In a traditional queue, the remove
 * operation returns an element. In this queue, {@link #peek} and {@link
 * #remove} are used in conjunction. Use {@code peek} to retrieve the first
 * element, and then {@code remove} to remove it after successful processing. If
 * the system crashes after {@code peek} and during processing, the element will
 * remain in the queue, to be processed when the system restarts.
 * <p/>
 * <p><strong>NOTE:</strong> The current implementation is built
 * for file systems that support atomic segment writes (like YAFFS). Most
 * conventional file systems don't support this; if the power goes out while
 * writing a segment, the segment will contain garbage and the file will be
 * corrupt. We'll add journaling support so this class can be used with more
 * file systems later.
 *
 * @author Bob Lee (bob@squareup.com)
 */


/*
 * Integers are forced to be 32-bit, maximum file size supported is 2^32 (4GB).
 */

/** frees if oldPointer is not NULL, assigns newPointer. */
#define freeAndAssign(OLD, NEW) _freeAndAssign((void**)(OLD), (void*)(NEW))
static bool _freeAndAssign(void **oldPointer, void* newPointer);

/** frees and assigns oldPointer IFF newPointer is not NULL. */
#define freeAndAssignNonNull(OLD, NEW) _freeAndAssignNonNull((void**)(OLD), (void*)(NEW))
static bool _freeAndAssignNonNull(void **oldPointer, void* newPointer);

// Use macro to maintain line number
#define NULLARG(P) ((P) == NULL ? LOG(LWARN, "Null argument passed") || 1 : 0)
#define CHECKOOM(P) ((P) == NULL ? LOG(LWARN, "Out of memory") || 1 : 0)

// For sanity tests
#define MAX_FILENAME_LEN 4096

// ------------------------------ Element -----------------------------------


#define Element_HEADER_LENGTH 4

/** A pointer to an element. */
typedef struct {
  
  /** Position in file. */
  uint32_t position;
  
  /** The length of the data. */
  uint32_t length;
  
} Element;

/**
 * Constructs a new element.
 *
 * @param position within file
 * @param length   of data
 */
Element* Element_new(uint32_t position, uint32_t length) {
  Element* e = malloc(sizeof(Element));
  if (CHECKOOM(e)) return NULL;
  e->position = position;
  e->length = length;
  return e;
}

void Element_fprintf(Element* e, FILE *fout) {
  fprintf(fout, "Element:[position = %d, length = %d]",
          e->position, e->length);
}


// ------------------------------ QueueFile -----------------------------------


/** Initial file size in bytes. */
#define QueueFile_INITIAL_LENGTH 4096 // one file system block

/** Length of header in bytes. */
#define QueueFile_HEADER_LENGTH 16 // May not be shorter than 16 bytes.

struct _QueueFile {
  
  /**
   * The underlying file. Uses a ring buffer to store entries. Designed so that
   * a modification isn't committed or visible until we write the header. The
   * header is much smaller than a segment. So long as the underlying file
   * system supports atomic segment writes, changes to the queue are atomic.
   * Storing the file length ensures we can recover from a failed expansion
   * (i.e. if setting the file length succeeds but the process dies before the
   * data can be copied).
   *
   *   Format:
   *     Header              (16 bytes)
   *     Element Ring Buffer (File Length - 16 bytes)
   *
   *   Header:
   *     File Length            (4 bytes)
   *     Element Count          (4 bytes)
   *     First Element Position (4 bytes, =0 if null)
   *     Last Element Position  (4 bytes, =0 if null)
   *
   *   Element:
   *     Length (4 bytes)
   *     Data   (Length bytes)
   */
  FILE* file;
  
  /** Cached file length. Always a power of 2. */
  uint32_t fileLength;
  
  /** Number of elements. */
  uint32_t elementCount;
  
  /** Pointer to first (or eldest) element. */
  Element* first;
  
  /** Pointer to last (or newest) element. */
  Element* last;
  
  /** In-memory buffer. Big enough to hold the header. */
  byte buffer[QueueFile_HEADER_LENGTH];

  /** mutex to synchronize method access */
  pthread_mutex_t mutex;
};

static bool initialize(char* filename);
static bool QueueFile_readHeader(QueueFile* qf);


// returns NULL on error.
QueueFile* QueueFile_new(char *filename) {
  if (NULLARG(filename)) return NULL;
  QueueFile* qf = malloc(sizeof(QueueFile));
  if (CHECKOOM(qf)) return NULL;
  memset(qf, 0, sizeof(QueueFile)); // making sure pointers & counters are null!
  
  qf->file = fopen(filename, "r+");
  if (qf->file == NULL) {
    if (initialize(filename)) {
      qf->file = fopen(filename, "r+");
    }
    if (qf->file == NULL) {
      free(qf);
      return NULL;
    }
  }
  if (!QueueFile_readHeader(qf)) {
    fclose(qf->file);
    free(qf);
    return NULL;
  }

  // TODO(jochen): consider NP mutex options, audit code for re-entrancy and
  //    eliminate recursive option if needed.
  // NOTE: there was a problem with static initializers on OSX
  //    http://gcc.gnu.org/bugzilla/show_bug.cgi?id=51906
  pthread_mutexattr_t mta;
  pthread_mutexattr_init(&mta);
  pthread_mutexattr_settype(&mta, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init (&qf->mutex, &mta);

  return qf;
}

bool QueueFile_closeAndFree(QueueFile *qf) {
  pthread_mutex_lock(&qf->mutex);
  bool success = !fclose(qf->file);
  if (success) {
    if (qf != NULL) {
      if (qf->first != NULL) {
        free(qf->first);
      }
      if (qf->last != NULL && qf->last != qf->first) free(qf->last);
      qf->first = qf->last = NULL;
    }
  }
  pthread_mutex_unlock(&qf->mutex);

  if(success)
    free(qf);

  return success;
}

/**
 * Stores int in buffer.
 */
static void writeInt(byte* buffer, uint32_t offset, uint32_t value) {
  buffer[offset] = (byte) (value >> 24);
  buffer[offset + 1] = (byte) (value >> 16);
  buffer[offset + 2] = (byte) (value >> 8);
  buffer[offset + 3] = (byte) value;
}

/**
 * Stores int values in buffer.
 */
static void writeInts(byte* buffer, uint32_t v1, uint32_t v2, uint32_t v3,
                      uint32_t v4) {
  writeInt(buffer, 0, v1);
  writeInt(buffer, 4, v2);
  writeInt(buffer, 8, v3);
  writeInt(buffer, 12, v4);
}

/** Reads an int from a byte[]. */
static uint32_t readInt(byte* buffer, uint32_t offset) {
  return ((buffer[offset] & 0xff) << 24)
    + ((buffer[offset + 1] & 0xff) << 16)
    + ((buffer[offset + 2] & 0xff) << 8)
    + (buffer[offset + 3] & 0xff);
}



static Element* QueueFile_readElement(QueueFile* qf, uint32_t position);

/** Reads the header. */
static bool QueueFile_readHeader(QueueFile* qf) {
  if (!FileIo_seek(qf->file, 0) ||
      !FileIo_read(qf->file, qf->buffer, 0, (uint32_t) sizeof(qf->buffer))) {
    return false;
  }

  qf->fileLength = readInt(qf->buffer, 0);
  uint32_t actualLength = (uint32_t) FileIo_getLength(qf->file);
  if (qf->fileLength > actualLength) {
    LOG(LWARN, "File is truncated. Expected length: %d, Actual length: %d",
        qf->fileLength,  actualLength);
    return false;
  }

  qf->elementCount = readInt(qf->buffer, 4);
  uint32_t firstOffset = readInt(qf->buffer, 8);
  uint32_t lastOffset = readInt(qf->buffer, 12);
  return freeAndAssign(&qf->first, QueueFile_readElement(qf, firstOffset)) &&
    freeAndAssign(&qf->last, QueueFile_readElement(qf, lastOffset));
}

/**
 * Writes header atomically. The arguments contain the updated values. The
 * class member fields should not have changed yet. This only updates the
 * state in the file. It's up to the caller to update the class member
 * variables *after* this call succeeds. Assumes segment writes are atomic in
 * the underlying file system.
 */
static bool QueueFile_writeHeader(QueueFile* qf, uint32_t fileLength,
                                  uint32_t elementCount, uint32_t firstPosition,
                                  uint32_t lastPosition) {
  writeInts(qf->buffer, fileLength, elementCount, firstPosition, lastPosition);
  return FileIo_seek(qf->file, 0) &&
           FileIo_write(qf->file, qf->buffer, 0, QueueFile_HEADER_LENGTH);
}


/** 
 * Returns the Element for the given offset. CALLER MUST FREE MEMORY.
 */
static Element* QueueFile_readElement(QueueFile* qf, uint32_t position) {
  if (position == 0 ||
      !FileIo_seek(qf->file, position) ||
      !FileIo_read(qf->file, qf->buffer, 0, (uint32_t) sizeof(uint32_t))) {
    return NULL;
  }
  uint32_t length = readInt(qf->buffer, 0);
  return Element_new(position, length);
}


/** Make a temporary string, caller must free result. */
char *makeTempName(const char* filename, int maxLen);

/** Atomically initializes a new file. */
static bool initialize(char* filename) {
  if (QueueFile_HEADER_LENGTH < 16) {
    LOG(LFATAL, "Configuration error, header length must be >= 16 bytes");
    return false;
  }

  char *tempname = makeTempName(filename, MAX_FILENAME_LEN);
  if (tempname == NULL) {
    LOG(LWARN, "Filename too long or out of memory: %s", filename);
    return false;
  }

  FILE* tempfile = fopen(tempname, "w+");
  if (tempfile == NULL) {
    free(tempname);
    return false;
  }
  
  bool success = false;
  // TODO(jochen): if truncate in setLength does not work for target platform, consider
  //  appending 0s using FileIo_writeZeros.
  if (FileIo_setLength(tempfile, QueueFile_INITIAL_LENGTH)) {
    byte headerBuffer[QueueFile_HEADER_LENGTH];
      writeInts(headerBuffer, QueueFile_INITIAL_LENGTH, 0, 0, 0);
      success = FileIo_write(tempfile, headerBuffer, 0, QueueFile_HEADER_LENGTH);
  }

  fclose(tempfile);
  success = success && rename(tempname, filename) == 0;
  if (!success) {
    LOG(LFATAL, "Error initializing temporary file %s", tempname);
    remove(tempname);
  }
  free(tempname);
  return success;
}

/** Wraps the position if it exceeds the end of the file. */
static uint32_t QueueFile_wrapPosition(const QueueFile* qf, uint32_t position) {
  return position < qf->fileLength ?
          position : QueueFile_HEADER_LENGTH + position - qf->fileLength;
}

/**
 * Writes count bytes from buffer to position in file. Automatically wraps
 * write if position is past the end of the file or if buffer overlaps it.
 *
 * @param position in file to write to
 * @param buffer   to write from
 * @param count    # of bytes to write
 */
static bool QueueFile_ringWrite(QueueFile* qf, uint32_t position,
    const byte* buffer,
    uint32_t offset, uint32_t count) {
  bool success = false;
  position = QueueFile_wrapPosition(qf, position);
  if (position + count <= qf->fileLength) {
    success = FileIo_seek(qf->file, position) &&
                FileIo_write(qf->file, buffer, offset, count);
  } else {
    // The write overlaps the EOF.
    // # of bytes to write before the EOF.
    uint32_t beforeEof = qf->fileLength - position;
    success = FileIo_seek(qf->file, position) &&
                FileIo_write(qf->file, buffer, offset, beforeEof) &&
                FileIo_seek(qf->file, QueueFile_HEADER_LENGTH) &&
                FileIo_write(qf->file, buffer, offset + beforeEof,
                    count - beforeEof);
  }
  return success;
}

/**
 * Reads count bytes into buffer from file. Wraps if necessary.
 *
 * @param position in file to read from
 * @param buffer   to read into
 * @param count    # of bytes to read
 */
static bool QueueFile_ringRead(QueueFile *qf, uint32_t position, byte* buffer,
    uint32_t offset, uint32_t count) {
  bool success = false;
  position = QueueFile_wrapPosition(qf, position);
  if (position + count <= qf->fileLength) {
    success = FileIo_seek(qf->file, position) &&
                FileIo_read(qf->file, buffer, 0, count);
  } else {
    // The read overlaps the EOF.
    // # of bytes to read before the EOF.
    uint32_t beforeEof = qf->fileLength - position;

    success = FileIo_seek(qf->file, position) &&
                FileIo_read(qf->file, buffer, offset, beforeEof) &&
                FileIo_seek(qf->file, QueueFile_HEADER_LENGTH) &&
                FileIo_read(qf->file, buffer, offset + beforeEof, count -
                    beforeEof);
  }
  return success;
}

/** Returns true if there are no entries or NULL passed. */
bool QueueFile_isEmpty(QueueFile* qf) {
  if (NULLARG(qf)) return true;
  pthread_mutex_lock(&qf->mutex);
  uint32_t elementCount = qf->elementCount == 0;
  pthread_mutex_unlock(&qf->mutex);
  return elementCount;
}

static bool QueueFile_expandIfNecessary(QueueFile* qf, uint32_t dataLength);

/**
 * Adds an element to the end of the queue.
 *
 * @param data   to copy bytes from
 * @param offset to start from in buffer
 * @param count  number of bytes to copy
 */
bool QueueFile_add(QueueFile* qf, const byte* data, uint32_t offset,
    uint32_t count) {
  if (NULLARG(qf) || NULLARG(data)) return false;

  bool success = false;
  pthread_mutex_lock(&qf->mutex);

  if(QueueFile_expandIfNecessary(qf, count)) {
    // Insert a new element after the current last element.
    bool wasEmpty = QueueFile_isEmpty(qf);
    uint32_t position = wasEmpty ? QueueFile_HEADER_LENGTH :
        QueueFile_wrapPosition(qf, qf->last->position + Element_HEADER_LENGTH +
            qf->last->length);
    Element* newLast = Element_new(position, count);
    // Write length & data.
    writeInt(qf->buffer, 0, count);
    if (newLast != NULL) {
      if (QueueFile_ringWrite(qf, newLast->position, qf->buffer, 0,
            Element_HEADER_LENGTH) &&
        QueueFile_ringWrite(qf, newLast->position + Element_HEADER_LENGTH, data,
            offset, count)) {

        // Commit the addition. If wasEmpty, first == last.
        uint32_t firstPosition = wasEmpty ? newLast->position : qf->first->position;
        success = QueueFile_writeHeader(qf, qf->fileLength, qf->elementCount + 1,
                                        firstPosition, newLast->position);
      }
      if (success) {
        if (freeAndAssignNonNull(&qf->last, newLast)) {
          if (wasEmpty) freeAndAssignNonNull(&qf->first,
                                             Element_new(qf->last->position,
                                                         qf->last->length));
          success = true;
          qf->elementCount++;
        }
      } else {
        free(newLast);
      }
    }
  }

  pthread_mutex_unlock(&qf->mutex);
  return success;
}

/** Returns the number of used bytes. */
static uint32_t QueueFile_usedBytes(QueueFile* qf) {
  if (qf->elementCount == 0) return QueueFile_HEADER_LENGTH;

  if (qf->last->position >= qf->first->position) {
    // Contiguous queue.
    return (qf->last->position - qf->first->position)   // all but last entry
        + Element_HEADER_LENGTH + qf->last->length // last entry
        + QueueFile_HEADER_LENGTH;
  } else {
    // tail < head. The queue wraps.
    return qf->last->position                      // buffer front + header
        + Element_HEADER_LENGTH + qf->last->length // last entry
        + qf->fileLength - qf->first->position;        // buffer end
  }
}

/** Returns number of unused bytes. */
static uint32_t QueueFile_remainingBytes(QueueFile* qf) {
  return qf->fileLength - QueueFile_usedBytes(qf);
}

/**
 * If necessary, expands the file to accommodate an additional element of the
 * given length.
 *
 * @param dataLength length of data being added
 * @returns false only if an error was encountered.
 */
static bool QueueFile_expandIfNecessary(QueueFile* qf, uint32_t dataLength) {
  uint32_t elementLength = Element_HEADER_LENGTH + dataLength;
  uint32_t remainingBytes = QueueFile_remainingBytes(qf);
  if (remainingBytes >= elementLength) {
    return true;
  }

  // Expand.
  uint32_t previousLength = qf->fileLength;
  uint32_t newLength;
  // Double the length until we can fit the new data.
  do {
    remainingBytes += previousLength;
    newLength = previousLength << 1;
    previousLength = newLength;
  } while (remainingBytes < elementLength);

// TODO(jochen): if truncate in setLength does not work for target platform, consider
//  appending 0s using FileIo_writeZeros.
  if (!FileIo_setLength(qf->file, newLength)) return false;

  // Calculate the position of the tail end of the data in the ring buffer
  uint32_t endOfLastElement = QueueFile_wrapPosition(qf, qf->last->position +
      Element_HEADER_LENGTH + qf->last->length);

  // If the buffer is split, we need to make it contiguous, so append the
  // tail of the queue to after the end of the old file.
  if (endOfLastElement < qf->first->position) {
    uint32_t count = endOfLastElement - Element_HEADER_LENGTH;
    if(!FileIo_transferTo(qf->file, QueueFile_HEADER_LENGTH,
        qf->fileLength, count)) return false;
  }

  // Commit the expansion.
  if (qf->last->position < qf->first->position) {
    uint32_t newLastPosition =
        qf->fileLength + qf->last->position - QueueFile_HEADER_LENGTH;
    if (!QueueFile_writeHeader(qf, newLength, qf->elementCount,
        qf->first->position, newLastPosition)) return false;
    if(!freeAndAssignNonNull(&qf->last,
        Element_new(newLastPosition, qf->last->length))) return false;
  } else {
    if(!QueueFile_writeHeader(qf, newLength, qf->elementCount,
        qf->first->position, qf->last->position)) return false;
  }
  qf->fileLength = newLength;
  return true;
}


/** Reads the eldest element. Returns null if the queue is empty.
 * CALLER MUST FREE THE RETURNED MEMORY */
byte* QueueFile_peek(QueueFile* qf, uint32_t *returnedLength) {
  if (NULLARG(qf) || NULLARG(returnedLength) || QueueFile_isEmpty(qf)) return NULL;
  pthread_mutex_lock(&qf->mutex);
  *returnedLength = 0;

  uint32_t length = qf->first->length;
  byte* data = malloc((size_t) length);
  if (CHECKOOM(data)) return NULL;
  if(!QueueFile_ringRead(qf, qf->first->position + Element_HEADER_LENGTH,
      data, 0, length)) {
    free(data);
    data = NULL;
  }
  *returnedLength = length;

  pthread_mutex_unlock(&qf->mutex);
  return data;
}


struct _QueueFile_ElementStream {
  QueueFile *qf;
  uint32_t position;
  uint32_t remaining;
};

/**
 * Read data from an element stream.
 * @param stream pointer to element stream
 * @param buffer  to copy bytes to
 * @param length  size of buffer
 * @param lengthRemaining will be set to number of bytes left.
 * @return false if an error occurred.
 * *********************************************************
 * WARNING! MUST ONLY BE USED INSIDE A CALLBACK FROM FOREACH
 * as this ensures the queuefile is under mutex lock.
 * the validity of stream is only guaranteed under this callback.
 * *********************************************************
 */
bool QueueFile_readElementStream(QueueFile_ElementStream* stream, byte* buffer,
    uint32_t length, uint32_t* lengthRemaining) {
  if (NULLARG(stream) || NULLARG(buffer) || NULLARG(lengthRemaining) ||
      NULLARG(stream->qf)) return false;
  *lengthRemaining = 0;
  if (stream->remaining == 0)
    return true;

  if (length > stream->remaining) length = stream->remaining;
  if (QueueFile_ringRead(stream->qf, stream->position, buffer, 0,
      length)) {
    stream->position = QueueFile_wrapPosition(stream->qf,
        stream->position + length);
    stream->remaining -= length;
    *lengthRemaining = stream->remaining;
  } else {
    return false;
  }
  return true;
}

/* Reads the next byte, returns as int, or -1 if the element has ended, or there
 * was an error.
 *
 * *********************************************************
 * WARNING! MUST ONLY BE USED INSIDE A CALLBACK FROM FOREACH
 * as this ensures the queuefile is under mutex lock.
 * the validity of stream is only guaranteed under this callback.
 * *********************************************************
 */
int QueueFile_readElementStreamNextByte(QueueFile_ElementStream* stream) {
  byte buffer;
  uint32_t remaining;
  if (stream->remaining == 0) {
    return -1;
  }
  if(!QueueFile_readElementStream(stream, &buffer, (uint32_t) sizeof(byte),
      &remaining))
    return -1;
  return (int)buffer;
}

/**
 * Invokes the given reader once for the first element in the queue.
 * There will be no callback if the queue is empty.
 * @return false if an error occurred.
 */
bool QueueFile_peekWithElementReader(QueueFile* qf,
    QueueFile_ElementReader reader) {
  if (NULLARG(reader) || NULLARG(qf)) return false;
  pthread_mutex_lock(&qf->mutex);

  bool success = false;
  if (qf->elementCount == 0) {
    success = true;
  } else {
    if (qf->first == NULL) {
      LOG(LFATAL, "Internal error: queue should have a first element.");
    } else {
      Element* current = QueueFile_readElement(qf, qf->first->position);
      if (current != NULL) {
        QueueFile_ElementStream stream;
        stream.qf = qf;
        stream.position = QueueFile_wrapPosition(qf,
            current->position + Element_HEADER_LENGTH);
        stream.remaining = current->length;
        free(current);
        (*reader)(&stream, stream.remaining);
        success = true;
      }
    }
  }
  
  pthread_mutex_unlock(&qf->mutex);
  return success;
}

/**
 * Invokes the given reader once for each element in the queue, from eldest to
 * most recently added. Note that this is under lock.
 * There will be no callback if the queue is empty.
 * @return false if an error occurred.
 */
bool QueueFile_forEach(QueueFile* qf, QueueFile_ElementReader reader) {
  if (NULLARG(reader) || NULLARG(qf)) return false;
  pthread_mutex_lock(&qf->mutex);

  bool success = false;
  if (qf->elementCount == 0) {
    success = true;
  } else {
    if (qf->first == NULL) {
      LOG(LFATAL, "Internal error: queue should have a first element.");
    } else {
      uint32_t nextReadPosition = qf->first->position;
      uint32_t i;
      bool stopRequested = false;
      success = true;
      for (i = 0; i < qf->elementCount && !stopRequested && success; i++) {
        Element* current = QueueFile_readElement(qf, nextReadPosition);
        if (current != NULL) {
          QueueFile_ElementStream stream;
          stream.qf = qf;
          stream.position = QueueFile_wrapPosition(qf,
              current->position + Element_HEADER_LENGTH);
          stream.remaining = current->length;
          stopRequested = !(*reader)(&stream, stream.remaining);
          nextReadPosition = QueueFile_wrapPosition(qf, current->position +
              Element_HEADER_LENGTH + current->length);
          free(current);
        } else {
          success = false;
        }
      }
    }
  }
  
  pthread_mutex_unlock(&qf->mutex);
  return success;
}

/** Returns the number of elements in this queue, or 0 if NULL is passed. */
uint32_t QueueFile_size(QueueFile* qf) {
  if(NULLARG(qf)) return 0;
  pthread_mutex_lock(&qf->mutex);
  uint32_t elementCount = qf->elementCount;
  pthread_mutex_unlock(&qf->mutex);
  return elementCount;
}

/**
 * Removes the eldest element.
 * @return false if empty or NULL passed.
 */
bool QueueFile_remove(QueueFile* qf) {
  if(NULLARG(qf)) return false;
  pthread_mutex_lock(&qf->mutex);

  bool success = false;
  if (!QueueFile_isEmpty(qf)) {
    if (qf->elementCount == 1) {
      success = QueueFile_clear(qf);
    } else {
      // assert elementCount > 1
      uint32_t newFirstPosition = QueueFile_wrapPosition(qf, qf->first->position +
          Element_HEADER_LENGTH + qf->first->length);
      if(QueueFile_ringRead(qf, newFirstPosition, qf->buffer, 0,
          Element_HEADER_LENGTH)) {
        int length = readInt(qf->buffer, 0);
        if(QueueFile_writeHeader(qf, qf->fileLength, qf->elementCount - 1,
            newFirstPosition, qf->last->position)) {
          if(freeAndAssignNonNull(&qf->first,
              Element_new(newFirstPosition, (uint32_t) length))) {
            qf->elementCount--;
            success = true;
          }
        }
      }
    }
  }

  pthread_mutex_unlock(&qf->mutex);
  return success;
}

/** Clears this queue. Truncates the file to the initial size.
 * @return false if an error occurred.
 */
bool QueueFile_clear(QueueFile* qf) {
  if (NULLARG(qf)) return false;
  bool success = false;
  pthread_mutex_lock(&qf->mutex);

  if(QueueFile_writeHeader(qf, QueueFile_INITIAL_LENGTH, 0, 0, 0)) {
    qf->elementCount = 0;
    if (qf->first != NULL) {
      free(qf->first);
    }
    qf->first = NULL;
    if (qf->last != NULL) {
      free(qf->last);
    }
    qf->last = NULL;
    if (qf->fileLength > QueueFile_INITIAL_LENGTH) {
      if(FileIo_setLength(qf->file, QueueFile_INITIAL_LENGTH)) {
        qf->fileLength = QueueFile_INITIAL_LENGTH;
        success = true;
      }
    } else {
      success = true;
    }
  }

  pthread_mutex_unlock(&qf->mutex);
  return success;
}

// TODO(jochen): bool QueueFile_fprintf(QueueFile *qf);

FILE* _for_testing_QueueFile_getFhandle(QueueFile *qf) {
  return qf->file;
}


// ---------------------------- Utility Functions ------------------------------


/*
 * Make a temporary string, caller must free result.
 */
char *makeTempName(const char* filename, int maxLen) {
  // Use a temp file so we don't leave a partially-initialized file.
  if (filename == NULL || maxLen < 4) {
    return NULL;
  }
  size_t len = strnlen(filename, (size_t) maxLen - 5) + 5;
  char *tempname = malloc(len);
  if (CHECKOOM(tempname)) {
    return NULL;
  }
  strncpy(tempname, filename, len);
  tempname[len-1] = '\0'; // make sure it's terminated if filename was too long.
  strcat(tempname, ".tmp");
  return tempname;
}

static bool _freeAndAssignNonNull(void **oldPointer, void* newPointer) {
  if (newPointer != NULL) {
    if (*oldPointer != NULL) {
      free(*oldPointer);
    }
    *oldPointer = newPointer;
    return true;
  }
  return false;
}

static bool _freeAndAssign(void **oldPointer, void* newPointer) {
  if (*oldPointer != NULL) {
    free(*oldPointer);
  }
  *oldPointer = newPointer;
  return true;
}
