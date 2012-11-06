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


#include <jni.h>

#include "queuefilenative.h"

/**
 * This file implements the methdods for queuefilenative.h (which was 
 * generated using javah).
 *
 * This code as a shim to call the QueueFile_XX methods in the C library.
 *
 * TODO(jochen): run tests with -Xcheck:jni on, maybe also -verbose:jni.
 *               once we have an android app, also do the following:
 *               In the AndroidManifest.xml file, add android:debuggable="true"
 *               to the <application> element.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
  
#include "queuefile.h"
#include "logutil.h"
#include "jniutils.h"

// cached IDs for Java fields.
static jfieldID nativeObjId = NULL;
static jmethodID callbackFuncId = NULL;

JNIEXPORT void JNICALL Java_com_squareup_tape_QueueFileNative_initIDs(
    JNIEnv *env, jclass cls) {
  nativeObjId = (*env)->GetFieldID(env, cls, "nativeObj", "Ljava/nio/ByteBuffer;");
  if ((*env)->ExceptionCheck(env)) nativeObjId = NULL;
}

JNIEXPORT void JNICALL
    Java_com_squareup_tape_QueueFileNative_00024NativeCallback_initIDs(
    JNIEnv *env, jclass cls) {
  callbackFuncId = (*env)->GetMethodID(env, cls, "callback",
                                       "(Ljava/nio/ByteBuffer;I)V");
  if ((*env)->ExceptionCheck(env)) callbackFuncId = NULL;
}
  
static jobject wrapCPointer(JNIEnv *env, void* p) {
  // Allocate DBB of size 0: we don't want anyone messing with it.
  return (*env)->NewDirectByteBuffer(env, p, 0);
}
  
static void* unwrapCPointer(JNIEnv *env, jobject directBuffer) {
  void* p = (*env)->GetDirectBufferAddress(env, directBuffer);
  if ((*env)->ExceptionCheck(env)) return NULL;
  if (p == NULL) {
    LOG(LWARN, "Bad native object");
    throwIoException(env, "bad native object: %p", p);
  }
  return p;
}

static void* getCPointer(JNIEnv *env, jobject thiz, jfieldID fieldId) {
  jobject directBuffer = (*env)->GetObjectField(env, thiz, fieldId);
  if ((*env)->ExceptionCheck(env)) return NULL;
  if (directBuffer == NULL) {
    LOG(LWARN, "Bad native object");
    throwIoException(env, "bad native queuefile object.");
    return NULL;
  }
  return unwrapCPointer(env, directBuffer);
}
  

JNIEXPORT jobject JNICALL Java_com_squareup_tape_QueueFileNative_nativeNew
    (JNIEnv *env, jobject thisz, jstring filename) {
  const char* fname = (*env)->GetStringUTFChars(env, filename, NULL);
  if ((*env)->ExceptionCheck(env)) return NULL;
      
  QueueFile *qf = QueueFile_new(fname);
  jobject wrappedPointer = NULL;
  if (qf == NULL) {
    throwIoException(env, "Could not create queuefile: %s", fname);
  } else {
    wrappedPointer = wrapCPointer(env, qf);
  }
  (*env)->ReleaseStringUTFChars(env, filename, fname);
  return wrappedPointer;
}

JNIEXPORT jint JNICALL Java_com_squareup_tape_QueueFileNative_getFileLength(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz, nativeObjId);
  if (qf == NULL) return;
  return QueueFile_getFileLength(qf);
}
  
JNIEXPORT void JNICALL Java_com_squareup_tape_QueueFileNative_addUnchecked(
    JNIEnv *env, jobject thiz, jbyteArray data, jint offset, jint length) {
  QueueFile* qf = getCPointer(env, thiz, nativeObjId);
  if (qf == NULL) return;
  jboolean isCopy;
  jbyte* buffer = (*env)->GetByteArrayElements(env, data, &isCopy);
  if ((*env)->ExceptionCheck(env)) return;

  bool success = QueueFile_add(qf, (const byte *) buffer,
                               (uint32_t) offset, (uint32_t) length);
  
  // release and don't copy back if it's a copy i.e. JNI_ABORT.
  (*env)->ReleaseByteArrayElements(env, data, buffer, JNI_ABORT);
  if ((*env)->ExceptionCheck(env)) return;
  if (!success) throwIoException(env, "Problem adding item to queue.");
}
  
JNIEXPORT jboolean JNICALL Java_com_squareup_tape_QueueFileNative_isEmpty(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz, nativeObjId);
  if (qf == NULL) return;
  return (jboolean)QueueFile_isEmpty(qf);
}

JNIEXPORT jbyteArray JNICALL Java_com_squareup_tape_QueueFileNative_peek__(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz, nativeObjId);
  if (qf == NULL) return;
  uint32_t returnedLength;
  byte* data = QueueFile_peek(qf, &returnedLength);
  if (data == NULL) {
    throwIoException(env, "Could not peek on queue.");
    return NULL;
  }

  // NOTE(jochen): we don't know the item length beforehand (and getting
  //               length involves disk i/o, so it's slow!), so we have to do
  //               the additional memcpy here, direct buffers aren't freed in
  //               the JVM, so can't use DirectByteBuffer with data.

  jbyteArray newData = (*env)->NewByteArray(env, returnedLength);
  if (!(*env)->ExceptionCheck(env) && newData != NULL) {
    (*env)->SetByteArrayRegion(env, newData, 0, returnedLength, (jbyte*) data);
  }
  free(data);
  return newData;
}



// Main entry point for peek with reader.
JNIEXPORT void JNICALL
    Java_com_squareup_tape_QueueFileNative_nativePeekWithReader(
    JNIEnv *env, jobject thiz, jobject nativeCallbackObj) {
  
  QueueFile* qf = getCPointer(env, thiz, nativeObjId);
  if (qf == NULL) return;
  if ((*env)->ExceptionCheck(env)) return;

  // Inner function: env and nativeCallbackObj are curried.
  bool peekReader(QueueFile_ElementStream* stream, uint32_t length) {
    jobject streamHandle = wrapCPointer(env, stream);
    if ((*env)->ExceptionCheck(env) || streamHandle == NULL) return false;
    jclass cls = (*env)->GetObjectClass(env, nativeCallbackObj);
    (*env)->CallVoidMethod(env, nativeCallbackObj, callbackFuncId,
                             streamHandle, (jint) length);
    if ((*env)->ExceptionCheck(env)) return false;
    return true;
  }
  QueueFile_peekWithElementReader(qf, peekReader);
}

// return -1 if at end of stream, else number of bytes actually read.
JNIEXPORT jint JNICALL
    Java_com_squareup_tape_QueueFileNative_nativeReadElementStream(
    JNIEnv *env, jclass cls, jobject streamHandle, jbyteArray data,
    jint offset, jint length) {
      
  if (streamHandle == NULL || data == NULL) {
    throwIoException(env, "Null pointer passed.");
    return -1;
  }
  QueueFile_ElementStream* stream = unwrapCPointer(env, streamHandle);
  if ((*env)->ExceptionCheck(env) || stream == NULL) return -1;

  jboolean isCopy;
  jbyte* buffer = (*env)->GetByteArrayElements(env, data, &isCopy);
  if ((*env)->ExceptionCheck(env)) return -1;
  
  uint32_t lengthRemaining = -1;
  int64_t bytesRead = QueueFile_readElementStream(stream,
                                                  ((byte*) buffer) + offset,
                                                  length, &lengthRemaining);
  jint retval = -1;
  if (bytesRead == -1) {
    throwIoException(env, "error reading from stream.");
  } else if (bytesRead > 0) {
    retval = (jint) bytesRead;
  } else if (lengthRemaining > 0) {
    // weird error condition, or length was 0!
    throwIoException(env, "error reading from stream or unexpected length? asked to read %d", length);
  }
  (*env)->ReleaseByteArrayElements(env, data, buffer, retval > 0 ? JNI_COMMIT :
                                   JNI_ABORT);
  if ((*env)->ExceptionCheck(env)) return -1;
  return retval;
}

JNIEXPORT jint JNICALL
    Java_com_squareup_tape_QueueFileNative_nativeReadElementStreamNextByte(
    JNIEnv *env, jclass cls, jobject streamHandle) {

  if (streamHandle == NULL) {
    throwIoException(env, "Null pointer passed.");
    return -1;
  }
  QueueFile_ElementStream* stream = unwrapCPointer(env, streamHandle);
  if ((*env)->ExceptionCheck(env) || stream == NULL) return -1;

  return QueueFile_readElementStreamNextByte(stream);
}

JNIEXPORT void JNICALL Java_com_squareup_tape_QueueFileNative_nativeForEach(
    JNIEnv *env, jobject thiz, jobject nativeCallbackObj) {
  
  QueueFile* qf = getCPointer(env, thiz, nativeObjId);
  if (qf == NULL) return;
  if ((*env)->ExceptionCheck(env)) return;
  
  // Inner function: env and nativeCallbackObj are curried.
  bool forEachReader(QueueFile_ElementStream* stream, uint32_t length) {
    jobject streamHandle = wrapCPointer(env, stream);
    if ((*env)->ExceptionCheck(env) || streamHandle == NULL) return false;
    jclass cls = (*env)->GetObjectClass(env, nativeCallbackObj);
    (*env)->CallVoidMethod(env, nativeCallbackObj, callbackFuncId,
                           streamHandle, (jint) length);
    if ((*env)->ExceptionCheck(env)) return false;
    return true;
  }
  QueueFile_forEach(qf, forEachReader);
}

JNIEXPORT jint JNICALL Java_com_squareup_tape_QueueFileNative_size(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz, nativeObjId);
  return (jint)QueueFile_size(qf);
}

JNIEXPORT void JNICALL Java_com_squareup_tape_QueueFileNative_remove(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz, nativeObjId);
  if (qf == NULL) return;
  if (!QueueFile_remove(qf)) throwIoException(env, "Error removing item");
}

JNIEXPORT void JNICALL Java_com_squareup_tape_QueueFileNative_clear(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz, nativeObjId);
  if (qf == NULL) return;
  if (!QueueFile_clear(qf)) throwIoException(env, "Error clearing queue.");
}

JNIEXPORT void JNICALL Java_com_squareup_tape_QueueFileNative_close(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz, nativeObjId);
  if (qf == NULL) return;
  if (!QueueFile_closeAndFree(qf)) throwIoException(env, "Error closing queue.");
}

JNIEXPORT jstring JNICALL Java_com_squareup_tape_QueueFileNative_toString(
  JNIEnv *env, jobject thiz) {
  // TODO(jochen): implement this.
  char msg[] = "toString() to be implemented";
  return (*env)->NewStringUTF(env, msg);
}

JNIEXPORT jint JNI_OnLoad(JavaVM* vm, void* reserved)
{
  return JNI_VERSION_1_6;
}

#ifdef __cplusplus
}
#endif
