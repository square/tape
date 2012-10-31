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
 * This file implements the methdods for queuefilenative.h (which was generated using javah).
 *
 * This acts as a shim to call the QueueFile_XX methods in the C library.
 *
 * TODO(jochen): ####jochen run tests with -Xcheck:jni on, maybe also -verbose:jni.
 * once we have an android app, also do the following:
 *    In the AndroidManifest.xml file, add android:debuggable="true" to the <application> element.
 */



#ifdef __cplusplus
extern "C" {
#endif

#include "queuefile.h"
#include "logutil.h"
#include "jniutils.h"

// cached ID for nativeObj field.
static jfieldID nativeObjId;

JNIEXPORT void JNICALL Java_com_squareup_tape_QueueFileNative_initIDs(
    JNIEnv *env, jclass cls) {
  nativeObjId = (*env)->GetFieldID(env, cls, "nativeObj", "Ljava/nio/ByteBuffer;");
}

static QueueFile* getCPointer(JNIEnv *env, jobject thiz) {
  jobject directBuffer = (*env)->GetObjectField(env, thiz, nativeObjId);
  if (directBuffer == NULL) {
    LOG(LWARN, "Bad native object");
    throwIoException(env, "bad native queuefile object.");
    return NULL;
  }
  QueueFile* qf = (QueueFile*) (*env)->GetDirectBufferAddress(env, directBuffer);
  if (qf == NULL) {
    LOG(LWARN, "Bad native object");
    throwIoException(env, "bad native object: %p", qf);
  }
  return qf;
}

static jobject wrapCPointer(JNIEnv *env, QueueFile* p) {
  // Allocate DBB of size 0 since we don't want anyone messing with it.
  return (*env)->NewDirectByteBuffer(env, (void*) p, 0);
}

  
JNIEXPORT jobject JNICALL Java_com_squareup_tape_QueueFileNative_nativeNew
    (JNIEnv *env, jobject thisz, jstring filename) {
  const char* fname = (*env)->GetStringUTFChars(env, filename, NULL);
  QueueFile *qf = QueueFile_new(fname);
  jobject wrappedPointer = NULL;
  if (qf == NULL) {
    throwIoException(env, "Could not create queuefile: %s", fname);
  } else {
    wrappedPointer = wrapCPointer(env, qf);
  }
  (*env)->ReleaseStringUTFChars(env, filename, fname);
  return wrappedPointer;
};

JNIEXPORT jint JNICALL Java_com_squareup_tape_QueueFileNative_getFileLength(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz);
  if (qf == NULL) return;
  return QueueFile_getFileLength(qf);
}
  
JNIEXPORT void JNICALL Java_com_squareup_tape_QueueFileNative_addUnchecked(
    JNIEnv *env, jobject thiz, jbyteArray data, jint offset, jint length) {
  QueueFile* qf = getCPointer(env, thiz);
  if (qf == NULL) return;
  //####jochen
}
  
JNIEXPORT jboolean JNICALL Java_com_squareup_tape_QueueFileNative_isEmpty(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz);
  if (qf == NULL) return;
  return (jboolean)QueueFile_isEmpty(qf);
};

JNIEXPORT jint JNICALL Java_com_squareup_tape_QueueFileNative_size(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz);
  return (jint)QueueFile_size(qf);
};

JNIEXPORT void JNICALL Java_com_squareup_tape_QueueFileNative_remove(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz);
  if (qf == NULL) return;
  if (!QueueFile_remove(qf)) throwIoException(env, "Error removing item");
};

JNIEXPORT void JNICALL Java_com_squareup_tape_QueueFileNative_clear(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz);
  if (qf == NULL) return;
  if (!QueueFile_clear(qf)) throwIoException(env, "Error clearing queue.");
};

JNIEXPORT void JNICALL Java_com_squareup_tape_QueueFileNative_close(
    JNIEnv *env, jobject thiz) {
  QueueFile* qf = getCPointer(env, thiz);
  if (qf == NULL) return;
  if (!QueueFile_closeAndFree(qf)) throwIoException(env, "Error closing queue.");
};

JNIEXPORT jstring JNICALL Java_com_squareup_tape_QueueFileNative_toString(
  JNIEnv *env, jobject thiz) {
  // TODO(jochen): implement this.
  char msg[] = "toString() to be implemented";
  return (*env)->NewStringUTF(env, msg);
};

JNIEXPORT jint JNI_OnLoad(JavaVM* vm, void* reserved)
{
  return JNI_VERSION_1_6;
}

#ifdef __cplusplus
}
#endif
