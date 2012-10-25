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
#include "jniutils.h"

QueueFile* getCPointer(JNIEnv *env, jobject directBuffer) {
  return (QueueFile*) (*env)->GetDirectBufferAddress(env, directBuffer);
}

jobject wrapCPointer(JNIEnv *env, QueueFile* p) {
  // Allocate DBB of size 0 since we don't want anyone messing with it.
  return (*env)->NewDirectByteBuffer(env, (void*) p, 0);
}


JNIEXPORT jobject JNICALL Java_com_squareup_tape_native_1_QueueFileNative_nativeNew
  (JNIEnv *env, jobject thisz, jstring filename) {

  const char* fname = (*env)->GetStringUTFChars(env, filename, NULL);
  QueueFile *qf = QueueFile_new(fname);
  (*env)->ReleaseStringUTFChars(env, filename, fname);

  jobject wrappedPointer = 0;
  if (qf == NULL) {
    throwIoException(env, "Could not create queuefile %s", fname);
  } else {
    jobject wrappedPointer = wrapCPointer(env, qf);
  }

  return wrappedPointer;
};

JNIEXPORT jint JNICALL Java_com_squareup_tape_native_QueueFileNative_getFileLength
  (JNIEnv *env, jobject thiz) {
  return 0;
};

JNIEXPORT void JNICALL Java_com_squareup_tape_native_QueueFileNative_add___3B
  (JNIEnv *env, jobject thiz, jbyteArray array) {

};

JNIEXPORT void JNICALL Java_com_squareup_tape_native_QueueFileNative_add___3BII
  (JNIEnv *env, jobject thiz, jbyteArray array, jint offset, jint count) {

};

JNIEXPORT jboolean JNICALL Java_com_squareup_tape_native_QueueFileNative_isEmpty
  (JNIEnv *env, jobject thiz) {
  return false;
};

JNIEXPORT jbyteArray JNICALL Java_com_squareup_tape_native_QueueFileNative_peek__
  (JNIEnv *env, jobject thiz) {
  return NULL;
};

JNIEXPORT void JNICALL Java_com_squareup_tape_native_QueueFileNative_peek__Lcom_squareup_tape_QueueFile_ElementReader_2
  (JNIEnv *env, jobject thiz, jobject reader) {

};

JNIEXPORT void JNICALL Java_com_squareup_tape_native_QueueFileNative_forEach
  (JNIEnv *env, jobject thiz, jobject reader) {

};

JNIEXPORT jint JNICALL Java_com_squareup_tape_native_QueueFileNative_size
  (JNIEnv *env, jobject thiz) {
  return 0;
};

JNIEXPORT void JNICALL Java_com_squareup_tape_native_QueueFileNative_remove
  (JNIEnv *env, jobject thiz) {

};

JNIEXPORT void JNICALL Java_com_squareup_tape_native_QueueFileNative_clear
  (JNIEnv *env, jobject thiz) {

};

JNIEXPORT void JNICALL Java_com_squareup_tape_native_QueueFileNative_close
  (JNIEnv *env, jobject thiz) {

};

JNIEXPORT jstring JNICALL Java_com_squareup_tape_native_QueueFileNative_toString
  (JNIEnv *env, jobject thiz) {
  return NULL;
};

#ifdef __cplusplus
}
#endif
