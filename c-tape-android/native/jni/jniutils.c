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

#include <stdlib.h>
#include <string.h>
#include <jni.h>

#include "jniutils.h"

static char* make_message(const char *fmt, va_list ap);

static jint throwThisException1MV(JNIEnv* env, const char* className, const char* message, int throwNoClass) {
  jint returnval = 0;
  jclass exClass;
  exClass = (*env)->FindClass(env, className);
  if (exClass == NULL) {
    if (throwNoClass) {
      returnval = throwNoClassDefError(env, "Could not find class %s", className);
    } else {
      returnval = 0; // HACKHACK: what to do?
    }
  } else {
    returnval = (*env)->ThrowNew(env, exClass, message);
  }
  return returnval;
}

jint throwNoClassDefError(JNIEnv* env, const char* message, ...) {
  va_list args;
  va_start (args, message);
  char* fullmsg = make_message(message, args);
  va_end (args);
  jint e = throwThisException1MV(env, "java/lang/NoClassDefFoundError", fullmsg, 0);
  free (fullmsg);
  return e;
}

jint throwIoException(JNIEnv* env, const char* message, ...) {
  va_list args;
  va_start (args, message);
  char* fullmsg = make_message(message, args);
  va_end (args);
  jint e = throwThisException1MV(env, "java/io/IOException", fullmsg, 1);
  free (fullmsg);
  return e;
}

// modified example taken from vsnprintf man page.
static char* make_message(const char *fmt, va_list ap) {
    int n;
    int size = 100;     /* Guess we need no more than 100 bytes. */
    char *p, *np;

   if ((p = malloc(size)) == NULL)
        return NULL;

   while (1) {

       /* Try to print in the allocated space. */

        n = vsnprintf(p, size, fmt, ap);

       /* If that worked, return the string. */

       if (n > -1 && n < size)
            return p;

       /* Else try again with more space. */

       if (n > -1)    /* glibc 2.1 */
            size = n+1; /* precisely what is needed */
        else           /* glibc 2.0 */
            size *= 2;  /* twice the old size */

       if ((np = realloc (p, size)) == NULL) {
            free(p);
            return NULL;
        } else {
            p = np;
        }
    }
}
