#
# Copyright (C) 2010 Square, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# TODO(jochen): consider building a library in c-tape/jni.
# generated the queuefilenative.h:
#  javah -classpath target/classes -o jni/queuefilenative.h com.squareup.tape.QueueFileNative
#
# $(warning hello)


LOCAL_PATH := $(call my-dir)
LOCAL_C_FILES := queuefilenative.c jniutils.c


OTHER_C_PATH := $(LOCAL_PATH)/../../../c-tape
OTHER_C_FILES := fileio.c logutil.c queuefile.c

include $(CLEAR_VARS)
LOCAL_MODULE := c-tape-android-native
LOCAL_CFLAGS := -D__ANDROID__
LOCAL_C_INCLUDES := $(abspath $(OTHER_C_PATH))
LOCAL_SRC_FILES := $(LOCAL_C_FILES) $(addprefix ../$(OTHER_C_PATH)/, $(OTHER_C_FILES))
LOCAL_LDLIBS := -llog
include $(BUILD_SHARED_LIBRARY)

