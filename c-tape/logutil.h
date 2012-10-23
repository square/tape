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

#ifndef LOGUTIL_H_
#define LOGUTIL_H_

/*
 * Bare bones logging tool.
 *
 * Usage example:
 *
 *   LOG(LINFO, "This prints to a logfile, hello %s, this is a number: %d", name, value);
 *   LOG_SETLEVEL_WARN;
 *   LOG(LFATAL, "This prints to a logfile, hello %s, this is a number: %d", name, value);
 *
 * TODO(jochen): consider using log4c.
 */

#define LOG _log_vprintf

// TODO(jochen): use varidic macros.
#define LDEBUG _LOGLEVEL_DEBUG, __FILE__, __LINE__
#define LINFO _LOGLEVEL_INFO, __FILE__, __LINE__
#define LWARN _LOGLEVEL_WARN, __FILE__, __LINE__
#define LFATAL _LOGLEVEL_FATAL, __FILE__, __LINE__

#define LOG_SETLEVEL_DEBUG _log_setlevel(_LOGLEVEL_DEBUG)
#define LOG_SETLEVEL_INFO _log_setlevel(_LOGLEVEL_INFO)
#define LOG_SETLEVEL_WARN _log_setlevel(_LOGLEVEL_WARN)
#define LOG_SETLEVEL_FATAL _log_setlevel(_LOGLEVEL_FATAL)


/** Used for debug builds, will fail program with stack trace */
#define LOG_SETDEBUGFAILLEVEL_WARN _log_debug_failatlevel(_LOGLEVEL_WARN)
/** Used for debug builds, will fail program with stack trace */
#define LOG_SETDEBUGFAILLEVEL_FATAL _log_debug_failatlevel(_LOGLEVEL_FATAL)

enum loglevel {
  _LOGLEVEL_DEBUG = 0, _LOGLEVEL_INFO, _LOGLEVEL_WARN, _LOGLEVEL_FATAL
};

void _log_setlevel(enum loglevel);
void _log_debug_failatlevel(enum loglevel);

/** returns 1 if something was printed, else 0 */
int _log_vprintf(enum loglevel, char* file, int line, char* msg, ...);

#endif
