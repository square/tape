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

#ifndef MINUNIT_H_
#define MINUNIT_H_

/*
 * Some minimal unit testing functions.
 */

#define mu_assert(test) do { if (!(test)) { fprintf(stderr, "%s:%d test fail: assertion failure\n",__FILE__, __LINE__); abort(); } } while (0)
#define mu_assertm(test, message) do { if (!(test)) { fprintf(stderr, "%s:%d test fail:  %s\n",__FILE__, __LINE__, message); abort(); } } while (0)
#define mu_assertmv(test, ...) do { if (!(test)) { fprintf(stderr, "%s:%d test fail:  ",__FILE__, __LINE__); vfprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); abort(); } } while (0)
#define mu_assert_notnull(test) do { if (!(test)) { fprintf(stderr, "%s:%d test fail:  null pointer\n",__FILE__, __LINE__); abort(); } } while (0)
#define mu_assert_memcmp(m1, m2, l) do { if(m1==NULL||m2==NULL||memcmp(m1,m2,(size_t)l)!=0) { fprintf(stderr, "%s:%d test fail:  data doesn't match\n",__FILE__, __LINE__); abort(); } } while (0)
#define mu_run_test(testfn) do { mu_setup(); tests_run++; testfn(); mu_teardown(); } while (0)
extern int tests_run;

#endif
